package codexappserver

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

func TestExternalRouteRequiresSupervisedContainerImage(t *testing.T) {
	tests := []struct {
		name  string
		env   map[string]string
		image string
		want  bool
		bad   bool
	}{
		{name: "native route remains native", env: map[string]string{}, want: false},
		{name: "external route without image fails closed", env: map[string]string{providerUpstreamEnv: "https://proxy.example/v1"}, bad: true},
		{name: "external profile setup failure fails closed", env: map[string]string{providerUpstreamEnv: "https://proxy.example/v1", "ZOE_CODEX_PROFILE_ERROR": "provider_credential_unavailable"}, image: "zoe-ao-codex:test", bad: true},
		{name: "external route with image is supervised", env: map[string]string{providerUpstreamEnv: "https://proxy.example/v1"}, image: "zoe-ao-codex:test", want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := supervisedExternalRoute(tt.env, tt.image)
			if (err != nil) != tt.bad || got != tt.want {
				t.Fatalf("supervisedExternalRoute() = %v, %v; want %v, error=%v", got, err, tt.want, tt.bad)
			}
		})
	}
}

func TestSupervisedContainerLaunchDoesNotResolveHostCodex(t *testing.T) {
	t.Setenv(containerImageEnv, "zoe-ao-codex:test")
	env := map[string]string{
		providerUpstreamEnv: "https://gateway.example/v1",
		containerImageEnv:   "zoe-ao-codex:test",
		"CODEX_HOME":        "/tmp/private-codex-home",
	}
	driver := New(fakePlugin{binErr: errors.New("host Codex is not installed")}, slog.New(slog.DiscardHandler))
	bin, launchEnv, err := driver.launchConfig(context.Background(), env)
	if err != nil {
		t.Fatalf("containerized route depended on host Codex binary: %v", err)
	}
	if bin != "" || !reflect.DeepEqual(launchEnv, envSlice(env)) {
		t.Fatalf("container launch configuration = bin %q env %v", bin, launchEnv)
	}
}

func TestSupervisedContainerLaunchFailsClosedWhenImageIsMissing(t *testing.T) {
	t.Setenv(containerImageEnv, "")
	driver := New(fakePlugin{bin: "/usr/bin/codex"}, slog.New(slog.DiscardHandler))
	if _, _, err := driver.launchConfig(context.Background(), map[string]string{providerUpstreamEnv: "https://gateway.example/v1"}); err == nil {
		t.Fatal("container-required external route fell back to host Codex when its image was missing")
	}
}

func TestSupervisedContainerProbeDoesNotResolveHostCodex(t *testing.T) {
	t.Setenv(containerImageEnv, "zoe-ao-codex:test")
	t.Setenv("AO_CODEX_EXTERNAL_BASE_URL", "https://gateway.example/v1")
	driver := New(fakePlugin{binErr: errors.New("host Codex is not installed")}, slog.New(slog.DiscardHandler))
	probed, err := driver.Probe(context.Background())
	if err != nil {
		t.Fatalf("supervised image probe depended on host Codex binary: %v", err)
	}
	if len(probed) == 0 {
		t.Fatal("supervised image probe returned no chat capabilities")
	}
}

func TestContainerImageAloneDoesNotBypassNativeProbe(t *testing.T) {
	t.Setenv(containerImageEnv, "zoe-ao-codex:test")
	t.Setenv("AO_CODEX_EXTERNAL_BASE_URL", "")
	driver := New(fakePlugin{binErr: errors.New("host Codex is not installed")}, slog.New(slog.DiscardHandler))
	if _, err := driver.Probe(context.Background()); err == nil {
		t.Fatal("native route skipped its host binary probe without an external provider route")
	}
}

func TestDangerFullAccessIsScopedToSupervisedContainer(t *testing.T) {
	policy, sandbox, reviewer := conversationApprovalSettings(&conversation{supervisedContainer: true}, ports.PermissionModeAuto, false)
	if policy != "on-request" || sandbox != "danger-full-access" || reviewer != "auto_review" {
		t.Fatalf("supervised auto approval settings = %q/%q/%q", policy, sandbox, reviewer)
	}
	_, nativeSandbox, _ := conversationApprovalSettings(&conversation{}, ports.PermissionModeAuto, false)
	if nativeSandbox != "workspace-write" {
		t.Fatalf("native auto sandbox = %q, want workspace-write", nativeSandbox)
	}
	_, readOnlySandbox, _ := conversationApprovalSettings(&conversation{supervisedContainer: true}, ports.PermissionModeAuto, true)
	if readOnlySandbox != "read-only" {
		t.Fatalf("supervised read-only sandbox = %q, want read-only", readOnlySandbox)
	}
}

func TestSupervisedThreadStartsFullAccessAndTurnInheritsIt(t *testing.T) {
	t.Setenv(containerImageEnv, "zoe-ao-codex:test")
	d, srv := newTestDriver(t)
	conv, err := d.Start(context.Background(), ports.ChatStartConfig{
		WorkspacePath: "/tmp/ws",
		Env: map[string]string{
			providerUpstreamEnv: "https://gateway.example/v1",
			containerImageEnv:   "zoe-ao-codex:test",
		},
		Permissions: ports.PermissionModeAuto,
	})
	if err != nil {
		t.Fatalf("start supervised conversation: %v", err)
	}
	defer func() { _ = conv.Close() }()
	start := srv.awaitFrame(func(f frame) bool { return f.Method == "thread/start" })
	var startParams struct {
		ApprovalPolicy string `json:"approvalPolicy"`
		Reviewer       string `json:"approvalsReviewer"`
		Sandbox        string `json:"sandbox"`
	}
	if err := json.Unmarshal(start.Params, &startParams); err != nil {
		t.Fatalf("decode thread/start: %v", err)
	}
	if startParams.ApprovalPolicy != "on-request" || startParams.Reviewer != "auto_review" || startParams.Sandbox != "danger-full-access" {
		t.Fatalf("supervised thread/start policy = %q/%q/%q", startParams.ApprovalPolicy, startParams.Reviewer, startParams.Sandbox)
	}
	if _, err := conv.SendTurn(context.Background(), ports.ChatUserMessage{
		Text:     "synthetic protocol probe",
		Settings: ports.ChatTurnSettings{Approval: ports.PermissionModeAuto},
	}); err != nil {
		t.Fatalf("send inherited-settings turn: %v", err)
	}
	turn := srv.awaitFrame(func(f frame) bool { return f.Method == "turn/start" })
	var turnParams struct {
		ApprovalPolicy string          `json:"approvalPolicy"`
		Reviewer       string          `json:"approvalsReviewer"`
		SandboxPolicy  json.RawMessage `json:"sandboxPolicy"`
	}
	if err := json.Unmarshal(turn.Params, &turnParams); err != nil {
		t.Fatalf("decode turn/start: %v", err)
	}
	var sandboxPolicy map[string]any
	if err := json.Unmarshal(turnParams.SandboxPolicy, &sandboxPolicy); err != nil {
		t.Fatalf("decode supervised turn sandboxPolicy: %v", err)
	}
	if turnParams.ApprovalPolicy != "on-request" || turnParams.Reviewer != "auto_review" || sandboxPolicy["type"] != "dangerFullAccess" {
		t.Fatalf("supervised turn policy = %q/%q/%#v", turnParams.ApprovalPolicy, turnParams.Reviewer, sandboxPolicy)
	}
}

func TestSupervisedTurnFullAccessDoesNotChangeNativeOrReadOnlyPolicy(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		readOnly, supervised bool
		wantType             string
	}{
		{name: "native auto", supervised: false, wantType: "workspaceWrite"},
		{name: "supervised auto", supervised: true, wantType: "dangerFullAccess"},
		{name: "supervised read-only", readOnly: true, supervised: true, wantType: "readOnly"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			params := map[string]any{}
			applyTurnSettings(params, ports.ChatTurnSettings{Approval: ports.PermissionModeAuto}, tc.readOnly, false, tc.supervised)
			policy, ok := params["sandboxPolicy"].(map[string]any)
			if !ok || policy["type"] != tc.wantType {
				t.Fatalf("turn sandboxPolicy = %#v, want type %q", params["sandboxPolicy"], tc.wantType)
			}
		})
	}
}

func TestConversationRetriesFailedContainerStop(t *testing.T) {
	var attempts atomic.Int32
	conv := &conversation{proc: &process{stop: func() error {
		if attempts.Add(1) == 1 {
			return errors.New("container stop unverified")
		}
		return nil
	}}}
	if err := conv.Close(); err == nil {
		t.Fatal("first conversation close unexpectedly succeeded")
	}
	if err := conv.Close(); err != nil {
		t.Fatalf("conversation close did not retry after failed container stop: %v", err)
	}
	if attempts.Load() != 2 {
		t.Fatalf("container stop attempts = %d, want 2", attempts.Load())
	}
}

func TestContainerStartupWaitCanDrainClosedWaitChannelAfterEarlyExit(t *testing.T) {
	done := make(chan error, 1)
	done <- errors.New("docker exited before writing cidfile")
	close(done)
	if _, err := waitContainerID(context.Background(), done, filepath.Join(t.TempDir(), "missing-cid")); err == nil {
		t.Fatal("waitContainerID unexpectedly succeeded without a container ID")
	}
	select {
	case <-done:
	default:
		t.Fatal("closed process wait channel blocked the startup cleanup path")
	}
}
