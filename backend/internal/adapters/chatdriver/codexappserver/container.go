package codexappserver

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	aoprocess "github.com/aoagents/agent-orchestrator/backend/internal/process"
)

const (
	containerModeEnv    = "ZOE_CODEX_CONTAINER"
	containerImageEnv   = "AO_CODEX_CONTAINER_IMAGE"
	providerUpstreamEnv = "ZOE_CODEX_PROXY_UPSTREAM"
	providerKeyEnv      = "ZOE_CODEX_PROXY_KEY"
	providerModelEnv    = "ZOE_CODEX_PROXY_MODEL"
	sessionIDEnv        = "ZOE_CODEX_SESSION_ID"
)

func spawnContainerAppServer(ctx context.Context, workdir string, env []string) (*process, error) {
	values := environmentMap(env)
	sessionID := strings.TrimSpace(values[sessionIDEnv])
	image := strings.TrimSpace(values[containerImageEnv])
	home := strings.TrimSpace(values["CODEX_HOME"])
	providerKey := strings.TrimSpace(values[providerKeyEnv])
	providerModel := strings.TrimSpace(values[providerModelEnv])
	if providerKey == "" {
		providerKey = strings.TrimSpace(values["ZOE_CODEX_API_KEY"])
	}
	if sessionID == "" || image == "" || home == "" || providerModel == "" || !filepath.IsAbs(workdir) || !filepath.IsAbs(home) {
		return nil, errors.New("supervised Codex container requires session identity, image, absolute workspace, and private CODEX_HOME")
	}
	if strings.ContainsAny(sessionID+image+workdir+home, "\x00\r\n,") || strings.HasPrefix(image, "-") {
		return nil, errors.New("supervised Codex container configuration is invalid")
	}
	if info, err := os.Stat(workdir); err != nil || !info.IsDir() {
		return nil, errors.New("supervised Codex workspace is unavailable")
	}
	if info, err := os.Stat(home); err != nil || !info.IsDir() {
		return nil, errors.New("supervised Codex home is unavailable")
	}
	broker, err := startProviderBroker(values[providerUpstreamEnv], providerKey, providerModel, filepath.Dir(workdir))
	if err != nil {
		return nil, err
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		_ = broker.Close()
		return nil, errors.New("supervised Codex launch identity unavailable")
	}
	launchID := hex.EncodeToString(nonce[:])
	role := values["ZOE_CODEX_RUNTIME_ROLE"]
	if role == "" {
		role = "worker"
	}
	if role != "worker" && role != "background" {
		_ = broker.Close()
		return nil, errors.New("supervised Codex runtime role invalid")
	}
	root := filepath.Dir(broker.SocketPath())
	cidfile := filepath.Join(root, "container-id")
	docker := strings.TrimSpace(values["AO_CODEX_DOCKER_BINARY"])
	if docker == "" {
		docker = "docker"
	}
	// The caller context owns startup only; session lifetime is owned by the
	// returned process's terminate closure. AO cancels its HTTP create context
	// after sending the session response, so tying the attach process directly to
	// that request would orphan the container and close the controller pipes.
	processCtx, cancelProcess := context.WithCancel(context.Background())
	name := "zoe-ao-" + shortSessionHash(sessionID) + "-" + launchID
	uidgid := fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid())
	args := []string{
		"run", "--rm", "--init", "-i", "--cidfile", cidfile,
		"--name", name,
		"--label", "io.zoe.managed=true",
		"--label", "io.zoe.runtime=ao-codex-chat",
		"--label", "io.zoe.ao.session=" + sessionID,
		"--label", "io.zoe.ao.launch=" + launchID,
		"--label", "io.zoe.ao.role=" + role,
		"--network", "none", "--cap-drop", "ALL", "--security-opt", "no-new-privileges",
		"--read-only", "--pids-limit", "256", "--memory", "2g", "--cpus", "2",
		"--user", uidgid,
		"--tmpfs", "/tmp:rw,nosuid,nodev,noexec,size=256m",
		// Bind mounts are read/write by default; Docker's --mount parser does
		// not accept the bare `rw` option supported by --volume.
		"--mount", "type=bind,src=" + workdir + ",dst=" + workdir,
		"--mount", "type=bind,src=" + home + ",dst=/codex-home",
		"--mount", "type=bind,src=" + broker.SocketPath() + ",dst=/run/zoe/provider.sock,readonly",
		"--workdir", workdir,
		"--env", "CODEX_HOME=/codex-home",
		"--env", "HOME=/tmp",
		"--env", "ZOE_PROVIDER_SOCKET=/run/zoe/provider.sock",
		// Codex expects the configured env_key to exist even though auth is
		// disabled. The host broker always replaces Authorization upstream.
		"--env", "ZOE_CONTAINER_PROXY_NO_AUTH=zoe-provider-broker-no-auth",
		"--env", "PATH=/usr/local/bin:/usr/bin:/bin",
		image,
		"--", "/usr/local/bin/codex",
		"-c", `model_provider="zoe_external"`,
		"-c", `model_providers.zoe_external.base_url="http://127.0.0.1:47631/v1"`,
		"-c", `model_providers.zoe_external.env_key="ZOE_CONTAINER_PROXY_NO_AUTH"`,
		"-c", `model_providers.zoe_external.requires_openai_auth=false`,
		"-c", `sandbox_mode="danger-full-access"`,
		"app-server",
	}
	// The Docker attach process must outlive the HTTP request that starts a
	// session. Startup cancellation is handled explicitly below; tying the child
	// to that short-lived context would close the controller pipes immediately
	// after AO returns the newly created session.
	cmd := aoprocess.CommandContext(processCtx, docker, args...)
	cmd.Dir = workdir
	// Docker receives no provider key or AO session environment. The credential
	// is held only by the in-process host broker above.
	cmd.Env = sanitizedDockerEnvironment(env)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		cancelProcess()
		_ = broker.Close()
		return nil, fmt.Errorf("container stdin pipe: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cancelProcess()
		_ = broker.Close()
		return nil, fmt.Errorf("container stdout pipe: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		cancelProcess()
		_ = broker.Close()
		return nil, fmt.Errorf("container stderr pipe: %w", err)
	}
	if err := cmd.Start(); err != nil {
		cancelProcess()
		_ = broker.Close()
		return nil, fmt.Errorf("start supervised Codex container: %w", err)
	}
	stderrCapture := &boundedBuffer{limit: 16 << 10}
	stderrDone := make(chan struct{})
	go func() {
		_, _ = io.Copy(stderrCapture, stderr)
		close(stderrDone)
	}()
	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
		close(done)
	}()
	startupCtx, cancelStartup := context.WithTimeout(ctx, 10*time.Second)
	defer cancelStartup()
	containerID, err := waitContainerID(startupCtx, done, cidfile)
	if err != nil {
		_ = stdin.Close()
		_ = cmd.Process.Kill()
		<-done
		<-stderrDone
		cleanupErr := cleanupStartingContainer(docker, cidfile, sessionID, launchID, name, done)
		cancelProcess()
		if cleanupErr == nil && broker.Close() == nil {
			// Return only the stable proof marker; startup stderr may contain
			// credentials and is unnecessary to the rejection protocol.
			return nil, ports.ErrChatStartupClean
		}
		// A failed creation has no process handle for AO to retry through. Close
		// the credential-bearing broker even if exact container cleanup cannot be
		// verified; the session labels remain available to supervisor recovery.
		_ = broker.Close()
		return nil, fmt.Errorf("%w; supervised Codex startup cleanup could not be verified: %v", startupError(err, strings.ReplaceAll(stderrCapture.String(), providerKey, "[redacted]")), cleanupErr)
	}
	var stopMu sync.Mutex
	stopped := false
	var verifiedAt time.Time
	stop := func() error {
		stopMu.Lock()
		defer stopMu.Unlock()
		if stopped {
			return nil
		}
		_ = stdin.Close()
		stopErr := stopContainerAndWait(docker, containerID, done)
		if stopErr != nil {
			// The callback remains available for a later retry. The command wait
			// goroutine always reaps Docker, even if Docker exits independently.
			return stopErr
		}
		cancelProcess()
		if brokerErr := broker.Close(); brokerErr != nil {
			return brokerErr
		}
		stopped = true
		verifiedAt = time.Now().UTC()
		return nil
	}
	terminateWithProof := func() (ports.ChatTerminationProof, error) {
		if err := stop(); err != nil {
			return ports.ChatTerminationProof{}, err
		}
		stopMu.Lock()
		defer stopMu.Unlock()
		if !stopped || verifiedAt.IsZero() || sessionID == "" {
			return ports.ChatTerminationProof{}, errors.New("supervised Codex process exit is unverified")
		}
		return ports.ChatTerminationProof{SessionID: sessionID, Method: ports.ChatTerminationNativeProcessExitWaited, CleanupComplete: true, VerifiedAt: verifiedAt}, nil
	}
	return &process{
		stdin: stdin, stdout: stdout,
		stop:               stop,
		terminate:          stop,
		terminateWithProof: terminateWithProof,
	}, nil
}

func cleanupStartingContainer(docker, cidfile, sessionID, launchID, name string, done <-chan error) error {
	containerID := ""
	if data, err := os.ReadFile(cidfile); err == nil {
		candidate := strings.TrimSpace(string(data))
		if validContainerID(candidate) {
			containerID = candidate
		}
	}
	if containerID == "" {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		args := []string{"ps", "-aq", "--no-trunc",
			"--filter", "label=io.zoe.managed=true",
			"--filter", "label=io.zoe.runtime=ao-codex-chat",
			"--filter", "label=io.zoe.ao.session=" + sessionID,
			"--filter", "label=io.zoe.ao.launch=" + launchID,
			"--filter", "name=^/" + name + "$",
		}
		output, err := exec.CommandContext(ctx, docker, args...).CombinedOutput()
		if err != nil {
			return errors.New("could not inspect containers created for this session")
		}
		ids := strings.Fields(string(output))
		if len(ids) == 0 {
			// Killing the CLI does not cancel an in-flight Docker daemon create.
			// An empty instantaneous query cannot prove no late runtime appears.
			return errors.New("startup container identity was never established")
		}
		if len(ids) != 1 || !validContainerID(ids[0]) {
			return errors.New("session labels matched an ambiguous container set")
		}
		containerID = ids[0]
	}
	return stopContainerAndWait(docker, containerID, done)
}

func validContainerID(id string) bool {
	return len(id) >= 12 && len(id) <= 128 && isHex(id)
}

type boundedBuffer struct {
	bytes.Buffer
	limit int
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	if b.limit > b.Len() {
		n := len(p)
		if remaining := b.limit - b.Len(); n > remaining {
			n = remaining
		}
		_, _ = b.Buffer.Write(p[:n])
	}
	return len(p), nil
}

func startupError(err error, stderr string) error {
	stderr = strings.TrimSpace(stderr)
	if stderr == "" {
		return err
	}
	return fmt.Errorf("%w: %s", err, stderr)
}

func supervisedExternalRoute(env map[string]string, image string) (bool, error) {
	if strings.TrimSpace(env["ZOE_CODEX_PROFILE_ERROR"]) != "" {
		return false, errors.New("external Codex worker profile is incomplete")
	}
	if strings.TrimSpace(env[providerUpstreamEnv]) == "" {
		return false, nil
	}
	if strings.TrimSpace(image) == "" {
		return false, errors.New("external Codex worker route requires the supervised container image")
	}
	return true, nil
}

func waitContainerID(ctx context.Context, done <-chan error, path string) (string, error) {
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		if data, err := os.ReadFile(path); err == nil {
			id := strings.TrimSpace(string(data))
			if validContainerID(id) {
				// Docker writes the complete ID before the container command starts.
				// cmd.Wait remains owned by the process stop closure below.
				return id, nil
			}
		}
		select {
		case err := <-done:
			return "", fmt.Errorf("Codex container exited before reporting its identity: %w", err)
		case <-ctx.Done():
			return "", ctx.Err()
		case <-deadline.C:
			return "", errors.New("Codex container did not report an identity before startup timeout")
		case <-ticker.C:
		}
	}
}

func stopContainerAndWait(docker, containerID string, done <-chan error) error {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	_, stopErr := exec.CommandContext(ctx, docker, "stop", "--time", "5", containerID).CombinedOutput()
	missing, inspectErr := inspectContainerMissing(ctx, docker, containerID)
	if inspectErr != nil {
		return inspectErr
	}
	if !missing {
		// Force removal targets only the ID read from this launch's cidfile.
		if _, err := exec.CommandContext(ctx, docker, "rm", "--force", containerID).CombinedOutput(); err != nil {
			if stopErr != nil {
				return errors.New("supervised Codex container could not be terminated")
			}
		}
		missing, inspectErr = inspectContainerMissing(ctx, docker, containerID)
		if inspectErr != nil {
			return inspectErr
		}
		if !missing {
			return errors.New("supervised Codex container still exists after stop")
		}
	}
	select {
	case <-done:
	case <-ctx.Done():
		return errors.New("Docker attach process did not exit after container termination")
	}
	return nil
}

func inspectContainerMissing(ctx context.Context, docker, id string) (bool, error) {
	output, err := exec.CommandContext(ctx, docker, "inspect", id).CombinedOutput()
	if err == nil {
		return false, nil
	}
	message := strings.ToLower(string(output))
	if strings.Contains(message, "no such object") || strings.Contains(message, "no such container") {
		return true, nil
	}
	return false, errors.New("supervised Codex container termination could not be verified")
}

func environmentMap(env []string) map[string]string {
	out := make(map[string]string, len(env))
	for _, item := range env {
		key, value, ok := strings.Cut(item, "=")
		if ok {
			out[key] = value
		}
	}
	return out
}

func sanitizedDockerEnvironment(env []string) []string {
	values := environmentMap(env)
	for key := range values {
		if strings.HasPrefix(key, "ZOE_CODEX_") || key == "ZOE_PROVIDER_SOCKET" {
			delete(values, key)
		}
	}
	out := make([]string, 0, len(values))
	for key, value := range values {
		out = append(out, key+"="+value)
	}
	return out
}

func shortSessionHash(sessionID string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(sessionID)))[:16]
}

func isHex(value string) bool {
	for _, r := range value {
		if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F')) {
			return false
		}
	}
	return value != ""
}
