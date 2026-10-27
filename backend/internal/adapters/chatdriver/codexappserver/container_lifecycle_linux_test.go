//go:build linux

package codexappserver

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

func TestContainerAttachOutlivesRequestContextAndStopsWithSession(t *testing.T) {
	docker, pidPath, stoppedPath := fakeDockerAttach(t, true)
	ctx, cancel := context.WithCancel(context.Background())
	proc, err := spawnContainerAppServer(ctx, t.TempDir(), containerTestEnv(docker))
	if err != nil {
		t.Fatalf("start fake supervised container: %v", err)
	}
	cancel()

	if _, err := proc.stdin.Write([]byte("still-attached\n")); err != nil {
		t.Fatalf("HTTP request cancellation closed the AO-owned Docker attach: %v", err)
	}
	if err := proc.stop(); err != nil {
		t.Fatalf("stop exact supervised container and attach: %v", err)
	}
	if _, err := os.Stat(stoppedPath); err != nil {
		t.Fatalf("exact container stop was not requested: %v", err)
	}
	assertProcessGone(t, pidPath)
}

func TestContainerStopProofBindsExactSessionAndOriginalWaitTime(t *testing.T) {
	docker, _, _ := fakeDockerAttach(t, true)
	proc, err := spawnContainerAppServer(context.Background(), t.TempDir(), containerTestEnv(docker))
	if err != nil {
		t.Fatalf("start fake supervised container: %v", err)
	}
	proof, err := proc.terminateWithProof()
	if err != nil {
		t.Fatalf("stop exact supervised container and wait for attach: %v", err)
	}
	if proof.SessionID != "container-lifecycle-test" || proof.Method != ports.ChatTerminationNativeProcessExitWaited || !proof.CleanupComplete || proof.VerifiedAt.IsZero() {
		t.Fatalf("termination proof = %+v", proof)
	}
	replayed, err := proc.terminateWithProof()
	if err != nil || replayed != proof {
		t.Fatalf("replayed proof = %+v, err=%v; want exact original %+v", replayed, err, proof)
	}
}

func TestContainerStartupCancellationKillsUnidentifiedAttach(t *testing.T) {
	docker, pidPath, _ := fakeDockerAttach(t, false)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := spawnContainerAppServer(ctx, t.TempDir(), containerTestEnv(docker))
		result <- err
	}()
	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, err := os.Stat(pidPath); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("fake Docker attach did not start")
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-result:
		if err == nil || errors.Is(err, ports.ErrChatStartupClean) {
			t.Fatalf("missing launch identity cannot prove startup cleanup: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("startup cancellation did not return")
	}
	assertProcessGone(t, pidPath)
}

func TestContainerStopFailureRetainsRetryableHandle(t *testing.T) {
	docker, pidPath, _ := fakeDockerAttach(t, true)
	stopCount := filepath.Join(filepath.Dir(docker), "stop.count")
	script := `#!/bin/sh
case "$1" in
  run)
    cidfile=""
    while [ "$#" -gt 0 ]; do
      if [ "$1" = "--cidfile" ]; then shift; cidfile="$1"; fi
      shift
    done
    echo "$$" > ` + shellQuote(pidPath) + `
    printf '%064d\n' 1 > "$cidfile"
    exec sleep 60
    ;;
  stop)
    count=0
    [ ! -f ` + shellQuote(stopCount) + ` ] || count=$(cat ` + shellQuote(stopCount) + `)
    count=$((count + 1))
    echo "$count" > ` + shellQuote(stopCount) + `
    if [ "$count" -eq 1 ]; then exit 1; fi
    kill -TERM "$(cat ` + shellQuote(pidPath) + `)" 2>/dev/null || true
    exit 0
    ;;
  inspect)
    count=0
    [ ! -f ` + shellQuote(stopCount) + ` ] || count=$(cat ` + shellQuote(stopCount) + `)
    if [ "$count" -lt 2 ]; then echo '{}'; exit 0; fi
    echo 'No such container' >&2
    exit 1
    ;;
  rm) exit 1 ;;
  *) exit 2 ;;
esac
`
	if err := os.WriteFile(docker, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	proc, err := spawnContainerAppServer(context.Background(), t.TempDir(), containerTestEnv(docker))
	if err != nil {
		t.Fatalf("start fake supervised container: %v", err)
	}
	if err := proc.stop(); err == nil {
		t.Fatal("first stop unexpectedly succeeded without proving container removal")
	}
	if err := proc.stop(); err != nil {
		t.Fatalf("retry stop after an unverified failure: %v", err)
	}
	assertProcessGone(t, pidPath)
}

func containerTestEnv(docker string) []string {
	return []string{
		"ZOE_CODEX_SESSION_ID=container-lifecycle-test",
		"AO_CODEX_CONTAINER_IMAGE=zoe-ao-codex:test",
		"AO_CODEX_DOCKER_BINARY=" + docker,
		"ZOE_CODEX_PROXY_UPSTREAM=https://provider.invalid/v1",
		"ZOE_CODEX_PROXY_KEY=fixture-only-provider-key",
		"ZOE_CODEX_PROXY_MODEL=gpt-6-luna",
		"CODEX_HOME=/tmp",
	}
}

func fakeDockerAttach(t *testing.T, writeContainerID bool) (docker, pidPath, stoppedPath string) {
	t.Helper()
	root := t.TempDir()
	docker = filepath.Join(root, "docker")
	pidPath = filepath.Join(root, "attach.pid")
	stoppedPath = filepath.Join(root, "container.stopped")
	cidCommand := ""
	if writeContainerID {
		cidCommand = `printf '%064d\n' 1 > "$cidfile"`
	}
	script := fmt.Sprintf(`#!/bin/sh
pidfile=%q
stopped=%q
case "$1" in
  run)
    cidfile=""
    while [ "$#" -gt 0 ]; do
      if [ "$1" = "--cidfile" ]; then shift; cidfile="$1"; fi
      shift
    done
    echo "$$" > "$pidfile"
    %s
    exec sleep 60
    ;;
  stop)
    echo stopped > "$stopped"
    kill -TERM "$(cat "$pidfile")" 2>/dev/null || true
    exit 0
    ;;
  inspect)
    echo "No such container" >&2
    exit 1
    ;;
  ps) exit 0 ;;
  *) exit 2 ;;
esac
`, pidPath, stoppedPath, cidCommand)
	if err := os.WriteFile(docker, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	return docker, pidPath, stoppedPath
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}

func assertProcessGone(t *testing.T, pidPath string) {
	t.Helper()
	contents, err := os.ReadFile(pidPath)
	if err != nil {
		return // The command may have been killed before it recorded its PID.
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(contents)))
	if err != nil {
		t.Fatalf("fake Docker attach PID is invalid: %v", err)
	}
	if err := syscall.Kill(pid, 0); err == nil || err != syscall.ESRCH {
		t.Fatalf("Docker attach PID %d is still alive or cannot be verified gone: %v", pid, err)
	}
}

func TestRepeatedSessionLaunchUsesDistinctDockerNamesAndLabels(t *testing.T) {
	docker, _, _ := fakeDockerAttach(t, true)
	logPath := filepath.Join(filepath.Dir(docker), "launch-args")
	data, err := os.ReadFile(docker)
	if err != nil {
		t.Fatal(err)
	}
	script := strings.Replace(string(data), `case "$1" in`, fmt.Sprintf("printf '%%s\\n' \"$@\" >> %s\ncase \"$1\" in", shellQuote(logPath)), 1)
	if err := os.WriteFile(docker, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		proc, err := spawnContainerAppServer(context.Background(), t.TempDir(), containerTestEnv(docker))
		if err != nil {
			t.Fatal(err)
		}
		if err := proc.terminate(); err != nil {
			t.Fatal(err)
		}
	}
	data, err = os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	var names, launches []string
	lines := strings.Split(string(data), "\n")
	for n, line := range lines {
		if line == "--name" && n+1 < len(lines) {
			names = append(names, lines[n+1])
		}
		if strings.HasPrefix(line, "io.zoe.ao.launch=") {
			launches = append(launches, strings.TrimPrefix(line, "io.zoe.ao.launch="))
		}
	}
	if len(names) != 2 || names[0] == names[1] || len(launches) != 2 || launches[0] == launches[1] || !strings.HasSuffix(names[0], launches[0]) || !strings.HasSuffix(names[1], launches[1]) {
		t.Fatalf("names=%v launches=%v", names, launches)
	}
}

func TestStartupCleanupCannotSelectOlderSameSessionRecord(t *testing.T) {
	root := t.TempDir()
	docker := filepath.Join(root, "docker")
	logPath := filepath.Join(root, "args")
	script := fmt.Sprintf(`#!/bin/sh
printf '%%s\n' "$@" >> %s
if [ "$1" = ps ]; then
  case " $* " in
    *" label=io.zoe.ao.launch=newlaunch "*)
      case " $* " in *" name=^/newname$ "*) exit 0;; esac
      ;;
  esac
  printf '%%064d\n' 1
  exit 0
fi
exit 8
`, shellQuote(logPath))
	if err := os.WriteFile(docker, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	done := make(chan error)
	close(done)
	err := cleanupStartingContainer(docker, filepath.Join(root, "missing-cid"), "same-session", "newlaunch", "newname", done)
	if err == nil {
		t.Fatal("absent launch identity was promoted to clean startup proof")
	}
	data, _ := os.ReadFile(logPath)
	if strings.Contains(string(data), "stop\n") || strings.Contains(string(data), "rm\n") {
		t.Fatalf("older container was selected for cleanup: %s", data)
	}
}
