//go:build linux

package agentaccount

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"golang.org/x/sys/unix"
)

func TestCodexExternalProfileReadsPrivateMemfdWithoutPersistingKey(t *testing.T) {
	const marker = "synthetic-memfd-provider-test-key"
	fd, err := unix.MemfdCreate("zoe-provider-test", unix.MFD_CLOEXEC)
	if err != nil {
		t.Fatal(err)
	}
	file := os.NewFile(uintptr(fd), "zoe-provider-test")
	defer file.Close()
	if _, err := file.WriteString(marker); err != nil {
		t.Fatal(err)
	}
	if err := file.Chmod(0400); err != nil {
		t.Fatal(err)
	}

	profile := &CodexExternalProfile{APIKeyFile: filepath.Join("/proc", strconv.Itoa(os.Getpid()), "fd", strconv.Itoa(fd))}
	got, err := profile.readAPIKey()
	if err != nil || got != marker {
		t.Fatalf("read private memfd key = %q, err=%v", got, err)
	}
}
