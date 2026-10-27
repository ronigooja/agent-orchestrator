package codexappserver

import (
    "path/filepath"
    "strings"
    "testing"
)

func TestDeploymentSocketUsesDaemonVisibleRoot(t *testing.T) {
    root := t.TempDir()
    broker, err := startProviderBroker("https://example.invalid/v1", "synthetic-key", "synthetic-model", root)
    if err != nil { t.Fatal(err) }
    defer broker.Close()
    relative, err := filepath.Rel(root, broker.SocketPath())
    if err != nil || strings.HasPrefix(relative, "..") { t.Fatal("socket escaped daemon-visible root") }
}
