package agentaccount

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCodexExternalProfileUsesPrivateIsolatedHomeAndNamedEnvKey(t *testing.T) {
	apiKey := externalTestKey(t)
	state := t.TempDir()
	oldHome := t.TempDir()
	oldConfig := filepath.Join(oldHome, "config.toml")
	if err := os.WriteFile(oldConfig, []byte("model_provider = \"openai\"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	keyFile := filepath.Join(t.TempDir(), "external-key")
	if err := os.WriteFile(keyFile, []byte(apiKey+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	profile, err := NewCodexExternalProfile(state, "https://gateway.example/v1", keyFile, "gpt-6-luna")
	if err != nil {
		t.Fatal(err)
	}
	profile.ContainerImage = "zoe-ao-codex:test"
	if profile.Home == oldHome || !strings.HasPrefix(profile.Home, state) {
		t.Fatalf("profile home=%q old home=%q", profile.Home, oldHome)
	}
	env := profile.RuntimeEnvForSession("session one")
	second := profile.RuntimeEnvForSession("session/two")
	if env["CODEX_HOME"] == second["CODEX_HOME"] || !strings.HasPrefix(env["CODEX_HOME"], profile.Home+string(os.PathSeparator)+"runs"+string(os.PathSeparator)) || env[codexExternalAPIKeyEnv] != apiKey || env[codexExternalModelEnv] != profile.Model || env["AO_CODEX_CONTAINER_IMAGE"] != profile.ContainerImage {
		t.Fatalf("runtime environment is incomplete; keys=%v", externalKeys(env))
	}
	config, err := os.ReadFile(filepath.Join(env["CODEX_HOME"], "config.toml"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(config)
	for _, want := range []string{"model_provider = \"zoe_external\"", "wire_api = \"responses\"", "env_key = \"ZOE_CODEX_API_KEY\"", "https://gateway.example/v1"} {
		if !strings.Contains(text, want) {
			t.Fatalf("config missing %q: %s", want, text)
		}
	}
	if strings.Contains(text, apiKey) {
		t.Fatal("secret value was written into Codex config")
	}
	if got, err := os.ReadFile(oldConfig); err != nil || string(got) != "model_provider = \"openai\"\n" {
		t.Fatalf("existing CODEX_HOME config changed: %q err=%v", got, err)
	}
	info, err := os.Stat(filepath.Join(env["CODEX_HOME"], "config.toml"))
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("profile config permissions=%v err=%v", info, err)
	}
	codex := &Codex{External: profile}
	loginEnv := codex.LaunchEnv(oldHome)
	if loginEnv["CODEX_HOME"] != oldHome || loginEnv[codexExternalAPIKeyEnv] != "" {
		t.Fatalf("account login was redirected to the external route: keys=%v", loginEnv)
	}
	workerEnv := codex.LaunchEnvForSession(oldHome, "session one")
	if workerEnv["CODEX_HOME"] != env["CODEX_HOME"] || workerEnv[codexExternalAPIKeyEnv] != apiKey {
		t.Fatalf("worker external route is incomplete: keys=%v", externalKeys(workerEnv))
	}
}

func TestCodexExternalProfileChecksReadOnlyAdvertisedModel(t *testing.T) {
	apiKey := externalTestKey(t)
	keyFile := filepath.Join(t.TempDir(), "provider-key")
	if err := os.WriteFile(keyFile, []byte(apiKey), 0600); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/models" {
			t.Errorf("request=%s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer "+apiKey {
			t.Errorf("authorization header was not set")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[{"id":"gpt-6-luna"}]}`))
	}))
	defer server.Close()
	profile, err := NewCodexExternalProfile(t.TempDir(), server.URL+"/v1", keyFile, "gpt-6-luna")
	if err != nil {
		t.Fatal(err)
	}
	if err := profile.Check(context.Background(), "gpt-6-luna"); err != nil {
		t.Fatalf("models probe: %v", err)
	}
	if err := profile.Check(context.Background(), "missing-model"); err == nil {
		t.Fatal("model outside deployment selection accepted")
	}
	if err := profile.Check(context.Background(), " gpt-6-luna "); err != nil {
		t.Fatalf("trimmed selected model rejected: %v", err)
	}
}

func externalTestKey(t *testing.T) string {
	t.Helper()
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		t.Fatal(err)
	}
	return base64.RawURLEncoding.EncodeToString(raw)
}

func externalKeys(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	return keys
}

func TestCodexExternalProfileRejectsInsecureOrIncompleteConfiguration(t *testing.T) {
	if profile, err := NewCodexExternalProfile(t.TempDir(), "", "/tmp/key", "gpt-6-luna"); err == nil || profile != nil {
		t.Fatalf("partial config = (%v,%v)", profile, err)
	}
	if profile, err := NewCodexExternalProfile(t.TempDir(), "http://gateway.example", "/tmp/key", "gpt-6-luna"); err == nil || profile != nil {
		t.Fatalf("remote HTTP config = (%v,%v)", profile, err)
	}
}

func TestCodexExternalProfileRejectsReadableSecretFile(t *testing.T) {
	apiKey := externalTestKey(t)
	keyFile := filepath.Join(t.TempDir(), "provider-key")
	if err := os.WriteFile(keyFile, []byte(apiKey), 0644); err != nil {
		t.Fatal(err)
	}
	profile, err := NewCodexExternalProfile(t.TempDir(), "https://gateway.example/v1", keyFile, "gpt-6-luna")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := profile.readAPIKey(); err == nil {
		t.Fatal("group-readable provider secret accepted")
	}
}

func TestCodexExternalProfileMarksCredentialReadFailureWithoutLosingRouteMarker(t *testing.T) {
	profile, err := NewCodexExternalProfile(t.TempDir(), "https://gateway.example/v1", filepath.Join(t.TempDir(), "missing-secret"), "gpt-6-luna")
	if err != nil {
		t.Fatal(err)
	}
	profile.ContainerImage = "zoe-ao-codex:test"
	env := profile.RuntimeEnvForSession("session-e2e")
	if env["ZOE_CODEX_PROXY_UPSTREAM"] != profile.BaseURL || env["AO_CODEX_CONTAINER_IMAGE"] != profile.ContainerImage || env[codexExternalProfileErrorEnv] != "provider_credential_unavailable" {
		t.Fatalf("failed profile did not preserve fail-closed markers: keys=%v", externalKeys(env))
	}
	if _, ok := env[codexExternalAPIKeyEnv]; ok {
		t.Fatal("provider credential appeared after secret read failure")
	}
}
