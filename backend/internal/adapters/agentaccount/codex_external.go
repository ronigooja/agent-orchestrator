package agentaccount

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const codexExternalAPIKeyEnv = "ZOE_CODEX_API_KEY"
const codexExternalProfileErrorEnv = "ZOE_CODEX_PROFILE_ERROR"
const codexExternalModelEnv = "ZOE_CODEX_PROXY_MODEL"

// CodexExternalProfile is a deployment-selected OpenAI-compatible Responses
// API route. Its CODEX_HOME is AO-owned and separate from the operator's home.
// The API key stays in a private deployment file and is never serialized into
// AO requests, Codex config.toml, or logs.
type CodexExternalProfile struct {
	BaseURL        string
	APIKeyFile     string
	Model          string
	ContainerImage string
	Home           string
}

// NewCodexExternalProfile returns nil when the external route is not
// configured. A partial or malformed configuration returns an error so AO can
// fail closed rather than falling back to another account or provider.
func NewCodexExternalProfile(stateDir, baseURL, apiKeyFile, model string) (*CodexExternalProfile, error) {
	baseURL, apiKeyFile = strings.TrimSpace(baseURL), strings.TrimSpace(apiKeyFile)
	model = strings.TrimSpace(model)
	if baseURL == "" && apiKeyFile == "" && model == "" {
		return nil, nil
	}
	if baseURL == "" || apiKeyFile == "" || model == "" || !filepath.IsAbs(stateDir) || !filepath.IsAbs(apiKeyFile) {
		return nil, errors.New("external Codex provider requires an absolute AO state directory, HTTPS base URL, absolute secret-file path, and deployment-selected model")
	}
	u, err := url.Parse(baseURL)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Scheme != "https" && !(u.Scheme == "http" && (u.Hostname() == "127.0.0.1" || u.Hostname() == "localhost" || u.Hostname() == "::1"))) {
		return nil, errors.New("external Codex provider base URL must use HTTPS or local HTTP without credentials, query, or fragment")
	}
	return &CodexExternalProfile{BaseURL: strings.TrimRight(u.String(), "/"), APIKeyFile: apiKeyFile, Model: model, Home: filepath.Join(stateDir, "harnesses", "codex", "external-provider")}, nil
}

func (p *CodexExternalProfile) Configured() bool { return p != nil }

// Check performs only the provider's read-only models probe. It proves the
// selected model is advertised by this configured route; it does not claim a
// ChatGPT account identity or usage subscription.
func (p *CodexExternalProfile) Check(ctx context.Context, model string) error {
	if p == nil {
		return errors.New("external Codex provider is not configured")
	}
	model = strings.TrimSpace(model)
	if model == "" {
		return errors.New("external Codex provider requires an explicit model")
	}
	if model != p.Model {
		return errors.New("external Codex provider does not permit the requested model")
	}
	key, err := p.readAPIKey()
	if err != nil {
		return errors.New("external Codex provider credential is unavailable")
	}
	endpoint := strings.TrimRight(p.BaseURL, "/")
	if !strings.HasSuffix(endpoint, "/v1") {
		endpoint += "/v1"
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"/models", nil)
	if err != nil {
		return errors.New("external Codex provider probe could not be prepared")
	}
	request.Header.Set("Authorization", "Bearer "+key)
	client := &http.Client{Timeout: 8 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := client.Do(request)
	if err != nil {
		return errors.New("external Codex provider models probe failed")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return errors.New("external Codex provider models probe was rejected")
	}
	var catalog struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&catalog); err != nil {
		return errors.New("external Codex provider returned an invalid models catalog")
	}
	for _, item := range catalog.Data {
		if item.ID == model {
			return nil
		}
	}
	return errors.New("external Codex provider does not advertise the selected model")
}

// RuntimeEnvForSession writes a separate, private Codex home for one AO
// session. The session id is hashed before it becomes part of a filesystem
// path, and the credential remains an environment reference rather than
// config.toml content.
func (p *CodexExternalProfile) RuntimeEnvForSession(sessionID string) map[string]string {
	if p == nil {
		return nil
	}
	home := p.sessionHome(sessionID)
	env := map[string]string{
		"CODEX_HOME":                 home,
		"ZOE_CODEX_SESSION_ID":      sessionID,
		"ZOE_CODEX_PROXY_UPSTREAM":  p.BaseURL,
		codexExternalModelEnv:        p.Model,
		codexExternalProfileErrorEnv: "",
	}
	if strings.TrimSpace(p.ContainerImage) != "" {
		env["AO_CODEX_CONTAINER_IMAGE"] = strings.TrimSpace(p.ContainerImage)
	}
	if err := p.writeConfig(home); err != nil {
		env[codexExternalProfileErrorEnv] = "profile_config_unavailable"
		return env
	}
	key, err := p.readAPIKey()
	if err != nil {
		env[codexExternalProfileErrorEnv] = "provider_credential_unavailable"
		return env
	}
	env[codexExternalAPIKeyEnv] = key
	return env
}

func (p *CodexExternalProfile) sessionHome(sessionID string) string {
	if strings.TrimSpace(sessionID) == "" {
		sessionID = "default"
	}
	sum := sha256.Sum256([]byte(sessionID))
	return filepath.Join(p.Home, "runs", hex.EncodeToString(sum[:16]))
}

func (p *CodexExternalProfile) readAPIKey() (string, error) {
	info, err := os.Lstat(p.APIKeyFile)
	if err != nil {
		return "", errors.New("secret file must be a private regular file")
	}
	if info.Mode().IsRegular() {
		if info.Mode().Perm()&0077 != 0 {
			return "", errors.New("secret file must be a private regular file")
		}
	} else if !privateMemfdPath(p.APIKeyFile, info) {
		return "", errors.New("secret file must be a private regular file")
	}
	data, err := os.ReadFile(p.APIKeyFile)
	if err != nil {
		return "", err
	}
	key := strings.TrimSpace(string(data))
	if key == "" || len(key) > 16<<10 || strings.ContainsAny(key, "\r\n\x00") {
		return "", errors.New("secret file content is invalid")
	}
	return key, nil
}

// privateMemfdPath permits a Linux launcher to lend AO a private in-memory
// secret without writing it to disk. The descriptor must belong to this user,
// have restrictive permissions, and resolve to a memfd (never an arbitrary
// /proc descriptor or symlink).
func privateMemfdPath(path string, linkInfo os.FileInfo) bool {
	if runtime.GOOS != "linux" || linkInfo == nil || linkInfo.Mode()&os.ModeSymlink == 0 {
		return false
	}
	parts := strings.Split(filepath.Clean(path), string(os.PathSeparator))
	if len(parts) != 5 || parts[1] != "proc" || parts[3] != "fd" {
		return false
	}
	pid, pidErr := strconv.Atoi(parts[2])
	fd, fdErr := strconv.Atoi(parts[4])
	if pidErr != nil || fdErr != nil || pid < 1 || fd < 0 {
		return false
	}
	target, err := os.Readlink(path)
	if err != nil || !strings.HasPrefix(target, "/memfd:") {
		return false
	}
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return false
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Uid == uint32(os.Getuid()) && info.Mode().Perm()&0077 == 0
}

func (p *CodexExternalProfile) writeConfig(home string) error {
	return p.writeConfigAt(home, p.BaseURL, true)
}

func (p *CodexExternalProfile) writeConfigAt(home, baseURL string, includeEnvKey bool) error {
	if p == nil || !filepath.IsAbs(home) {
		return errors.New("external Codex profile home must be absolute")
	}
	if err := os.MkdirAll(home, 0700); err != nil {
		return err
	}
	if err := os.Chmod(home, 0700); err != nil {
		return err
	}
	info, err := os.Lstat(home)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("external Codex profile home must be a real private directory")
	}
	configPath := filepath.Join(home, "config.toml")
	if existing, err := os.Lstat(configPath); err == nil && (existing.Mode()&os.ModeSymlink != 0 || !existing.Mode().IsRegular()) {
		return errors.New("external Codex config must be a regular file")
	}
	contents := fmt.Sprintf("model_provider = %q\n\n[model_providers.zoe_external]\nname = %q\nbase_url = %q\nwire_api = %q\nrequires_openai_auth = false\n", "zoe_external", "Zoe external API", baseURL, "responses")
	if includeEnvKey {
		contents += fmt.Sprintf("env_key = %q\n", codexExternalAPIKeyEnv)
	}
	tmp, err := os.CreateTemp(home, ".config.toml-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(0600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.WriteString(contents); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, configPath); err != nil {
		return err
	}
	return nil
}
