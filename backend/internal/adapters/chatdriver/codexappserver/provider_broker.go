package codexappserver

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// providerBroker keeps the deployment credential in the AO daemon. The
// worker container can reach only this per-session Unix socket, and the
// container-side bridge exposes only the fixed Responses provider routes on
// loopback.
type providerBroker struct {
	server   *http.Server
	listener net.Listener
	root     string
}

func startProviderBroker(upstream, key, selectedModel string, socketRoots ...string) (*providerBroker, error) {
	upstream, key = strings.TrimRight(strings.TrimSpace(upstream), "/"), strings.TrimSpace(key)
	selectedModel = strings.TrimSpace(selectedModel)
	if key == "" || selectedModel == "" || strings.ContainsAny(selectedModel, "\r\n\x00") {
		return nil, errors.New("Codex provider bridge credential or selected model is unavailable")
	}
	target, err := url.Parse(upstream)
	if err != nil || target.Host == "" || target.User != nil || target.RawQuery != "" || target.Fragment != "" || (target.Scheme != "https" && !(target.Scheme == "http" && (target.Hostname() == "127.0.0.1" || target.Hostname() == "localhost" || target.Hostname() == "::1"))) {
		return nil, errors.New("Codex provider bridge requires a fixed HTTPS or loopback HTTP endpoint")
	}
	if !strings.HasSuffix(strings.TrimRight(target.Path, "/"), "/v1") && target.Path != "/v1" {
		target.Path = strings.TrimRight(target.Path, "/") + "/v1"
	}
	socketRoot := os.TempDir()
	if len(socketRoots) > 0 {
		socketRoot = socketRoots[0]
	}
	root, err := os.MkdirTemp(socketRoot, "p-")
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(root, 0700); err != nil {
		_ = os.RemoveAll(root)
		return nil, err
	}
	socket := filepath.Join(root, "s")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		_ = os.RemoveAll(root)
		return nil, err
	}
	if err := os.Chmod(socket, 0600); err != nil {
		_ = listener.Close()
		_ = os.RemoveAll(root)
		return nil, err
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.Transport = &http.Transport{Proxy: nil, ResponseHeaderTimeout: 2 * time.Minute, DisableKeepAlives: true}
	proxy.Director = nil
	modelsPath := strings.TrimRight(target.Path, "/") + "/models"
	proxy.ModifyResponse = func(response *http.Response) error {
		if response.Request != nil && response.Request.Method == http.MethodGet && response.Request.URL.Path == modelsPath {
			return filterModelCatalog(response, selectedModel)
		}
		return nil
	}
	proxy.Rewrite = func(pr *httputil.ProxyRequest) {
		pr.SetURL(target)
		resource := strings.TrimPrefix(pr.In.URL.Path, "/v1")
		if resource == "" {
			resource = "/"
		}
		pr.Out.URL.Path = strings.TrimRight(target.Path, "/") + resource
		pr.Out.URL.RawPath = ""
		pr.Out.Host = target.Host
		pr.Out.Header.Set("Authorization", "Bearer "+key)
		pr.Out.Header.Del("Proxy-Authorization")
	}
	server := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.RawQuery != "" || r.URL.ForceQuery || r.URL.EscapedPath() != r.URL.Path {
				http.Error(w, "provider route denied", http.StatusNotFound)
				return
			}
			if !((r.Method == http.MethodGet && r.URL.Path == "/v1/models") || (r.Method == http.MethodPost && (r.URL.Path == "/v1/responses" || r.URL.Path == "/v1/responses/compact"))) {
				http.Error(w, "provider route denied", http.StatusNotFound)
				return
			}
			if r.Method == http.MethodPost {
				r.Body = http.MaxBytesReader(w, r.Body, 32<<20)
				body, err := io.ReadAll(r.Body)
				if err != nil {
					http.Error(w, "provider request rejected", http.StatusBadRequest)
					return
				}
				if err := requireSelectedModel(body, selectedModel); err != nil {
					http.Error(w, "provider model denied", http.StatusForbidden)
					return
				}
				r.Body = io.NopCloser(bytes.NewReader(body))
				r.ContentLength = int64(len(body))
			}
			proxy.ServeHTTP(w, r)
		}),
		ReadHeaderTimeout: 10 * time.Second,
	}
	b := &providerBroker{server: server, listener: listener, root: root}
	go func() { _ = server.Serve(listener) }()
	return b, nil
}

// requireSelectedModel rejects malformed or ambiguous top-level fields so a
// duplicate model key cannot smuggle a second destination through a decoder.
func requireSelectedModel(body []byte, selectedModel string) error {
	fields, err := decodeUniqueJSONObject(body)
	if err != nil {
		return err
	}
	rawModel, ok := fields["model"]
	if !ok {
		return errors.New("Codex request omitted model")
	}
	var model string
	if err := json.Unmarshal(rawModel, &model); err != nil || model != selectedModel {
		return errors.New("Codex request model is not the selected deployment model")
	}
	return nil
}

func decodeUniqueJSONObject(body []byte) (map[string]json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(body))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return nil, errors.New("Codex request must be a JSON object")
	}
	fields := make(map[string]json.RawMessage)
	for decoder.More() {
		token, err = decoder.Token()
		if err != nil {
			return nil, errors.New("Codex request JSON is invalid")
		}
		key, ok := token.(string)
		if !ok {
			return nil, errors.New("Codex request JSON is invalid")
		}
		if _, exists := fields[key]; exists {
			return nil, errors.New("Codex request has duplicate fields")
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, errors.New("Codex request JSON is invalid")
		}
		fields[key] = value
	}
	if token, err = decoder.Token(); err != nil || token != json.Delim('}') {
		return nil, errors.New("Codex request JSON is invalid")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, errors.New("Codex request has trailing data")
	}
	return fields, nil
}

func filterModelCatalog(response *http.Response, selectedModel string) error {
	const maxCatalogBytes = 1 << 20
	if response.StatusCode != http.StatusOK {
		return nil
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxCatalogBytes+1))
	if err != nil || len(body) > maxCatalogBytes {
		return errors.New("Codex provider model catalog is invalid")
	}
	fields, err := decodeUniqueJSONObject(body)
	if err != nil {
		return errors.New("Codex provider model catalog is invalid")
	}
	var items []json.RawMessage
	rawItems, ok := fields["data"]
	if !ok || json.Unmarshal(rawItems, &items) != nil {
		return errors.New("Codex provider model catalog is invalid")
	}
	filtered := make([]json.RawMessage, 0, 1)
	for _, item := range items {
		entry, err := decodeUniqueJSONObject(item)
		if err != nil {
			return errors.New("Codex provider model catalog is invalid")
		}
		var id string
		if rawID, ok := entry["id"]; ok && json.Unmarshal(rawID, &id) == nil && id == selectedModel {
			filtered = append(filtered, item)
		}
	}
	if len(filtered) != 1 {
		return errors.New("Codex provider model catalog does not contain exactly one selected model")
	}
	data, err := json.Marshal(filtered)
	if err != nil {
		return errors.New("Codex provider model catalog is invalid")
	}
	filteredBody, err := json.Marshal(map[string]json.RawMessage{"data": data})
	if err != nil {
		return errors.New("Codex provider model catalog is invalid")
	}
	_ = response.Body.Close()
	response.Body = io.NopCloser(bytes.NewReader(filteredBody))
	response.ContentLength = int64(len(filteredBody))
	response.Header.Set("Content-Length", fmt.Sprint(len(filteredBody)))
	response.Header.Del("Content-Encoding")
	response.Header.Del("Transfer-Encoding")
	response.TransferEncoding = nil
	response.Uncompressed = false
	return nil
}

func (b *providerBroker) SocketPath() string {
	if b == nil {
		return ""
	}
	return filepath.Join(b.root, "s")
}

func (b *providerBroker) Close() error {
	if b == nil {
		return nil
	}
	_ = b.server.Close()
	if err := b.listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		return err
	}
	if err := os.RemoveAll(b.root); err != nil {
		return fmt.Errorf("remove Codex provider bridge socket: %w", err)
	}
	return nil
}
