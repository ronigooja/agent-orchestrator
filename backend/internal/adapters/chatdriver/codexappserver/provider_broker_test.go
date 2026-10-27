package codexappserver

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestProviderBrokerKeepsCredentialHostSideAndRestrictsRoutes(t *testing.T) {
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		t.Fatal(err)
	}
	apiKey := base64.RawURLEncoding.EncodeToString(secret)
	var calls int
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if got := r.Header.Get("Authorization"); got != "Bearer "+apiKey {
			t.Errorf("upstream authorization did not match host-held key")
		}
		if strings.HasSuffix(r.URL.Path, "/v1/models") {
			_, _ = io.WriteString(w, `{"object":"list","data":[{"id":"gpt-6-luna"},{"id":"other-model"}]}`)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "data: provider-response\n\n")
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
	}))
	defer upstream.Close()

	broker, err := startProviderBroker(upstream.URL+"/gateway", apiKey, "gpt-6-luna")
	if err != nil {
		t.Fatal(err)
	}
	defer broker.Close()
	transport := &http.Transport{DialContext: func(context.Context, string, string) (net.Conn, error) {
		return net.Dial("unix", broker.SocketPath())
	}}
	client := &http.Client{Transport: transport, Timeout: 3 * time.Second}
	response, err := client.Get("http://provider/v1/models")
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil || response.StatusCode != http.StatusOK || !strings.Contains(string(body), "gpt-6-luna") || strings.Contains(string(body), "other-model") {
		t.Fatalf("models proxy status/body = %d/%q, err=%v", response.StatusCode, body, err)
	}

	response, err = client.Get("http://provider/v1/secret")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusNotFound || calls != 1 {
		t.Fatalf("unapproved route status/calls = %d/%d", response.StatusCode, calls)
	}

	response, err = client.Post("http://provider/v1/responses", "application/json", strings.NewReader(`{"model":"gpt-6-luna"}`))
	if err != nil {
		t.Fatal(err)
	}
	body, err = io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil || response.StatusCode != http.StatusOK || !strings.Contains(string(body), "provider-response") {
		t.Fatalf("stream proxy status/body = %d/%q, err=%v", response.StatusCode, body, err)
	}
	if calls != 2 {
		t.Fatalf("upstream calls = %d, want two allowed calls", calls)
	}
}

func TestProviderBrokerBindsBothResponsesEndpointsToDeploymentModel(t *testing.T) {
	const selectedModel = "gpt-6-luna"
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		t.Fatal(err)
	}
	apiKey := base64.RawURLEncoding.EncodeToString(secret)
	var calls int
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Header.Get("Authorization") != "Bearer "+apiKey {
			t.Error("upstream authorization did not match host-held key")
		}
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok")
	}))
	defer upstream.Close()
	broker, err := startProviderBroker(upstream.URL, apiKey, selectedModel)
	if err != nil {
		t.Fatal(err)
	}
	defer broker.Close()
	transport := &http.Transport{DialContext: func(context.Context, string, string) (net.Conn, error) { return net.Dial("unix", broker.SocketPath()) }}
	client := &http.Client{Transport: transport, Timeout: 3 * time.Second}
	for _, test := range []struct {
		path string
		body string
		want int
	}{
		{"/v1/responses", `{"model":"gpt-6-luna","input":"x"}`, http.StatusOK},
		{"/v1/responses/compact", `{"model":"gpt-6-luna","input":[]}`, http.StatusOK},
		{"/v1/responses", `{"model":"other-model"}`, http.StatusForbidden},
		{"/v1/responses", `{"model":"gpt-6-luna","model":"other-model"}`, http.StatusForbidden},
		{"/v1/responses", `{"model":null}`, http.StatusForbidden},
		{"/v1/responses", `{"input":"missing model"}`, http.StatusForbidden},
		{"/v1/responses/compact", `{"input":[]}`, http.StatusForbidden},
		{"/v1/responses/compact", `{"model":"other-model","input":[]}`, http.StatusForbidden},
	} {
		request, err := http.NewRequest(http.MethodPost, "http://provider"+test.path, strings.NewReader(test.body))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Content-Type", "application/json")
		response, err := client.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
		if response.StatusCode != test.want {
			t.Errorf("%s status=%d, want %d", test.path, response.StatusCode, test.want)
		}
	}
	if calls != 2 {
		t.Fatalf("upstream calls=%d, want only the two selected-model requests", calls)
	}
	for _, path := range []string{"/v1/responses?alt=other", "/v1/responses/../responses", "/v1/responses/%2fcompact"} {
		request, err := http.NewRequest(http.MethodPost, "http://provider"+path, strings.NewReader(`{"model":"gpt-6-luna"}`))
		if err != nil {
			t.Fatal(err)
		}
		response, err := client.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
		if response.StatusCode != http.StatusNotFound {
			t.Errorf("route %q status=%d, want %d", path, response.StatusCode, http.StatusNotFound)
		}
	}
	if calls != 2 {
		t.Fatalf("denied requests reached upstream; calls=%d", calls)
	}
}

func TestProviderBrokerRejectsMissingDeploymentModel(t *testing.T) {
	if _, err := startProviderBroker("https://gateway.invalid/v1", "nonempty-placeholder", ""); err == nil {
		t.Fatal("broker accepted missing deployment model")
	}
}
