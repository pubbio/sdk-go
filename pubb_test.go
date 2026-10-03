package pubb_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pubb "github.com/pubbio/sdk-go"
)

const receipt = `{"success":true,"accepted":true,"channels":["updates"],"publicationId":"pub-1","duplicate":false}`

func client(t *testing.T, host string, httpClient *http.Client) *pubb.Client {
	t.Helper()
	c, err := pubb.NewClient(pubb.Config{AppID: "app/id", AppKey: "public-key", AppSecret: "test-secret", APIHost: host, HTTPClient: httpClient})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestPublishContract(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.RequestURI != "/api/apps/app%2Fid/events" || r.Method != "POST" {
			t.Errorf("invalid endpoint: %s %s", r.Method, r.RequestURI)
		}
		if r.Header.Get("Authorization") != "Bearer test-secret" || r.Header.Get("Idempotency-Key") != "event-1" {
			t.Error("missing request headers")
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if body["name"] != "message" || body["socket_id"] != "1.2" || body["data"].(map[string]any)["text"] != "Merhaba 🌍" || len(body["channel"].([]any)) != 2 {
			t.Errorf("unexpected body: %#v", body)
		}
		w.WriteHeader(202)
		fmt.Fprint(w, receipt)
	}))
	defer srv.Close()
	result, err := client(t, srv.URL, nil).Trigger(context.Background(), []string{"updates", "room"}, "message", map[string]string{"text": "Merhaba 🌍"}, &pubb.TriggerOptions{SocketID: "1.2", IdempotencyKey: "event-1"})
	if err != nil || result.PublicationID != "pub-1" || result.Duplicate {
		t.Fatalf("%+v %v", result, err)
	}
}

func TestErrorStatusAndNoRetry(t *testing.T) {
	var count atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count.Add(1)
		w.Header().Set("Retry-After", "5")
		w.WriteHeader(429)
		fmt.Fprint(w, "sensitive body")
	}))
	defer srv.Close()
	_, err := client(t, srv.URL, nil).Trigger(context.Background(), []string{"updates"}, "event", nil, nil)
	var api *pubb.APIError
	if !errors.As(err, &api) || api.StatusCode != 429 || api.RetryAfter != "5" || strings.Contains(err.Error(), "sensitive") || count.Load() != 1 {
		t.Fatal(err)
	}
}

func TestRedirectRejectedAndCallerUnmodified(t *testing.T) {
	var redirected atomic.Bool
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { redirected.Store(true) }))
	defer target.Close()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, target.URL, 307) }))
	defer srv.Close()
	custom := &http.Client{}
	_, err := client(t, srv.URL, custom).Trigger(context.Background(), []string{"updates"}, "event", nil, nil)
	var api *pubb.APIError
	if !errors.As(err, &api) || api.StatusCode != 307 || redirected.Load() || custom.CheckRedirect != nil {
		t.Fatal("redirect leaked or modified caller", err)
	}
}

func TestCancellation(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { close(entered); <-release }))
	defer srv.Close()
	defer close(release)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	c := client(t, srv.URL, nil)
	go func() { _, err := c.Trigger(ctx, []string{"updates"}, "event", nil, nil); done <- err }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("request did not arrive")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancellation ignored")
	}
}

func TestInvalidAcceptance(t *testing.T) {
	for _, body := range []string{"{}", "[]", "invalid", strings.Replace(receipt, `"duplicate":false`, `"extra":true`, 1), strings.Replace(receipt, `"accepted":true`, `"accepted":false`, 1)} {
		t.Run(body, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(202); fmt.Fprint(w, body) }))
			defer srv.Close()
			_, err := client(t, srv.URL, nil).Trigger(context.Background(), []string{"updates"}, "event", nil, nil)
			if !errors.Is(err, pubb.ErrInvalidResponse) {
				t.Fatal(err)
			}
		})
	}
}

func TestOnly202(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, receipt) }))
	defer srv.Close()
	_, err := client(t, srv.URL, nil).Trigger(context.Background(), []string{"updates"}, "event", nil, nil)
	var api *pubb.APIError
	if !errors.As(err, &api) || api.StatusCode != 200 {
		t.Fatal(err)
	}
}

func TestDuplicateReceipt(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(202)
		fmt.Fprint(w, strings.Replace(receipt, `"duplicate":false`, `"duplicate":true`, 1))
	}))
	defer srv.Close()
	result, err := client(t, srv.URL, nil).Trigger(context.Background(), []string{"updates"}, "event", nil, nil)
	if err != nil || !result.Duplicate {
		t.Fatal(result, err)
	}
}

func TestValidation(t *testing.T) {
	c := client(t, "http://127.0.0.1:1", nil)
	for _, test := range []struct {
		channels []string
		name     string
		data     any
		opts     *pubb.TriggerOptions
	}{
		{nil, "event", nil, nil}, {[]string{"debug-room"}, "event", nil, nil}, {[]string{"_room"}, "event", nil, nil},
		{[]string{"updates"}, "", nil, nil}, {[]string{"updates"}, "event", strings.Repeat("é", 5120), nil},
		{[]string{"updates"}, "event", math.NaN(), nil}, {[]string{"updates"}, "event", nil, &pubb.TriggerOptions{IdempotencyKey: "bad key"}},
	} {
		if _, err := c.Trigger(context.Background(), test.channels, test.name, test.data, test.opts); err == nil {
			t.Fatal("expected validation error")
		}
	}
}

func TestSigningVectors(t *testing.T) {
	c := client(t, "https://api.pubb.io", nil)
	private, err := c.AuthorizeChannel("123.456", "private-room")
	if err != nil || private.Auth != "public-key:f3312a32998c7c174fb5d44146dad485d980398fe616181bf1f758764ccf603a" {
		t.Fatal(private, err)
	}
	presence, err := c.AuthorizePresenceChannel("123.456", "presence-room", "user-1", map[string]string{"name": "Ada"})
	if err != nil || presence.Auth != "public-key:41d9f92ca8a5496a5f346458be7d75c0d79bce9928260f8d139330140140d939" || presence.ChannelData != `{"user_id":"user-1","user_info":{"name":"Ada"}}` {
		t.Fatal(presence, err)
	}
	if _, err = c.AuthorizeChannel("1.2", "presence-room"); err == nil {
		t.Fatal("expected private prefix validation")
	}
	if _, err = c.AuthorizePresenceChannel("1.2", "private-room", "u", nil); err == nil {
		t.Fatal("expected presence prefix validation")
	}
	if strings.Contains(fmt.Sprintf("%+v %#v", c, c), "test-secret") {
		t.Fatal("client formatting exposed secret")
	}
}

func TestConfiguration(t *testing.T) {
	for _, host := range []string{"ftp://example.com", "https://user:pass@example.com", "https://example.com?q=1"} {
		if _, err := pubb.NewClient(pubb.Config{AppID: "id", AppKey: "key", AppSecret: "secret", APIHost: host}); err == nil {
			t.Fatal("accepted bad URL")
		}
	}
}
