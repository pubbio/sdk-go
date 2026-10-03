// Package pubb publishes events and signs channel authorization on trusted servers.
package pubb

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode/utf16"
)

// Config contains server credentials. Do not ship AppSecret in client apps.
type Config struct {
	AppID, AppKey, AppSecret string
	APIHost                  string       // Defaults to https://api.pubb.io.
	HTTPClient               *http.Client // Optional. Shallow copied; its transport remains caller-owned.
}

// Client is safe for concurrent use and never retries publications automatically.
type Client struct {
	key, secret, endpoint string
	http                  *http.Client
}

func (c *Client) String() string   { return "pubb.Client" }
func (c *Client) GoString() string { return c.String() }

// NewClient validates configuration. Redirects are disabled even on a supplied client.
func NewClient(config Config) (*Client, error) {
	for field, value := range map[string]string{"AppID": config.AppID, "AppKey": config.AppKey, "AppSecret": config.AppSecret} {
		if strings.TrimSpace(value) == "" {
			return nil, fmt.Errorf("pubb: %s is required", field)
		}
	}
	host := config.APIHost
	if host == "" {
		host = "https://api.pubb.io"
	}
	u, err := url.Parse(host)
	if err != nil || u == nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("pubb: APIHost must be an HTTP(S) URL without credentials, query or fragment")
	}
	httpClient := http.Client{Timeout: 30 * time.Second}
	if config.HTTPClient != nil {
		httpClient = *config.HTTPClient
	}
	httpClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &Client{key: config.AppKey, secret: config.AppSecret,
		endpoint: strings.TrimRight(host, "/") + "/api/apps/" + url.PathEscape(config.AppID) + "/events", http: &httpClient}, nil
}

type TriggerOptions struct {
	SocketID       string
	IdempotencyKey string
}

// TriggerResponse is an acceptance receipt, not confirmation of delivery.
type TriggerResponse struct {
	Success       bool     `json:"success"`
	Accepted      bool     `json:"accepted"`
	Channels      []string `json:"channels"`
	PublicationID string   `json:"publicationId"`
	Duplicate     bool     `json:"duplicate"`
}

type APIError struct {
	StatusCode int
	RetryAfter string
}

func (e *APIError) Error() string { return fmt.Sprintf("pubb: API returned HTTP %d", e.StatusCode) }

var ErrInvalidResponse = errors.New("pubb: API returned an invalid acceptance response")

func validString(s string, limit int) bool {
	return strings.TrimSpace(s) != "" && len(utf16.Encode([]rune(s))) <= limit
}

// Trigger sends one event to 1–100 channels. Reuse an idempotency key for retries.
// Context cancellation and the configured HTTP client timeout bound the request.
func (c *Client) Trigger(ctx context.Context, channels []string, name string, data any, options *TriggerOptions) (*TriggerResponse, error) {
	if len(channels) < 1 || len(channels) > 100 {
		return nil, errors.New("pubb: use 1 to 100 channels")
	}
	for _, channel := range channels {
		if !validString(channel, 200) || strings.HasPrefix(channel, "debug-") || strings.HasPrefix(channel, "_") {
			return nil, errors.New("pubb: invalid or reserved channel name")
		}
	}
	if !validString(name, 200) {
		return nil, errors.New("pubb: invalid event name")
	}
	payload, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	if len(payload) > 10240 {
		return nil, errors.New("pubb: data exceeds 10 KiB of JSON")
	}
	opts := TriggerOptions{}
	if options != nil {
		opts = *options
	}
	if len(utf16.Encode([]rune(opts.SocketID))) > 100 {
		return nil, errors.New("pubb: socket ID exceeds 100 characters")
	}
	if opts.IdempotencyKey != "" {
		if len(opts.IdempotencyKey) > 128 {
			return nil, errors.New("pubb: invalid idempotency key")
		}
		for _, b := range []byte(opts.IdempotencyKey) {
			if b < 33 || b > 126 {
				return nil, errors.New("pubb: invalid idempotency key")
			}
		}
	}
	body, err := json.Marshal(struct {
		Channels []string        `json:"channel"`
		Name     string          `json:"name"`
		Data     json.RawMessage `json:"data"`
		SocketID string          `json:"socket_id,omitempty"`
	}{channels, name, payload, opts.SocketID})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.secret)
	req.Header.Set("Content-Type", "application/json")
	if opts.IdempotencyKey != "" {
		req.Header.Set("Idempotency-Key", opts.IdempotencyKey)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		return nil, &APIError{resp.StatusCode, resp.Header.Get("Retry-After")}
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 128*1024+1))
	if err != nil {
		return nil, err
	}
	if len(raw) > 128*1024 {
		return nil, ErrInvalidResponse
	}
	var result struct {
		Success       bool     `json:"success"`
		Accepted      bool     `json:"accepted"`
		Channels      []string `json:"channels"`
		PublicationID string   `json:"publicationId"`
		Duplicate     *bool    `json:"duplicate"`
	}
	if json.Unmarshal(raw, &result) != nil || !result.Success || !result.Accepted || len(result.Channels) == 0 || result.PublicationID == "" || result.Duplicate == nil {
		return nil, ErrInvalidResponse
	}
	for _, channel := range result.Channels {
		if channel == "" {
			return nil, ErrInvalidResponse
		}
	}
	return &TriggerResponse{true, true, result.Channels, result.PublicationID, *result.Duplicate}, nil
}

type Authorization struct {
	Auth        string `json:"auth"`
	ChannelData string `json:"channel_data,omitempty"`
}

// AuthorizeChannel signs a private channel after your app has checked permissions.
func (c *Client) AuthorizeChannel(socketID, channel string) (*Authorization, error) {
	if !validString(socketID, 100) || !validString(channel, 200) || !strings.HasPrefix(channel, "private-") {
		return nil, errors.New("pubb: a socket ID and private- channel are required")
	}
	return &Authorization{Auth: c.sign(socketID + ":" + channel)}, nil
}

// AuthorizePresenceChannel signs the exact ChannelData bytes returned to the client.
// Derive userID and permissions from your backend session, not client input.
func (c *Client) AuthorizePresenceChannel(socketID, channel, userID string, userInfo any) (*Authorization, error) {
	if !validString(socketID, 100) || !validString(channel, 200) || !strings.HasPrefix(channel, "presence-") || strings.TrimSpace(userID) == "" {
		return nil, errors.New("pubb: a socket ID, presence- channel and user ID are required")
	}
	data, err := json.Marshal(struct {
		UserID   string `json:"user_id"`
		UserInfo any    `json:"user_info,omitempty"`
	}{userID, userInfo})
	if err != nil {
		return nil, err
	}
	return &Authorization{Auth: c.sign(socketID + ":" + channel + ":" + string(data)), ChannelData: string(data)}, nil
}

func (c *Client) sign(value string) string {
	mac := hmac.New(sha256.New, []byte(c.secret))
	mac.Write([]byte(value))
	return c.key + ":" + hex.EncodeToString(mac.Sum(nil))
}
