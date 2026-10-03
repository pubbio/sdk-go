# Pubb Go server SDK

Publish events and authorize private/presence subscriptions from Go 1.22+.
Standard library only; no runtime dependencies. Keep credentials on your backend.

## Install

```sh
go get github.com/pubbio/sdk-go@main
```

Go records the resolved commit as a pseudo-version in your application's go.mod.

## Publish

```go
import (
    "context"
    "os"
    pubb "github.com/pubbio/sdk-go"
)

client, err := pubb.NewClient(pubb.Config{
    AppID: os.Getenv("PUBB_APP_ID"), AppKey: os.Getenv("PUBB_APP_KEY"),
    AppSecret: os.Getenv("PUBB_APP_SECRET"), APIHost: "https://api.pubb.io",
})
if err != nil { /* handle configuration error */ }
result, err := client.Trigger(context.Background(), []string{"updates"}, "message",
    map[string]string{"text": "Hello"}, &pubb.TriggerOptions{IdempotencyKey: "message-42"})
if err != nil { /* handle request error */ }
_ = result.PublicationID
```

The runnable example is in `examples/publish`. Pass `nil` for default options;
`SocketID` excludes a connection. HTTP 202 means accepted for processing, not
delivered. No automatic retries: reuse an idempotency key for the same event.
Use `errors.As` for `*pubb.APIError` (`StatusCode`, `RetryAfter`) and `errors.Is`
for `pubb.ErrInvalidResponse`. Network errors are returned directly.

The default timeout is 30 seconds; requests honor context cancellation. A custom
`HTTPClient` is copied without changing its transport, timeout or ownership.
Redirects are disabled to prevent forwarding credentials. Clients are safe for
concurrent use. Do not mutate a supplied transport while it is in use.

## Authorize subscriptions

After authenticating your user and checking channel permissions, return the
result as JSON from your backend route:

```go
auth, err := client.AuthorizeChannel("123.456", "private-room")
presence, err := client.AuthorizePresenceChannel("123.456", "presence-room",
    "user-1", map[string]string{"name": "Ada"})
```

Use the session's user ID, not untrusted client input. The helpers sign data but
do not check access permissions. Forward `ChannelData` unchanged; the exact JSON
bytes are signed. This SDK implements Pubb's HTTP API and shared HMAC format;
it is not a drop-in replacement for Pusher's HTTP SDK.

## Development

```sh
gofmt -w .
go vet ./...
go test -race ./...
go build ./...
```

Tests cover real local HTTP requests, cancellation, redirects, response validation,
and cross-language signing vectors. MIT.
