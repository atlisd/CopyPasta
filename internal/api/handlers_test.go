package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/redis/go-redis/v9"

	"copypasta/internal/store"
)

func newTestServer(t *testing.T) http.Handler {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })

	web := fstest.MapFS{"index.html": &fstest.MapFile{Data: []byte("<html>ui</html>")}}
	cfg := Config{MaxPasteBytes: 1 << 20, DefaultTTL: 10 * time.Minute}
	return NewServer(store.New(rdb), cfg, web).Routes()
}

func post(t *testing.T, h http.Handler, path, body string) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	var out map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode %s response %q: %v", path, rec.Body.String(), err)
	}
	return rec, out
}

// openChannel creates a channel and returns its PIN and write token.
func openChannel(t *testing.T, h http.Handler, ttl int) (string, string) {
	t.Helper()
	rec, body := post(t, h, "/api/channel", `{"ttl":`+strconv.Itoa(ttl)+`}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("create channel status = %d, want 200", rec.Code)
	}
	pin, _ := body["pin"].(string)
	token, _ := body["token"].(string)
	if len(pin) != 6 || token == "" {
		t.Fatalf("bad channel: pin=%q token=%q", pin, token)
	}
	return pin, token
}

func TestCreatePublishFetch(t *testing.T) {
	h := newTestServer(t)
	pin, token := openChannel(t, h, 60)

	// Nothing sent yet: the channel is valid but empty.
	rec, body := post(t, h, "/api/fetch", `{"pin":"`+pin+`"}`)
	if rec.Code != http.StatusOK || body["empty"] != true {
		t.Fatalf("fetch on empty channel: status=%d body=%+v", rec.Code, body)
	}

	rec, _ = post(t, h, "/api/publish",
		`{"pin":"`+pin+`","token":"`+token+`","kind":"text","data":"hello there","ttl":60}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("publish status = %d, want 200", rec.Code)
	}

	rec, body = post(t, h, "/api/fetch", `{"pin":"`+pin+`"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("fetch status = %d, want 200", rec.Code)
	}
	if body["data"] != "hello there" {
		t.Fatalf("data = %v, want %q", body["data"], "hello there")
	}
}

func TestPublishUpdatesSamePIN(t *testing.T) {
	h := newTestServer(t)
	pin, token := openChannel(t, h, 60)

	for _, want := range []string{"first", "second"} {
		rec, _ := post(t, h, "/api/publish",
			`{"pin":"`+pin+`","token":"`+token+`","kind":"text","data":"`+want+`","ttl":60}`)
		if rec.Code != http.StatusOK {
			t.Fatalf("publish %q: status = %d", want, rec.Code)
		}
		_, body := post(t, h, "/api/fetch", `{"pin":"`+pin+`"}`)
		if body["data"] != want {
			t.Fatalf("data = %v, want %q", body["data"], want)
		}
	}
}

func TestPublishRejectsWrongToken(t *testing.T) {
	h := newTestServer(t)
	pin, _ := openChannel(t, h, 60)

	rec, _ := post(t, h, "/api/publish",
		`{"pin":"`+pin+`","token":"wrong","kind":"text","data":"evil","ttl":60}`)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", rec.Code)
	}
}

func TestPublishToUnknownPIN(t *testing.T) {
	h := newTestServer(t)

	rec, _ := post(t, h, "/api/publish",
		`{"pin":"000000","token":"whatever","kind":"text","data":"x","ttl":60}`)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}

func TestFetchUnknownPIN(t *testing.T) {
	h := newTestServer(t)

	rec, _ := post(t, h, "/api/fetch", `{"pin":"000000"}`)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}

func TestFetchRejectsMalformedPIN(t *testing.T) {
	h := newTestServer(t)

	for _, pin := range []string{"12345", "1234567", "12345a", ""} {
		rec, _ := post(t, h, "/api/fetch", `{"pin":"`+pin+`"}`)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("pin %q: status = %d, want 400", pin, rec.Code)
		}
	}
}

func TestRejectsUnknownTTL(t *testing.T) {
	h := newTestServer(t)

	rec, _ := post(t, h, "/api/channel", `{"ttl":86400}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestPublishRejectsEmptyAndBadKind(t *testing.T) {
	h := newTestServer(t)
	pin, token := openChannel(t, h, 60)
	head := `{"pin":"` + pin + `","token":"` + token + `",`

	cases := map[string]string{
		"empty data": head + `"kind":"text","data":"","ttl":60}`,
		"bad kind":   head + `"kind":"video","data":"x","ttl":60}`,
		"bad base64": head + `"kind":"image","mime":"image/png","data":"not base64!","ttl":60}`,
		"bad mime":   head + `"kind":"image","mime":"text/plain","data":"AAAA","ttl":60}`,
	}
	for name, body := range cases {
		if rec, _ := post(t, h, "/api/publish", body); rec.Code != http.StatusBadRequest {
			t.Fatalf("%s: status = %d, want 400", name, rec.Code)
		}
	}
}

func TestPublishRejectsOversizedBody(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })
	h := NewServer(store.New(rdb), Config{MaxPasteBytes: 64, DefaultTTL: time.Minute},
		fstest.MapFS{}).Routes()

	body := `{"pin":"123456","token":"t","kind":"text","data":"` + strings.Repeat("x", 500) + `"}`
	req := httptest.NewRequest(http.MethodPost, "/api/publish", strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", rec.Code)
	}
}

func TestImageRoundTrip(t *testing.T) {
	h := newTestServer(t)
	pin, token := openChannel(t, h, 60)

	// 1x1 transparent PNG.
	const png = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII="
	rec, _ := post(t, h, "/api/publish",
		`{"pin":"`+pin+`","token":"`+token+`","kind":"image","mime":"image/png","data":"`+png+`","ttl":60}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("publish status = %d, want 200", rec.Code)
	}

	rec, body := post(t, h, "/api/fetch", `{"pin":"`+pin+`"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("fetch status = %d, want 200", rec.Code)
	}
	if body["kind"] != "image" || body["mime"] != "image/png" || body["data"] != png {
		t.Fatalf("round trip lost data: %+v", body)
	}
}

func TestFetchRateLimited(t *testing.T) {
	h := newTestServer(t)

	var last int
	for i := 0; i < 25; i++ {
		req := httptest.NewRequest(http.MethodPost, "/api/fetch", strings.NewReader(`{"pin":"000000"}`))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		last = rec.Code
	}
	if last != http.StatusTooManyRequests {
		t.Fatalf("last status = %d, want 429", last)
	}
}

func TestResponsesAreNotCached(t *testing.T) {
	h := newTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if got := rec.Header().Get("Cache-Control"); !strings.Contains(got, "no-store") {
		t.Fatalf("Cache-Control = %q, want no-store", got)
	}
	if got := rec.Header().Get("X-Robots-Tag"); !strings.Contains(got, "noindex") {
		t.Fatalf("X-Robots-Tag = %q, want noindex", got)
	}
}

func TestHealthz(t *testing.T) {
	h := newTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
}

// ---- WebSocket ----------------------------------------------------------

// TestSubscribePushesUpdates is the heart of the live flow: a receiver holding
// the socket sees each value the moment the sender publishes it.
func TestSubscribePushesUpdates(t *testing.T) {
	h := newTestServer(t)
	srv := httptest.NewServer(h)
	defer srv.Close()

	pin, token := openChannel(t, h, 60)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, _, err := websocket.Dial(ctx, wsURL(srv.URL, pin), nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.CloseNow()

	for _, want := range []string{"first value", "second value"} {
		rec, _ := post(t, h, "/api/publish",
			`{"pin":"`+pin+`","token":"`+token+`","kind":"text","data":"`+want+`","ttl":60}`)
		if rec.Code != http.StatusOK {
			t.Fatalf("publish %q: status = %d", want, rec.Code)
		}

		readCtx, cancelRead := context.WithTimeout(ctx, 5*time.Second)
		var msg map[string]any
		err := wsjson.Read(readCtx, conn, &msg)
		cancelRead()
		if err != nil {
			t.Fatalf("read %q: %v", want, err)
		}
		if msg["data"] != want {
			t.Fatalf("pushed %v, want %q", msg["data"], want)
		}
		if msg["expires_in"].(float64) <= 0 {
			t.Fatalf("expires_in = %v, want > 0", msg["expires_in"])
		}
	}
}

// TestSubscribeSendsSnapshotOnJoin covers the receiver arriving after the
// sender has already pushed something: it must not have to wait for the next
// Send to see the current value.
func TestSubscribeSendsSnapshotOnJoin(t *testing.T) {
	h := newTestServer(t)
	srv := httptest.NewServer(h)
	defer srv.Close()

	pin, token := openChannel(t, h, 60)
	rec, _ := post(t, h, "/api/publish",
		`{"pin":"`+pin+`","token":"`+token+`","kind":"text","data":"already here","ttl":60}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("publish status = %d", rec.Code)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, _, err := websocket.Dial(ctx, wsURL(srv.URL, pin), nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.CloseNow()

	var msg map[string]any
	if err := wsjson.Read(ctx, conn, &msg); err != nil {
		t.Fatalf("read snapshot: %v", err)
	}
	if msg["data"] != "already here" {
		t.Fatalf("snapshot = %v, want %q", msg["data"], "already here")
	}
}

// TestIdleSubscriberStillGetsUpdates guards the failure that would look like
// "it worked, then quietly stopped updating": a socket that sat idle must still
// deliver. The server here runs with a WriteTimeout shorter than the idle gap,
// which net/http clears on hijack — this pins that behaviour down so a future
// change to the accept path can't silently start timing sockets out.
func TestIdleSubscriberStillGetsUpdates(t *testing.T) {
	h := newTestServer(t)
	srv := httptest.NewUnstartedServer(h)
	srv.Config.WriteTimeout = 500 * time.Millisecond
	srv.Start()
	defer srv.Close()

	pin, token := openChannel(t, h, 60)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	conn, _, err := websocket.Dial(ctx, wsURL(srv.URL, pin), nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.CloseNow()

	// Sit idle well past the server's write timeout, then send.
	time.Sleep(1500 * time.Millisecond)

	rec, _ := post(t, h, "/api/publish",
		`{"pin":"`+pin+`","token":"`+token+`","kind":"text","data":"late but delivered","ttl":60}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("publish status = %d", rec.Code)
	}

	readCtx, cancelRead := context.WithTimeout(ctx, 5*time.Second)
	defer cancelRead()
	var msg map[string]any
	if err := wsjson.Read(readCtx, conn, &msg); err != nil {
		t.Fatalf("socket died after the server write timeout: %v", err)
	}
	if msg["data"] != "late but delivered" {
		t.Fatalf("got %v, want %q", msg["data"], "late but delivered")
	}
}

func TestSubscribeRejectsUnknownPIN(t *testing.T) {
	srv := httptest.NewServer(newTestServer(t))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, resp, err := websocket.Dial(ctx, wsURL(srv.URL, "000000"), nil)
	if err == nil {
		conn.CloseNow()
		t.Fatal("dial succeeded for an unknown pin")
	}
	if resp == nil || resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %v, want 404", resp)
	}
}

func TestSubscribeRejectsMalformedPIN(t *testing.T) {
	srv := httptest.NewServer(newTestServer(t))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, resp, err := websocket.Dial(ctx, wsURL(srv.URL, "abc"), nil)
	if err == nil {
		conn.CloseNow()
		t.Fatal("dial succeeded for a malformed pin")
	}
	if resp == nil || resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %v, want 400", resp)
	}
}

func wsURL(base, pin string) string {
	return "ws" + strings.TrimPrefix(base, "http") + "/api/subscribe?pin=" + pin
}
