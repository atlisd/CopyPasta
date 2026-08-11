package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestStore(t *testing.T) (*Store, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })
	return New(rdb), mr
}

func TestCreatePublishGet(t *testing.T) {
	s, _ := newTestStore(t)
	ctx := context.Background()

	pin, token, err := s.Create(ctx, time.Minute)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if len(pin) != 6 {
		t.Fatalf("pin = %q, want 6 digits", pin)
	}
	if token == "" {
		t.Fatal("empty write token")
	}

	// A fresh channel exists but has nothing on it yet.
	if _, ttl, err := s.Get(ctx, pin); !errors.Is(err, ErrEmpty) {
		t.Fatalf("Get on empty channel = %v, want ErrEmpty", err)
	} else if ttl <= 0 {
		t.Fatalf("ttl = %v, want > 0", ttl)
	}

	if err := s.Publish(ctx, pin, token, Paste{Kind: "text", Data: "hello"}, time.Minute); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	got, ttl, err := s.Get(ctx, pin)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Data != "hello" || got.Kind != "text" {
		t.Fatalf("got %+v, want text/hello", got)
	}
	if ttl <= 0 || ttl > time.Minute {
		t.Fatalf("ttl = %v, want (0, 1m]", ttl)
	}
}

func TestPublishTwiceReplacesValue(t *testing.T) {
	s, _ := newTestStore(t)
	ctx := context.Background()

	pin, token, err := s.Create(ctx, time.Minute)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	for _, want := range []string{"first", "second", "third"} {
		if err := s.Publish(ctx, pin, token, Paste{Kind: "text", Data: want}, time.Minute); err != nil {
			t.Fatalf("Publish %q: %v", want, err)
		}
		got, _, err := s.Get(ctx, pin)
		if err != nil {
			t.Fatalf("Get after %q: %v", want, err)
		}
		if got.Data != want {
			t.Fatalf("got %q, want %q", got.Data, want)
		}
	}
}

func TestPublishRejectsWrongToken(t *testing.T) {
	s, _ := newTestStore(t)
	ctx := context.Background()

	pin, _, err := s.Create(ctx, time.Minute)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	err = s.Publish(ctx, pin, "not-the-token", Paste{Kind: "text", Data: "evil"}, time.Minute)
	if !errors.Is(err, ErrBadToken) {
		t.Fatalf("Publish with wrong token = %v, want ErrBadToken", err)
	}
	if _, _, err := s.Get(ctx, pin); !errors.Is(err, ErrEmpty) {
		t.Fatalf("channel was written to anyway: %v", err)
	}
}

func TestPublishToMissingChannel(t *testing.T) {
	s, _ := newTestStore(t)

	err := s.Publish(context.Background(), "000000", "token", Paste{Kind: "text", Data: "x"}, time.Minute)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("Publish = %v, want ErrNotFound", err)
	}
}

func TestPublishSlidesExpiry(t *testing.T) {
	s, mr := newTestStore(t)
	ctx := context.Background()

	pin, token, err := s.Create(ctx, time.Minute)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	mr.FastForward(45 * time.Second)
	if err := s.Publish(ctx, pin, token, Paste{Kind: "text", Data: "still here"}, time.Minute); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// Past the original deadline, but the update bought another full minute.
	mr.FastForward(30 * time.Second)
	if _, ttl, err := s.Get(ctx, pin); err != nil {
		t.Fatalf("Get: %v", err)
	} else if ttl <= 0 {
		t.Fatalf("ttl = %v, want > 0 after a sliding update", ttl)
	}
}

func TestSubscribeReceivesUpdates(t *testing.T) {
	s, _ := newTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pin, token, err := s.Create(ctx, time.Minute)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	updates, unsubscribe, err := s.Subscribe(ctx, pin)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer unsubscribe()

	for _, want := range []string{"one", "two"} {
		if err := s.Publish(ctx, pin, token, Paste{Kind: "text", Data: want}, time.Minute); err != nil {
			t.Fatalf("Publish %q: %v", want, err)
		}
		select {
		case got := <-updates:
			if got.Data != want {
				t.Fatalf("received %q, want %q", got.Data, want)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("no update delivered for %q", want)
		}
	}
}

func TestSubscribeIgnoresOtherChannels(t *testing.T) {
	s, _ := newTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mine, _, err := s.Create(ctx, time.Minute)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	other, otherToken, err := s.Create(ctx, time.Minute)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	updates, unsubscribe, err := s.Subscribe(ctx, mine)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer unsubscribe()

	if err := s.Publish(ctx, other, otherToken, Paste{Kind: "text", Data: "not yours"}, time.Minute); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case got := <-updates:
		t.Fatalf("leaked another channel's value: %+v", got)
	case <-time.After(300 * time.Millisecond):
	}
}

func TestExpiryLeavesNothing(t *testing.T) {
	s, mr := newTestStore(t)
	ctx := context.Background()

	pin, token, err := s.Create(ctx, time.Minute)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := s.Publish(ctx, pin, token, Paste{Kind: "text", Data: "secret"}, time.Minute); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	mr.FastForward(2 * time.Minute)

	if _, _, err := s.Get(ctx, pin); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get after expiry = %v, want ErrNotFound", err)
	}
	// The only key left should be the (also expiring) failure counter.
	if keys := mr.Keys(); len(keys) != 1 || keys[0] != failKey(pin) {
		t.Fatalf("leftover keys = %v", keys)
	}
}

func TestCloseDeletesEverything(t *testing.T) {
	s, mr := newTestStore(t)
	ctx := context.Background()

	pin, token, err := s.Create(ctx, time.Hour)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := s.Publish(ctx, pin, token, Paste{Kind: "text", Data: "secret"}, time.Hour); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	if err := s.Close(ctx, pin, token); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if keys := mr.Keys(); len(keys) != 0 {
		t.Fatalf("leftover keys = %v", keys)
	}
}

func TestWrongPINLocksOut(t *testing.T) {
	s, _ := newTestStore(t)
	ctx := context.Background()

	const pin = "000000" // no channel under it
	for i := 0; i < maxAttempts; i++ {
		if _, _, err := s.Get(ctx, pin); !errors.Is(err, ErrNotFound) {
			t.Fatalf("attempt %d = %v, want ErrNotFound", i, err)
		}
	}

	if _, _, err := s.Get(ctx, pin); !errors.Is(err, ErrLockedOut) {
		t.Fatalf("after %d misses = %v, want ErrLockedOut", maxAttempts, err)
	}
}

func TestLockoutBurnsAChannelLandingOnThatPIN(t *testing.T) {
	s, mr := newTestStore(t)
	ctx := context.Background()

	const pin = "123456"
	// One guess short of the limit, then the final miss must wipe whatever the
	// PIN holds — so a guesser who gets there as a channel appears loses it.
	mr.Set(failKey(pin), "4")

	if _, _, err := s.Get(ctx, pin); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get = %v, want ErrNotFound", err)
	}
	if mr.Exists(tokenKey(pin)) || mr.Exists(pasteKey(pin)) {
		t.Fatal("channel keys survived the lockout")
	}
	if _, _, err := s.Get(ctx, pin); !errors.Is(err, ErrLockedOut) {
		t.Fatalf("next Get = %v, want ErrLockedOut", err)
	}
}

func TestPINCollisionRerolls(t *testing.T) {
	s, _ := newTestStore(t)
	ctx := context.Background()

	seen := make(map[string]bool)
	for i := 0; i < 50; i++ {
		pin, _, err := s.Create(ctx, time.Minute)
		if err != nil {
			t.Fatalf("Create %d: %v", i, err)
		}
		if seen[pin] {
			t.Fatalf("pin %q handed out twice", pin)
		}
		seen[pin] = true
	}
}

func TestRandomPINFormat(t *testing.T) {
	for i := 0; i < 200; i++ {
		pin, err := randomPIN()
		if err != nil {
			t.Fatalf("randomPIN: %v", err)
		}
		if len(pin) != pinDigits {
			t.Fatalf("pin = %q, want %d digits", pin, pinDigits)
		}
		for _, c := range pin {
			if c < '0' || c > '9' {
				t.Fatalf("pin = %q contains a non-digit", pin)
			}
		}
	}
}

func TestRandomTokensDiffer(t *testing.T) {
	seen := make(map[string]bool)
	for i := 0; i < 100; i++ {
		tok, err := randomToken()
		if err != nil {
			t.Fatalf("randomToken: %v", err)
		}
		if seen[tok] {
			t.Fatalf("token %q repeated", tok)
		}
		seen[tok] = true
	}
}
