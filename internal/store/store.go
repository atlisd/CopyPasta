// Package store holds live paste channels in Redis, keyed by a random 6-digit PIN.
//
// A channel is created by the sending machine, which gets back the PIN plus a
// write token. The token is what lets it push new values; the PIN alone only
// grants read access, so a reader can never overwrite what the sender put there.
//
// Every key carries a TTL and Redis is run without persistence, so an expired
// channel leaves nothing behind: no rows, no tombstones, no file on disk.
package store

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	// ErrNotFound is returned when a PIN has no channel (wrong, or expired).
	ErrNotFound = errors.New("channel not found")
	// ErrEmpty is returned when a channel exists but nothing has been sent yet.
	ErrEmpty = errors.New("channel is empty")
	// ErrLockedOut is returned once a PIN has seen too many wrong attempts.
	ErrLockedOut = errors.New("too many attempts")
	// ErrBadToken is returned when a publish presents the wrong write token.
	ErrBadToken = errors.New("invalid write token")
	// ErrNoPIN is returned when no free PIN could be found.
	ErrNoPIN = errors.New("could not allocate a pin")
)

const (
	pinDigits = 6
	// maxAttempts wrong PINs burns the channel. The PIN space is only a million
	// wide, so guessing has to stay expensive.
	maxAttempts = 5
	// pinTries is how often we reroll on a PIN collision before giving up.
	pinTries = 10
)

// Paste is one value pushed onto a channel.
type Paste struct {
	Kind    string    `json:"kind"` // "text" or "image"
	MIME    string    `json:"mime"` // set for images, e.g. "image/png"
	Data    string    `json:"data"` // text body, or base64 image bytes
	Created time.Time `json:"created"`
}

// Store persists channels in Redis and fans updates out over Redis pub/sub.
type Store struct {
	rdb *redis.Client
}

func New(rdb *redis.Client) *Store { return &Store{rdb: rdb} }

func tokenKey(pin string) string { return "token:" + pin }
func pasteKey(pin string) string { return "paste:" + pin }
func failKey(pin string) string  { return "fail:" + pin }
func topic(pin string) string    { return "chan:" + pin }

// Create reserves a PIN for ttl and returns it with the write token the sender
// needs in order to publish.
func (s *Store) Create(ctx context.Context, ttl time.Duration) (pin, token string, err error) {
	token, err = randomToken()
	if err != nil {
		return "", "", err
	}

	// SetNX so two concurrent senders can never land on the same PIN.
	for i := 0; i < pinTries; i++ {
		pin, err = randomPIN()
		if err != nil {
			return "", "", err
		}
		ok, err := s.rdb.SetNX(ctx, tokenKey(pin), token, ttl).Result()
		if err != nil {
			return "", "", err
		}
		if ok {
			return pin, token, nil
		}
	}
	return "", "", ErrNoPIN
}

// Publish replaces the channel's value and pushes it to every live subscriber.
// It also slides the channel's expiry out by ttl, so a channel in active use
// stays alive and an abandoned one still dies on schedule.
func (s *Store) Publish(ctx context.Context, pin, token string, p Paste, ttl time.Duration) error {
	if err := s.checkToken(ctx, pin, token); err != nil {
		return err
	}

	p.Created = time.Now().UTC()
	body, err := json.Marshal(p)
	if err != nil {
		return err
	}

	pipe := s.rdb.TxPipeline()
	pipe.Set(ctx, pasteKey(pin), body, ttl)
	pipe.Expire(ctx, tokenKey(pin), ttl)
	pipe.Publish(ctx, topic(pin), body)
	_, err = pipe.Exec(ctx)
	return err
}

// checkToken verifies the caller may write to pin.
func (s *Store) checkToken(ctx context.Context, pin, token string) error {
	want, err := s.rdb.Get(ctx, tokenKey(pin)).Result()
	if errors.Is(err, redis.Nil) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if subtle.ConstantTimeCompare([]byte(want), []byte(token)) != 1 {
		return ErrBadToken
	}
	return nil
}

// Get returns the channel's current value, counting wrong guesses and burning
// the channel once too many pile up. ErrEmpty means the PIN is valid but the
// sender has not pushed anything yet — the caller should subscribe and wait.
func (s *Store) Get(ctx context.Context, pin string) (Paste, time.Duration, error) {
	fails, err := s.rdb.Get(ctx, failKey(pin)).Int()
	if err != nil && !errors.Is(err, redis.Nil) {
		return Paste{}, 0, err
	}
	if fails >= maxAttempts {
		return Paste{}, 0, ErrLockedOut
	}

	// The token key is what defines the channel's existence and lifetime.
	ttl, err := s.rdb.TTL(ctx, tokenKey(pin)).Result()
	if err != nil {
		return Paste{}, 0, err
	}
	if ttl < 0 {
		if err := s.recordFailure(ctx, pin); err != nil {
			return Paste{}, 0, err
		}
		return Paste{}, 0, ErrNotFound
	}

	body, err := s.rdb.Get(ctx, pasteKey(pin)).Bytes()
	if errors.Is(err, redis.Nil) {
		return Paste{}, ttl, ErrEmpty
	}
	if err != nil {
		return Paste{}, 0, err
	}

	var p Paste
	if err := json.Unmarshal(body, &p); err != nil {
		return Paste{}, 0, err
	}
	return p, ttl, nil
}

// Subscribe streams every value published to pin until ctx is cancelled. The
// returned cleanup func must be called by the caller.
func (s *Store) Subscribe(ctx context.Context, pin string) (<-chan Paste, func(), error) {
	sub := s.rdb.Subscribe(ctx, topic(pin))
	if _, err := sub.Receive(ctx); err != nil { // wait for the subscription to land
		sub.Close()
		return nil, nil, err
	}

	out := make(chan Paste)
	go func() {
		defer close(out)
		for msg := range sub.Channel() {
			var p Paste
			if err := json.Unmarshal([]byte(msg.Payload), &p); err != nil {
				continue
			}
			select {
			case out <- p:
			case <-ctx.Done():
				return
			}
		}
	}()

	return out, func() { sub.Close() }, nil
}

// TTL reports how long pin has left, or 0 if it is gone.
func (s *Store) TTL(ctx context.Context, pin string) (time.Duration, error) {
	ttl, err := s.rdb.TTL(ctx, tokenKey(pin)).Result()
	if err != nil {
		return 0, err
	}
	if ttl < 0 {
		return 0, nil
	}
	return ttl, nil
}

// Close deletes a channel and everything attached to it, right now.
func (s *Store) Close(ctx context.Context, pin, token string) error {
	if err := s.checkToken(ctx, pin, token); err != nil {
		return err
	}
	return s.rdb.Del(ctx, tokenKey(pin), pasteKey(pin)).Err()
}

// recordFailure counts a wrong guess against pin and, at the limit, deletes
// whatever that PIN might later hold so a brute-force run gains nothing.
func (s *Store) recordFailure(ctx context.Context, pin string) error {
	n, err := s.rdb.Incr(ctx, failKey(pin)).Result()
	if err != nil {
		return err
	}
	if n == 1 {
		if err := s.rdb.Expire(ctx, failKey(pin), 15*time.Minute).Err(); err != nil {
			return err
		}
	}
	if n >= maxAttempts {
		return s.rdb.Del(ctx, tokenKey(pin), pasteKey(pin)).Err()
	}
	return nil
}

// Ping checks that Redis is reachable.
func (s *Store) Ping(ctx context.Context) error { return s.rdb.Ping(ctx).Err() }

func randomPIN() (string, error) {
	max := big.NewInt(1)
	for i := 0; i < pinDigits; i++ {
		max.Mul(max, big.NewInt(10))
	}
	n, err := rand.Int(rand.Reader, max)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%0*d", pinDigits, n), nil
}

func randomToken() (string, error) {
	buf := make([]byte, 24)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}
