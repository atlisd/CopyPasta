// copypasta is a tiny ephemeral clipboard: paste text or an image on one
// machine, read it back on another with a 6-digit PIN, and have it vanish.
package main

import (
	"context"
	"embed"
	"errors"
	"flag"
	"io/fs"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"copypasta/internal/api"
	"copypasta/internal/store"
)

//go:embed web
var webFS embed.FS

func main() {
	// The image is distroless, so there is no curl for a container healthcheck —
	// the binary probes itself instead.
	healthcheck := flag.Bool("healthcheck", false, "probe the local server and exit")
	flag.Parse()
	if *healthcheck {
		os.Exit(probe(env("PORT", "8080")))
	}

	web, err := fs.Sub(webFS, "web")
	if err != nil {
		log.Fatalf("embed web assets: %v", err)
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:     env("REDIS_ADDR", "redis:6379"),
		Password: os.Getenv("REDIS_PASSWORD"),
	})
	defer rdb.Close()

	cfg := api.Config{
		MaxPasteBytes: envInt64("MAX_PASTE_BYTES", 5<<20),
		DefaultTTL:    time.Duration(envInt64("DEFAULT_TTL_SECONDS", 600)) * time.Second,
	}

	srv := &http.Server{
		Addr:              ":" + env("PORT", "8080"),
		Handler:           api.NewServer(store.New(rdb), cfg, web).Routes(),
		ReadHeaderTimeout: 10 * time.Second,
		WriteTimeout:      30 * time.Second,
	}

	go func() {
		log.Printf("copypasta listening on %s", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("serve: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("shutdown: %v", err)
	}
	log.Print("bye")
}

// probe hits /healthz and returns a process exit code.
func probe(port string) int {
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get("http://127.0.0.1:" + port + "/healthz")
	if err != nil {
		log.Printf("healthcheck: %v", err)
		return 1
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		log.Printf("healthcheck: status %d", resp.StatusCode)
		return 1
	}
	return 0
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt64(key string, fallback int64) int64 {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		log.Printf("%s=%q is not a number, using %d", key, v, fallback)
		return fallback
	}
	return n
}
