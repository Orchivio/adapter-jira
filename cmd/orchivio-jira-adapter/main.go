// Command orchivio-jira-adapter serves the plugin.v1 TrackerAdapter for one
// Jira Data Center instance or for all Jira Cloud sites.
package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	pb "github.com/orchivio/orchestrator/contract/gen/orchivio/plugin/v1"
	"google.golang.org/grpc"

	"github.com/orchivio/adapter-jira/internal/adapter"
)

const shutdownTimeout = 10 * time.Second

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	if err := run(); err != nil {
		slog.Error("jira adapter failed", "err", err)
		os.Exit(1)
	}
}

func env(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

func run() error {
	interval, err := time.ParseDuration(env("JIRA_POLL_INTERVAL", "30s"))
	if err != nil {
		return err
	}
	srv, err := adapter.New(adapter.Config{Deployment: os.Getenv("JIRA_DEPLOYMENT"), URL: os.Getenv("JIRA_URL"), PollInterval: interval})
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	lis, err := net.Listen("tcp", env("ORCHIVIO_GRPC_ADDR", "127.0.0.1:7301"))
	if err != nil {
		return err
	}
	// ponytail: insecure gRPC, bind localhost; mTLS before remote workers.
	g := grpc.NewServer()
	pb.RegisterTrackerAdapterServer(g, srv)
	errs := make(chan error, 1)
	go func() { errs <- g.Serve(lis) }()
	slog.Info("jira adapter started", "deployment", os.Getenv("JIRA_DEPLOYMENT"), "grpc", lis.Addr().String())

	select {
	case err = <-errs:
	case <-ctx.Done():
	}
	srv.Close()
	done := make(chan struct{})
	go func() { g.GracefulStop(); close(done) }()
	select {
	case <-done:
	case <-time.After(shutdownTimeout):
		g.Stop()
	}
	return err
}
