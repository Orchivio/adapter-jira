package adapter

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const requestTimeout = 30 * time.Second

// newHTTPClient honours the proxy variables (via the default transport) and
// adds SSL_CERT_FILE to the system roots; the standard library ignores that
// variable on macOS and only reads it once on Linux.
// ponytail: SSL_CERT_DIR is left to the standard library (Linux only).
func newHTTPClient() (*http.Client, error) {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if file := os.Getenv("SSL_CERT_FILE"); file != "" {
		pem, err := os.ReadFile(file)
		if err != nil {
			return nil, fmt.Errorf("read SSL_CERT_FILE: %w", err)
		}
		pool, err := x509.SystemCertPool()
		if err != nil {
			pool = x509.NewCertPool()
		}
		if !pool.AppendCertsFromPEM(pem) {
			return nil, errors.New("SSL_CERT_FILE contains no PEM certificates")
		}
		transport.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	}
	return &http.Client{
		Timeout:   requestTimeout,
		Transport: transport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}, nil
}

// parseBase accepts a Jira base URL (with optional context path) without
// credentials, query or fragment.
func parseBase(base string) (*url.URL, error) {
	u, err := url.Parse(base)
	if err != nil || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" ||
		(u.Scheme != "http" && u.Scheme != "https") {
		return nil, errors.New("expected an HTTP(S) Jira URL without credentials, query or fragment")
	}
	return u, nil
}

type apiError int

func (e apiError) Error() string { return fmt.Sprintf("Jira returned HTTP %d", e) }

func request(ctx context.Context, client *http.Client, base *url.URL, path string, method string, query url.Values, auth func(*http.Request), in, out any) error {
	var body io.Reader
	if in != nil {
		data, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(data)
	}
	endpoint := strings.TrimRight(base.String(), "/") + path
	if len(query) != 0 {
		endpoint += "?" + query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, body)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	auth(req)
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return apiError(resp.StatusCode)
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	_, err = io.Copy(io.Discard, resp.Body)
	return err
}

// grpcError maps upstream failures; 401 becomes UNAUTHENTICATED so the
// orchestrator marks the connection needs_reauth.
func grpcError(err error) error {
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return status.FromContextError(err).Err()
	case errors.Is(err, apiError(http.StatusUnauthorized)):
		return status.Error(codes.Unauthenticated, err.Error())
	case errors.Is(err, apiError(http.StatusForbidden)):
		return status.Error(codes.PermissionDenied, err.Error())
	case errors.Is(err, apiError(http.StatusNotFound)):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, apiError(http.StatusTooManyRequests)):
		return status.Error(codes.ResourceExhausted, err.Error())
	default:
		return status.Error(codes.Unavailable, err.Error())
	}
}
