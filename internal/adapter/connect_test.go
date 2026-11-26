package adapter

import (
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	pb "github.com/orchivio/orchestrator/contract/gen/orchivio/plugin/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCapabilitiesListFormFields(t *testing.T) {
	cloud, _ := New(Config{Deployment: "cloud", PollInterval: 1})
	dc, _ := New(Config{Deployment: "datacenter", URL: "https://jira.example.test", PollInterval: 1})
	names := func(s *Server) []string {
		caps, _ := s.GetCapabilities(t.Context(), &pb.GetCapabilitiesRequest{})
		if caps.GetAdapterName() != "jira" || !caps.GetSupportsStatusUpdate() {
			t.Fatalf("caps = %v", caps)
		}
		var out []string
		for _, f := range caps.GetConnectFields() {
			out = append(out, fmt.Sprintf("%s:%v:%v", f.GetName(), f.GetRequired(), f.GetSecret()))
		}
		return out
	}
	if got := names(cloud); !reflect.DeepEqual(got, []string{"siteUrl:true:false", "email:true:false", "token:true:true", "jql:false:false"}) {
		t.Fatalf("cloud fields = %v", got)
	}
	if got := names(dc); !reflect.DeepEqual(got, []string{"token:true:true", "jql:false:false"}) {
		t.Fatalf("datacenter fields = %v", got)
	}
}

func TestNewValidatesDeployment(t *testing.T) {
	for _, cfg := range []Config{
		{Deployment: "", PollInterval: 1},
		{Deployment: "datacenter", PollInterval: 1},
		{Deployment: "cloud", URL: "https://acme.atlassian.net", PollInterval: 1},
		{Deployment: "datacenter", URL: "https://jira.example.test"},
	} {
		if _, err := New(cfg); err == nil {
			t.Errorf("New(%+v) succeeded", cfg)
		}
	}
}

func TestCloudSiteURLIsRestricted(t *testing.T) {
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{"accountId":"bot-1","displayName":"Bot"}`)
	}))
	t.Cleanup(fake.Close)
	s, hits := newCloud(t, fake)
	for _, site := range []string{
		"evil.com", "https://evil.com", "http://x.atlassian.net", "https://x.atlassian.net.evil.com",
		"x.atlassian.net.evil.com", "https://x.atlassian.net@evil.com", "x.atlassian.net@evil.com",
		"https://x.atlassian.net/../", "x.atlassian.net/../", "https://x.atlassian.net:8443", "https://-x.atlassian.net", fake.URL,
	} {
		_, err := s.CompleteConnect(t.Context(), &pb.CompleteConnectRequest{Fields: map[string]string{"siteUrl": site, "email": "bot@example.test", "token": fakeToken}})
		if status.Code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), "siteUrl") || strings.Contains(err.Error(), "evil") {
			t.Errorf("%q: %v; want InvalidArgument naming siteUrl", site, err)
		}
	}
	if n := hits.Load(); n != 0 {
		t.Fatalf("fake saw %d requests", n)
	}
}

func TestCompleteConnectReturnsServerIdentity(t *testing.T) {
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+fakeToken {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		switch r.URL.Path {
		case "/rest/api/2/myself":
			fmt.Fprint(w, `{"key":"JIRAUSER1","name":"orchivio-bot","displayName":"Orchivio Bot"}`)
		case "/rest/api/2/serverInfo":
			fmt.Fprint(w, `{"serverId":"B7AE-1234"}`)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(fake.Close)
	s := newDataCenter(t, fake.URL)
	host := strings.TrimPrefix(fake.URL, "http://")

	resp, err := s.CompleteConnect(t.Context(), &pb.CompleteConnectRequest{Fields: map[string]string{"token": fakeToken, "jql": "labels = bot"}})
	if err != nil {
		t.Fatal(err)
	}
	var cred credential
	if json.Unmarshal(resp.GetCredential().GetData(), &cred) != nil {
		t.Fatal("credential is not JSON")
	}
	if resp.GetExternalId() != "B7AE-1234" || resp.GetLabel() != host+" (Orchivio Bot)" {
		t.Fatalf("identity = %q, %q", resp.GetExternalId(), resp.GetLabel())
	}
	want := credential{Site: fake.URL, Token: fakeToken, JQL: "labels = bot", BotID: "JIRAUSER1", BotName: "orchivio-bot"}
	if cred != want {
		t.Fatalf("credential = %+v; want %+v", cred, want)
	}
}

func TestCompleteConnectCloudUsesSiteHost(t *testing.T) {
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if email, token, ok := r.BasicAuth(); !ok || email != "bot@example.test" || token != fakeToken || r.Host != "acme.atlassian.net" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		switch r.URL.Path {
		case "/rest/api/2/myself":
			fmt.Fprint(w, `{"accountId":"5b10ac8d82e05b22cc7d4ef5","displayName":"Orchivio"}`)
		case "/rest/api/2/serverInfo":
			fmt.Fprint(w, `{"serverId":"ignored-on-cloud"}`)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(fake.Close)
	s, _ := newCloud(t, fake)

	resp, err := s.CompleteConnect(t.Context(), &pb.CompleteConnectRequest{Fields: map[string]string{"siteUrl": "https://ACME.atlassian.net/", "email": "bot@example.test", "token": fakeToken}})
	if err != nil {
		t.Fatal(err)
	}
	var cred credential
	_ = json.Unmarshal(resp.GetCredential().GetData(), &cred)
	if resp.GetExternalId() != "acme.atlassian.net" || resp.GetLabel() != "acme.atlassian.net (Orchivio)" ||
		cred.Site != "https://acme.atlassian.net" || cred.BotID != "5b10ac8d82e05b22cc7d4ef5" || cred.BotName != "5b10ac8d82e05b22cc7d4ef5" {
		t.Fatalf("CompleteConnect = %v, %+v", resp, cred)
	}
}

func TestCompleteConnectRejectedTokenNeverEchoes(t *testing.T) {
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	t.Cleanup(fake.Close)
	s := newDataCenter(t, fake.URL)

	_, err := s.CompleteConnect(t.Context(), &pb.CompleteConnectRequest{Fields: map[string]string{"token": "s3cr3t-value"}})
	if status.Code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), "token rejected") || strings.Contains(err.Error(), "s3cr3t") {
		t.Fatalf("CompleteConnect = %v; want InvalidArgument token rejected", err)
	}
	_, err = s.CompleteConnect(t.Context(), &pb.CompleteConnectRequest{Fields: map[string]string{}})
	if status.Code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), "token") {
		t.Fatalf("missing token: %v", err)
	}
}

func TestFormOnlyConnectionLifecycle(t *testing.T) {
	s := newDataCenter(t, "https://jira.example.test")
	if _, err := s.BeginConnect(t.Context(), &pb.BeginConnectRequest{}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("BeginConnect = %v", err)
	}
	cred := dcCredential(t)
	got, err := s.RefreshCredential(t.Context(), &pb.RefreshCredentialRequest{Credential: cred})
	if err != nil || string(got.GetData()) != string(cred.GetData()) {
		t.Fatalf("RefreshCredential = %v, %v", got, err)
	}
	if _, err := s.Disconnect(t.Context(), &pb.DisconnectRequest{Credential: cred}); err != nil {
		t.Fatalf("Disconnect = %v", err)
	}
}

func TestMalformedCredentialIsNotEchoed(t *testing.T) {
	s := newDataCenter(t, "https://jira.example.test")
	_, err := s.GetTicket(t.Context(), &pb.GetTicketRequest{TicketRef: "ABC-1", Credential: &pb.Credential{Data: []byte(`{"token":"s3cr3t"`)}})
	if status.Code(err) != codes.InvalidArgument || strings.Contains(err.Error(), "s3cr3t") {
		t.Fatalf("GetTicket = %v", err)
	}
}

func TestTLSWithCustomCA(t *testing.T) {
	fake := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"key":"ABC-1","fields":{"summary":"Secure"}}`)
	}))
	t.Cleanup(fake.Close)
	caFile := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: fake.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SSL_CERT_FILE", caFile)
	s := newDataCenter(t, fake.URL)

	ticket, err := s.GetTicket(t.Context(), &pb.GetTicketRequest{TicketRef: "ABC-1", Credential: dcCredential(t)})
	if err != nil || ticket.GetTitle() != "Secure" {
		t.Fatalf("GetTicket = %v, %v", ticket, err)
	}
}
