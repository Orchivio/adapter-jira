package adapter

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/orchivio/orchestrator/contract/gen/orchivio/plugin/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const fakeToken = "fake-token"

// redirect sends every request, whatever its host, to target and counts it.
type redirect struct {
	target *url.URL
	hits   *atomic.Int32
}

func (r redirect) RoundTrip(req *http.Request) (*http.Response, error) {
	r.hits.Add(1)
	req = req.Clone(req.Context())
	req.URL.Scheme, req.URL.Host = r.target.Scheme, r.target.Host
	return http.DefaultTransport.RoundTrip(req)
}

// newCloud returns a cloud server whose HTTP calls all land on fake.
func newCloud(t *testing.T, fake *httptest.Server) (*Server, *atomic.Int32) {
	t.Helper()
	s, err := New(Config{Deployment: "cloud", PollInterval: 30 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	target, _ := url.Parse(fake.URL)
	hits := &atomic.Int32{}
	s.http.Transport = redirect{target, hits}
	return s, hits
}

func newDataCenter(t *testing.T, base string) *Server {
	t.Helper()
	s, err := New(Config{Deployment: "datacenter", URL: base, PollInterval: 30 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func credentialOf(t *testing.T, c credential) *pb.Credential {
	t.Helper()
	data, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	return &pb.Credential{Data: data}
}

func dcCredential(t *testing.T) *pb.Credential {
	return credentialOf(t, credential{Token: fakeToken, BotID: "JIRAUSER1", BotName: "orchivio-bot"})
}

func TestWikiMarkup(t *testing.T) {
	for _, tc := range []struct{ wiki, md string }{
		{"h2. *Heading* _italic_", "## **Heading** _italic_"},
		{"* item\n*# child\n*## grandchild\n* next", "- item\n  1. child\n     1. grandchild\n- next"},
		{"[label|https://example.test] and {{*literal*}} [~accountid:1:abc]", "[label](https://example.test) and `*literal*` @accountid:1:abc"},
		{"{code:go}\n*literal*\n{code}", "```go\n*literal*\n```"},
		{"{quote}\nOne\nTwo\n{quote}", "> One\n> Two"},
		{"|| Key || Value ||\n| one | two |", "| Key | Value |\n| --- | --- |\n| one | two |"},
		{"{unknown}", "{unknown}"},
	} {
		t.Run(tc.wiki, func(t *testing.T) {
			raw, _ := json.Marshal(tc.wiki)
			markup := wikiMarkup{}
			if md, err := markup.ToMarkdown(raw); err != nil || md != tc.md {
				t.Fatalf("ToMarkdown = %q, %v; want %q", md, err, tc.md)
			}
			result, err := markup.FromMarkdown(tc.md)
			var wiki string
			if err != nil || json.Unmarshal(result, &wiki) != nil || wiki != tc.wiki {
				t.Fatalf("FromMarkdown = %s, %v; want %q", result, err, tc.wiki)
			}
		})
	}
	md, err := (wikiMarkup{}).ToMarkdown(json.RawMessage(`"{noformat}\n*literal*\n{noformat}"`))
	if err != nil || md != "```\n*literal*\n```" {
		t.Fatalf("noformat = %q, %v", md, err)
	}
}

func TestJiraREST(t *testing.T) {
	var transitioned atomic.Bool
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+fakeToken {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		switch r.Method + " " + r.URL.Path {
		case "GET /jira/rest/api/2/issue/ABC-1":
			if r.URL.Query().Get("fields") == "status" {
				current := "Open"
				if transitioned.Load() {
					current = "In Progress"
				}
				fmt.Fprintf(w, `{"fields":{"status":{"name":%q}}}`, current)
				return
			}
			if r.URL.Query().Get("expand") != "names" {
				t.Error("missing field names expansion")
			}
			fmt.Fprint(w, `{"key":"ABC-1","names":{"customfield_1":"Custom","customfield_2":"Empty"},"fields":{"summary":"Example","description":"h1. Intro","customfield_1":{"number":42,"secret":"AKIA1234567890123456"},"customfield_2":[],"labels":["demo"]}}`)
		case "GET /jira/rest/api/2/issue/ABC-1/transitions":
			fmt.Fprint(w, `{"transitions":[{"id":"3","to":{"name":"In Progress"}}]}`)
		case "POST /jira/rest/api/2/issue/ABC-1/transitions":
			var body map[string]map[string]string
			if json.NewDecoder(r.Body).Decode(&body) != nil || body["transition"]["id"] != "3" {
				t.Errorf("unexpected transition %v", body)
			}
			transitioned.Store(true)
			w.WriteHeader(http.StatusNoContent)
		case "POST /jira/rest/api/2/issue/ABC-1/comment":
			var body map[string]string
			if json.NewDecoder(r.Body).Decode(&body) != nil || body["body"] != "*done*" {
				t.Errorf("unexpected comment %v", body)
			}
			w.WriteHeader(http.StatusCreated)
			fmt.Fprint(w, `{"id":"987"}`)
		case "GET /jira/rest/api/2/project/ABC":
			fmt.Fprint(w, `{"key":"ABC"}`)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(fake.Close)
	s := newDataCenter(t, fake.URL+"/jira")
	cred := dcCredential(t)
	ctx := t.Context()
	ticket, err := s.GetTicket(ctx, &pb.GetTicketRequest{TicketRef: "ABC-1", Credential: cred})
	if err != nil || ticket.GetDescription() != "# Intro" || ticket.GetFields()["Custom"] != `{"number":42,"secret":"****"}` ||
		ticket.GetFields()["Empty"] != "" || ticket.GetUrl() != fake.URL+"/jira/browse/ABC-1" {
		t.Fatalf("GetTicket = %v, %v", ticket, err)
	}
	if _, err := s.GetTicket(ctx, &pb.GetTicketRequest{TicketRef: "ABC-99", Credential: cred}); status.Code(err) != codes.NotFound {
		t.Fatalf("missing issue: %v", err)
	}
	req := &pb.SetStatusRequest{TicketRef: "ABC-1", Status: "in progress", Credential: cred}
	for _, changed := range []bool{true, false} {
		if result, err := s.SetStatus(ctx, req); err != nil || result.GetChanged() != changed {
			t.Fatalf("SetStatus = %v, %v; want changed=%v", result, err, changed)
		}
	}
	if result, err := s.PostComment(ctx, &pb.PostCommentRequest{TicketRef: "ABC-1", Body: "**done**", Credential: cred}); err != nil || result.GetCommentId() != "987" {
		t.Fatalf("PostComment = %v, %v", result, err)
	}
	access, err := s.CheckAccess(ctx, &pb.CheckAccessRequest{Credentials: []*pb.Credential{cred}, Items: []string{"ABC", "NOPE", "../x"}})
	if err != nil || !reflect.DeepEqual(access.GetDenied(), []string{"NOPE", "../x"}) {
		t.Fatalf("CheckAccess = %v, %v", access, err)
	}
}

func TestSetStatusPicksTransition(t *testing.T) {
	var posted atomic.Value
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method + " " + r.URL.Path {
		case "GET /rest/api/2/issue/ABC-1":
			fmt.Fprint(w, `{"fields":{"status":{"name":"In Progress"}}}`)
		case "GET /rest/api/2/issue/ABC-1/transitions":
			fmt.Fprint(w, `{"transitions":[{"id":"11","to":{"name":"Done"}},{"id":"21","to":{"name":"In Review"}}]}`)
		case "POST /rest/api/2/issue/ABC-1/transitions":
			var body map[string]map[string]string
			_ = json.NewDecoder(r.Body).Decode(&body)
			posted.Store(body["transition"]["id"])
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(fake.Close)
	s := newDataCenter(t, fake.URL)
	cred := dcCredential(t)

	result, err := s.SetStatus(t.Context(), &pb.SetStatusRequest{TicketRef: "ABC-1", Status: "in review", Credential: cred})
	if err != nil || !result.GetChanged() || posted.Load() != "21" {
		t.Fatalf("SetStatus = %v, %v, transition %v; want changed via 21", result, err, posted.Load())
	}
	_, err = s.SetStatus(t.Context(), &pb.SetStatusRequest{TicketRef: "ABC-1", Status: "Waiting", Credential: cred})
	if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "ABC-1") || !strings.Contains(err.Error(), "Waiting") {
		t.Fatalf("unreachable status: %v", err)
	}
}

func TestRejectedTokenIsUnauthenticated(t *testing.T) {
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	t.Cleanup(fake.Close)
	s := newDataCenter(t, fake.URL)
	cred := dcCredential(t)
	ctx := t.Context()

	_, getErr := s.GetTicket(ctx, &pb.GetTicketRequest{TicketRef: "ABC-1", Credential: cred})
	_, postErr := s.PostComment(ctx, &pb.PostCommentRequest{TicketRef: "ABC-1", Body: "hi", Credential: cred})
	_, statusErr := s.SetStatus(ctx, &pb.SetStatusRequest{TicketRef: "ABC-1", Status: "Done", Credential: cred})
	_, accessErr := s.CheckAccess(ctx, &pb.CheckAccessRequest{Credentials: []*pb.Credential{cred}, Items: []string{"ABC"}})

	for name, err := range map[string]error{"GetTicket": getErr, "PostComment": postErr, "SetStatus": statusErr, "CheckAccess": accessErr} {
		if status.Code(err) != codes.Unauthenticated || strings.Contains(err.Error(), fakeToken) {
			t.Errorf("%s: %v; want Unauthenticated without token", name, err)
		}
	}
}

// fakeStream is the server side of StreamEvents without a network.
type fakeStream struct {
	grpc.ServerStream
	ctx  context.Context
	sent []*pb.TrackerEvent
}

func (f *fakeStream) Context() context.Context { return f.ctx }
func (f *fakeStream) Send(e *pb.TrackerEvent) error {
	f.sent = append(f.sent, e)
	return nil
}

func TestStreamUsesRequestKeysOnly(t *testing.T) {
	searches := make(chan string, 4)
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/rest/api/2/search" {
			http.NotFound(w, r)
			return
		}
		searches <- r.URL.Query().Get("jql")
		fmt.Fprint(w, `{"issues":[],"total":0}`)
	}))
	t.Cleanup(fake.Close)
	s := newDataCenter(t, fake.URL)
	ticks := make(chan time.Time)
	s.after = func(d time.Duration) <-chan time.Time {
		if d != 30*time.Second {
			t.Errorf("interval = %v", d)
		}
		return ticks
	}
	ctx, cancel := context.WithCancel(t.Context())
	stream := &fakeStream{ctx: ctx}
	cred := credentialOf(t, credential{Token: fakeToken, BotID: "JIRAUSER1", BotName: "orchivio-bot", JQL: "labels = bot"})
	done := make(chan error, 1)

	go func() {
		done <- s.StreamEvents(&pb.StreamEventsRequest{ProjectKeys: []string{"a", "B"}, Credential: cred}, stream)
	}()
	first := <-searches
	ticks <- time.Time{}
	second := <-searches
	cancel()

	if err := <-done; err != nil {
		t.Fatalf("StreamEvents = %v", err)
	}
	if !strings.HasPrefix(first, `project in ("A","B") AND (labels = bot) AND updated >= `) || !strings.HasSuffix(first, " ORDER BY updated ASC, key ASC") {
		t.Fatalf("first JQL = %q", first)
	}
	if second == "" {
		t.Fatal("no poll after tick")
	}
}

func TestStreamRejectsInvalidProjectKey(t *testing.T) {
	s := newDataCenter(t, "https://jira.example.test")
	err := s.StreamEvents(&pb.StreamEventsRequest{ProjectKeys: []string{`A") OR project is not EMPTY`}, Credential: dcCredential(t)}, &fakeStream{ctx: t.Context()})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("StreamEvents = %v; want InvalidArgument", err)
	}
}

func TestCloudPollingOverlapAndPagination(t *testing.T) {
	start := time.Date(2026, 11, 24, 10, 15, 0, 0, time.UTC)
	fresh := start.Add(time.Second).Format(time.RFC3339)
	old := start.Add(-time.Second).Format(time.RFC3339)
	var includeLate atomic.Bool
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		email, token, ok := r.BasicAuth()
		if !ok || email != "bot@example.test" || token != fakeToken || r.Host != "acme.atlassian.net" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		switch r.URL.Path {
		case "/rest/api/2/search/jql":
			if jql := r.URL.Query().Get("jql"); !strings.Contains(jql, `project in ("ABC") AND updated >= `) {
				t.Errorf("unexpected JQL %q", jql)
			}
			if r.URL.Query().Get("nextPageToken") == "" {
				fmt.Fprint(w, `{"nextPageToken":"second","issues":[]}`)
			} else {
				fmt.Fprint(w, `{"issues":[{"key":"ABC-1"}],"isLast":true}`)
			}
		case "/rest/api/2/issue/ABC-1/changelog":
			fmt.Fprintf(w, `{"isLast":true,"values":[
				{"id":"new","created":%q,"author":{"accountId":"human","displayName":"Human"},"items":[{"fieldId":"assignee","to":"bot-1"}]},
				{"id":"self","created":%q,"author":{"accountId":"bot-1"},"items":[{"fieldId":"assignee","to":"bot-1"}]}]}`, fresh, fresh)
		case "/rest/api/2/issue/ABC-1/comment":
			if r.URL.Query().Get("startAt") == "0" {
				fmt.Fprintf(w, `{"total":3,"comments":[
					{"id":"old","created":%q,"author":{"accountId":"human"},"body":"[~accountid:bot-1]"},
					{"id":"own","created":%q,"author":{"accountId":"bot-1"},"body":"[~accountid:bot-1] echo"}]}`, old, fresh)
			} else {
				body := "plain comment"
				if includeLate.Load() {
					body = "[~accountid:bot-1] please help"
				}
				fmt.Fprintf(w, `{"total":3,"comments":[{"id":"late","created":%q,"author":{"accountId":"human"},"body":%q}]}`, fresh, body)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(fake.Close)
	s, _ := newCloud(t, fake)
	c, err := s.connection(credentialOf(t, credential{Site: "https://acme.atlassian.net", Email: "bot@example.test", Token: fakeToken, BotID: "bot-1", BotName: "bot-1"}))
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	var events []*pb.TrackerEvent
	send := func(e *pb.TrackerEvent) error {
		events = append(events, e)
		return nil
	}
	for i := range 3 {
		if i == 1 {
			includeLate.Store(true)
		}
		if err := s.poll(ctx, c, []string{"ABC"}, start.Add(time.Duration(i)*30*time.Second), send); err != nil {
			t.Fatal(err)
		}
	}
	var ids []string
	for _, e := range events {
		ids = append(ids, e.GetExternalId())
	}
	if want := []string{"assign:new", "comment:late"}; !reflect.DeepEqual(ids, want) {
		t.Fatalf("events = %v; want %v", ids, want)
	}
	if a := events[0].GetActor(); a.GetId() != "human" || a.GetUsername() != "human" || a.GetName() != "Human" {
		t.Fatalf("actor = %v", a)
	}
}

func TestDataCenterPollMatchesBotKeyAndMention(t *testing.T) {
	start := time.Date(2026, 11, 24, 10, 15, 0, 0, time.UTC)
	fresh := start.Add(time.Second).Format("2006-01-02T15:04:05.000-0700")
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/rest/api/2/search":
			fmt.Fprint(w, `{"issues":[{"key":"ABC-2"}],"total":1}`)
		case "/rest/api/2/issue/ABC-2":
			fmt.Fprintf(w, `{"key":"ABC-2","changelog":{"total":1,"histories":[
				{"id":"7","created":%q,"author":{"key":"JIRAUSER9","name":"jdoe","displayName":"J. Doe","emailAddress":"jdoe@example.test"},"items":[{"field":"assignee","to":"JIRAUSER1"}]}]}}`, fresh)
		case "/rest/api/2/issue/ABC-2/comment":
			fmt.Fprintf(w, `{"total":2,"comments":[
				{"id":"8","created":%q,"author":{"key":"JIRAUSER9","name":"jdoe"},"body":"[~orchivio-bot] fix it"},
				{"id":"9","created":%q,"author":{"key":"JIRAUSER1","name":"orchivio-bot"},"body":"[~orchivio-bot] on it"}]}`, fresh, fresh)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(fake.Close)
	s := newDataCenter(t, fake.URL)
	c, err := s.connection(dcCredential(t))
	if err != nil {
		t.Fatal(err)
	}
	var events []*pb.TrackerEvent
	err = s.poll(t.Context(), c, []string{"ABC"}, start, func(e *pb.TrackerEvent) error {
		events = append(events, e)
		return nil
	})
	if err != nil || len(events) != 2 {
		t.Fatalf("poll = %v, events %v", err, events)
	}
	want := &pb.Actor{Id: "JIRAUSER9", Username: "jdoe", Email: "jdoe@example.test", Name: "J. Doe"}
	if e := events[0]; e.GetExternalId() != "assign:7" || e.GetKind() != pb.TrackerEvent_ASSIGNED || !reflect.DeepEqual(e.GetActor().String(), want.String()) {
		t.Fatalf("assignment = %v", e)
	}
	if e := events[1]; e.GetExternalId() != "comment:8" || !e.GetMentionsBot() || e.GetBody() != "@orchivio-bot fix it" {
		t.Fatalf("comment = %v", e)
	}
}
