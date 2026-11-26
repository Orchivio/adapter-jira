// Package adapter implements the plugin.v1 TrackerAdapter for Jira Data
// Center and Jira Cloud. It keeps no tenant access: every call carries the
// tenant connection's credential.
package adapter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	pb "github.com/orchivio/orchestrator/contract/gen/orchivio/plugin/v1"
	"github.com/orchivio/orchestrator/contract/redact"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Config is the operator level (Ebene 1): which Jira instance this process serves.
type Config struct {
	Deployment   string // "datacenter" or "cloud"
	URL          string // datacenter only; cloud sites come from the connection
	PollInterval time.Duration
}

type Server struct {
	pb.UnimplementedTrackerAdapterServer
	http     *http.Client
	base     *url.URL // datacenter instance; nil on cloud
	cloud    bool
	markup   Markup
	interval time.Duration
	now      func() time.Time
	after    func(time.Duration) <-chan time.Time
	ctx      context.Context
	cancel   context.CancelFunc

	mu      sync.Mutex
	cursors map[cursorKey]*pollCursor // read state per connection and project key
}

type user struct {
	AccountID   string
	Key         string
	Name        string
	DisplayName string
	Email       string `json:"emailAddress"`
}

type issue struct {
	ID, Key   string
	Names     map[string]string
	Fields    map[string]json.RawMessage
	Changelog struct {
		Histories []change
		Total     int
	}
}

var (
	projectPattern = regexp.MustCompile(`^[A-Z][A-Z0-9_]*$`)
	ticketPattern  = regexp.MustCompile(`^[A-Z][A-Z0-9_]*-[0-9]+$`)
)

func New(cfg Config) (*Server, error) {
	if cfg.PollInterval <= 0 {
		return nil, errors.New("JIRA_POLL_INTERVAL must be positive")
	}
	client, err := newHTTPClient()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{http: client, markup: wikiMarkup{}, interval: cfg.PollInterval, now: time.Now, after: time.After,
		ctx: ctx, cancel: cancel, cursors: make(map[cursorKey]*pollCursor)}
	switch cfg.Deployment {
	case "cloud":
		if cfg.URL != "" {
			cancel()
			return nil, errors.New("JIRA_URL must be empty for JIRA_DEPLOYMENT=cloud; sites come from tenant connections")
		}
		s.cloud = true
	case "datacenter":
		if s.base, err = parseBase(cfg.URL); err != nil {
			cancel()
			return nil, errors.New("JIRA_URL: " + err.Error())
		}
	default:
		cancel()
		return nil, errors.New("JIRA_DEPLOYMENT must be datacenter or cloud")
	}
	return s, nil
}

// Close ends open streams so GracefulStop does not wait for them.
func (s *Server) Close() { s.cancel() }

func ticketKey(ref string) (string, error) {
	ref = strings.ToUpper(ref)
	if !ticketPattern.MatchString(ref) {
		return "", status.Error(codes.InvalidArgument, "expected a Jira issue key")
	}
	return ref, nil
}

func (s *Server) GetCapabilities(context.Context, *pb.GetCapabilitiesRequest) (*pb.Capabilities, error) {
	fields := dataCenterFields
	if s.cloud {
		fields = cloudFields
	}
	return &pb.Capabilities{AdapterName: "jira", ContractVersion: "plugin.v1", SupportsStatusUpdate: true, ConnectFields: fields}, nil
}

func (s *Server) GetTicket(ctx context.Context, req *pb.GetTicketRequest) (*pb.Ticket, error) {
	c, err := s.connection(req.GetCredential())
	if err != nil {
		return nil, err
	}
	key, err := ticketKey(req.GetTicketRef())
	if err != nil {
		return nil, err
	}
	var item issue
	query := url.Values{"expand": {"names"}, "fields": {"*all"}}
	if err := c.request(ctx, http.MethodGet, "/issue/"+key, query, nil, &item); err != nil {
		return nil, grpcError(err)
	}
	if key, err = ticketKey(item.Key); err != nil {
		return nil, status.Error(codes.Unavailable, "Jira returned an invalid issue key")
	}
	var title string
	var labels []string
	_ = json.Unmarshal(item.Fields["summary"], &title)
	_ = json.Unmarshal(item.Fields["labels"], &labels)
	for i := range labels {
		labels[i] = redact.String(labels[i])
	}
	description, err := s.markup.ToMarkdown(item.Fields["description"])
	if err != nil {
		return nil, status.Error(codes.Unavailable, "Jira returned an invalid description")
	}
	return &pb.Ticket{TicketRef: key, Title: redact.String(title), Description: redact.String(description), Labels: labels,
		Fields: ticketFields(item), Url: strings.TrimRight(c.base.String(), "/") + "/browse/" + key}, nil
}

// ticketFields keys every non-empty field by its display name; duplicate
// names get the field id appended.
func ticketFields(item issue) map[string]string {
	fields := make(map[string]string)
	var fieldIDs []string
	for id := range item.Fields {
		fieldIDs = append(fieldIDs, id)
	}
	slices.Sort(fieldIDs)
	usedNames := make(map[string]bool)
	for _, name := range item.Names {
		usedNames[redact.String(name)] = true
	}
	for _, id := range fieldIDs {
		value := fieldText(item.Fields[id])
		if value == "" {
			continue
		}
		name := redact.String(item.Names[id])
		if name == "" {
			name = id
		}
		if _, exists := fields[name]; exists {
			name += " (" + id + ")"
			for usedNames[name] || fields[name] != "" {
				name += " (" + id + ")"
			}
		}
		fields[name] = redact.String(value)
	}
	return fields
}

func fieldText(raw json.RawMessage) string {
	var compact bytes.Buffer
	if json.Compact(&compact, raw) != nil {
		return ""
	}
	switch value := compact.String(); value {
	case "null", "[]", "{}":
		return ""
	default:
		if value[0] == '"' {
			var text string
			_ = json.Unmarshal(raw, &text)
			if strings.TrimSpace(text) == "" {
				return ""
			}
			return text
		}
		return value
	}
}

func (s *Server) PostComment(ctx context.Context, req *pb.PostCommentRequest) (*pb.PostCommentResponse, error) {
	c, err := s.connection(req.GetCredential())
	if err != nil {
		return nil, err
	}
	key, err := ticketKey(req.GetTicketRef())
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(req.GetBody()) == "" {
		return nil, status.Error(codes.InvalidArgument, "comment body is required")
	}
	raw, err := s.markup.FromMarkdown(req.GetBody())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid comment markup")
	}
	var comment struct{ ID string }
	if err := c.request(ctx, http.MethodPost, "/issue/"+key+"/comment", nil, map[string]json.RawMessage{"body": raw}, &comment); err != nil {
		return nil, grpcError(err)
	}
	if comment.ID == "" {
		return nil, status.Error(codes.Unavailable, "Jira did not return a comment ID")
	}
	return &pb.PostCommentResponse{CommentId: comment.ID}, nil
}

// SetStatus runs the transition whose target status matches (case-insensitive).
func (s *Server) SetStatus(ctx context.Context, req *pb.SetStatusRequest) (*pb.SetStatusResponse, error) {
	c, err := s.connection(req.GetCredential())
	if err != nil {
		return nil, err
	}
	key, err := ticketKey(req.GetTicketRef())
	if err != nil {
		return nil, err
	}
	target := strings.TrimSpace(req.GetStatus())
	if target == "" {
		return nil, status.Error(codes.InvalidArgument, "status is required")
	}
	path := "/issue/" + key
	current, err := c.currentStatus(ctx, path)
	if err != nil {
		return nil, grpcError(err)
	}
	if strings.EqualFold(current, target) {
		return &pb.SetStatusResponse{}, nil
	}
	var page struct {
		Transitions []struct {
			ID string
			To struct{ Name string }
		}
	}
	if err := c.request(ctx, http.MethodGet, path+"/transitions", nil, nil, &page); err != nil {
		return nil, grpcError(err)
	}
	for _, t := range page.Transitions {
		if t.ID == "" || !strings.EqualFold(t.To.Name, target) {
			continue
		}
		body := map[string]map[string]string{"transition": {"id": t.ID}}
		err := c.request(ctx, http.MethodPost, path+"/transitions", nil, body, nil)
		if errors.Is(err, apiError(400)) || errors.Is(err, apiError(409)) || errors.Is(err, apiError(422)) {
			// A concurrent change may already have reached the target.
			if now, err := c.currentStatus(ctx, path); err == nil && strings.EqualFold(now, target) {
				return &pb.SetStatusResponse{}, nil
			}
			break
		}
		if err != nil {
			return nil, grpcError(err)
		}
		return &pb.SetStatusResponse{Changed: true}, nil
	}
	return nil, status.Errorf(codes.FailedPrecondition, "Jira issue %s cannot transition to %q", key, target)
}

func (c conn) currentStatus(ctx context.Context, path string) (string, error) {
	var item struct {
		Fields struct {
			Status struct{ Name string }
		}
	}
	err := c.request(ctx, http.MethodGet, path, url.Values{"fields": {"status"}}, nil, &item)
	return item.Fields.Status.Name, err
}
