package adapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	pb "github.com/orchivio/orchestrator/contract/gen/orchivio/plugin/v1"
	"github.com/orchivio/orchestrator/contract/redact"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Overlap covers Jira's minute-resolution JQL dates and delayed indexing.
const overlap = 2 * time.Minute

type jiraTime struct{ time.Time }

func (t *jiraTime) UnmarshalJSON(raw []byte) error {
	var text string
	if err := json.Unmarshal(raw, &text); err != nil || text == "" {
		return err
	}
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02T15:04:05.999999999-0700"} {
		value, err := time.Parse(layout, text)
		if err == nil {
			t.Time = value
			return nil
		}
	}
	return errors.New("invalid Jira timestamp")
}

// cursorKey keeps read state per connection (site + technical user) and project key.
type cursorKey struct{ site, bot, project string }

type pollCursor struct {
	started, watermark time.Time
	seen               map[string]time.Time
}

type datedEvent struct {
	event *pb.TrackerEvent
	at    time.Time
}

type change struct {
	ID      string
	Created jiraTime
	Author  user
	Items   []struct {
		Field, FieldID, From, To, ToString string
	}
}

type comment struct {
	ID      string
	Body    json.RawMessage
	Created jiraTime
	Author  user
}

// StreamEvents polls the requested project keys with the stream's credential
// until the client or the server goes away.
func (s *Server) StreamEvents(req *pb.StreamEventsRequest, stream pb.TrackerAdapter_StreamEventsServer) error {
	var keys []string
	for _, project := range req.GetProjectKeys() {
		key := strings.ToUpper(project)
		if !projectPattern.MatchString(key) {
			return status.Error(codes.InvalidArgument, "project_keys contains an invalid Jira project key")
		}
		if !slices.Contains(keys, key) {
			keys = append(keys, key)
		}
	}
	if len(keys) == 0 {
		return status.Error(codes.InvalidArgument, "project_keys must not be empty")
	}
	c, err := s.connection(req.GetCredential())
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	stop := context.AfterFunc(s.ctx, cancel)
	defer stop()
	for {
		if err := s.poll(ctx, c, keys, s.now(), stream.Send); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		select {
		case <-ctx.Done():
			return nil
		case <-s.after(s.interval):
		}
	}
}

// poll fetches events since each key's watermark minus overlap and sends the
// unseen ones. A failed fetch is logged and retried on the next tick; a 401
// ends the stream with UNAUTHENTICATED.
func (s *Server) poll(ctx context.Context, c conn, keys []string, now time.Time, send func(*pb.TrackerEvent) error) error {
	s.mu.Lock()
	var earliest time.Time
	windows := make(map[string]time.Time, len(keys))
	curs := make(map[string]*pollCursor, len(keys))
	for _, key := range keys {
		id := cursorKey{c.base.String(), c.cred.BotID, key}
		cur := s.cursors[id]
		if cur == nil {
			cur = &pollCursor{started: now, watermark: now, seen: make(map[string]time.Time)}
			s.cursors[id] = cur
		}
		curs[key] = cur
		window := cur.watermark.Add(-overlap)
		if cur.started.After(window) {
			window = cur.started
		}
		windows[key] = window
		if earliest.IsZero() || window.Before(earliest) {
			earliest = window
		}
	}
	s.mu.Unlock()
	events, err := s.fetch(ctx, c, keys, windows, earliest)
	if errors.Is(err, apiError(http.StatusUnauthorized)) {
		return grpcError(err)
	}
	if err != nil {
		if ctx.Err() == nil {
			slog.Warn("Jira polling failed", "site", c.base.Host, "error", err)
		}
		return nil
	}
	slices.SortStableFunc(events, func(a, b datedEvent) int { return a.at.Compare(b.at) })
	for _, item := range events {
		cur, id := curs[item.event.GetProjectKey()], item.event.GetExternalId()
		s.mu.Lock()
		_, seen := cur.seen[id]
		s.mu.Unlock()
		if seen {
			continue
		}
		if err := send(item.event); err != nil {
			return err
		}
		s.mu.Lock()
		cur.seen[id] = item.at
		s.mu.Unlock()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, cur := range curs {
		if now.After(cur.watermark) {
			cur.watermark = now
		}
		for id, at := range cur.seen {
			if at.Before(cur.watermark.Add(-overlap)) {
				delete(cur.seen, id)
			}
		}
	}
	return nil
}

// pollJQL always restricts to the stream's project keys; a connection's own
// JQL narrows further. The cursor is epoch milliseconds, which JQL reads
// independent of the technical user's time zone.
// ponytail: a custom JQL with its own ORDER BY breaks the query; strip it if tenants need that.
func pollJQL(custom string, keys []string, earliest time.Time) string {
	jql := "project in (\"" + strings.Join(keys, "\",\"") + "\")"
	if custom != "" {
		jql += " AND (" + custom + ")"
	}
	return jql + " AND updated >= " + strconv.FormatInt(earliest.Truncate(time.Minute).UnixMilli(), 10) + " ORDER BY updated ASC, key ASC"
}

func (s *Server) fetch(ctx context.Context, c conn, keys []string, windows map[string]time.Time, earliest time.Time) ([]datedEvent, error) {
	var events []datedEvent
	err := c.search(ctx, pollJQL(c.cred.JQL, keys, earliest), func(item issue) error {
		key, err := ticketKey(item.Key)
		if err != nil {
			return nil
		}
		project, _, _ := strings.Cut(key, "-")
		since, ok := windows[project]
		if !ok {
			return nil
		}
		found, err := c.issueEvents(ctx, key, since)
		if errors.Is(err, apiError(http.StatusNotFound)) {
			return nil
		}
		events = append(events, found...)
		return err
	})
	return events, err
}

func (c conn) search(ctx context.Context, jql string, visit func(issue) error) error {
	query := url.Values{"jql": {jql}, "fields": {"key"}, "maxResults": {"100"}}
	path := "/search"
	if c.cloud {
		path += "/jql"
	}
	for start := 0; ; {
		if !c.cloud {
			query.Set("startAt", strconv.Itoa(start))
		}
		var page struct {
			Issues        []issue
			Total         int
			IsLast        bool
			NextPageToken string
		}
		if err := c.request(ctx, http.MethodGet, path, query, nil, &page); err != nil {
			return err
		}
		for _, item := range page.Issues {
			if err := visit(item); err != nil {
				return err
			}
		}
		if c.cloud {
			if page.IsLast || page.NextPageToken == "" {
				return nil
			}
			if page.NextPageToken == query.Get("nextPageToken") {
				return errors.New("Jira Cloud search pagination did not advance")
			}
			query.Set("nextPageToken", page.NextPageToken)
		} else {
			start += len(page.Issues)
			if start >= page.Total {
				return nil
			}
			if len(page.Issues) == 0 {
				return errors.New("Jira search pagination did not advance")
			}
		}
	}
}

func (c conn) issueEvents(ctx context.Context, key string, since time.Time) ([]datedEvent, error) {
	var events []datedEvent
	add := func(ev *pb.TrackerEvent, at time.Time) {
		if ev != nil && !at.Before(since) {
			events = append(events, datedEvent{ev, at})
		}
	}
	path := "/issue/" + key
	if c.cloud {
		err := readPages(ctx, c, path+"/changelog", "values", func(ch change) {
			add(c.assignment(key, ch), ch.Created.Time)
		})
		if err != nil {
			return nil, err
		}
	} else {
		var item issue
		query := url.Values{"expand": {"changelog"}, "fields": {"key"}}
		if err := c.request(ctx, http.MethodGet, path, query, nil, &item); err != nil {
			return nil, err
		}
		if item.Changelog.Total > len(item.Changelog.Histories) {
			return nil, fmt.Errorf("Jira returned a truncated changelog for %s", key)
		}
		for _, ch := range item.Changelog.Histories {
			add(c.assignment(key, ch), ch.Created.Time)
		}
	}
	err := readPages(ctx, c, path+"/comment", "comments", func(cm comment) {
		if !cm.Created.Before(since) {
			add(c.commented(key, cm), cm.Created.Time)
		}
	})
	return events, err
}

func readPages[T any](ctx context.Context, c conn, path, field string, visit func(T)) error {
	query := url.Values{"maxResults": {"100"}}
	for start := 0; ; {
		query.Set("startAt", strconv.Itoa(start))
		var page struct {
			Values   []T
			Comments []T
			Total    int
			IsLast   bool
		}
		if err := c.request(ctx, http.MethodGet, path, query, nil, &page); err != nil {
			return err
		}
		items := page.Values
		if field == "comments" {
			items = page.Comments
		}
		for _, item := range items {
			visit(item)
		}
		start += len(items)
		if page.IsLast || start >= page.Total {
			return nil
		}
		if len(items) == 0 {
			return fmt.Errorf("Jira pagination did not advance for %s", path)
		}
	}
}

// assignment reports a changelog entry that assigns the issue to the
// technical user, unless the technical user made it.
func (c conn) assignment(key string, ch change) *pb.TrackerEvent {
	actor := c.actor(ch.Author)
	if ch.ID == "" || actor == nil || c.isBot(ch.Author) {
		return nil
	}
	for _, item := range ch.Items {
		if !strings.EqualFold(item.Field, "assignee") && item.FieldID != "assignee" {
			continue
		}
		if item.To == "" || item.To == item.From {
			continue
		}
		if item.To == c.cred.BotID || strings.EqualFold(item.To, c.cred.BotName) {
			project, _, _ := strings.Cut(key, "-")
			return &pb.TrackerEvent{ExternalId: "assign:" + ch.ID, Kind: pb.TrackerEvent_ASSIGNED, TicketRef: key, ProjectKey: project, Actor: actor}
		}
	}
	return nil
}

// commented reports a comment that mentions the technical user (DC
// [~username], Cloud [~accountid:id]), unless the technical user wrote it.
func (c conn) commented(key string, cm comment) *pb.TrackerEvent {
	actor := c.actor(cm.Author)
	if cm.ID == "" || actor == nil || c.isBot(cm.Author) {
		return nil
	}
	prefix := "[~"
	if c.cloud {
		prefix += "accountid:"
	}
	var wiki string
	_ = json.Unmarshal(cm.Body, &wiki)
	body, err := wikiMarkup{}.ToMarkdown(cm.Body)
	if err != nil || !strings.Contains(strings.ToLower(wiki), strings.ToLower(prefix+c.cred.BotName+"]")) {
		return nil
	}
	project, _, _ := strings.Cut(key, "-")
	return &pb.TrackerEvent{ExternalId: "comment:" + cm.ID, Kind: pb.TrackerEvent_COMMENTED, TicketRef: key, ProjectKey: project,
		Actor: actor, Body: redact.String(body), MentionsBot: true}
}
