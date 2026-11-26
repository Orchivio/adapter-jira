package adapter

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"regexp"
	"strings"

	pb "github.com/orchivio/orchestrator/contract/gen/orchivio/plugin/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// credential is what the orchestrator stores per tenant connection. BotID is
// the technical user's immutable id (Cloud accountId, DC user key); BotName is
// the name used in mentions (Cloud accountId, DC username).
type credential struct {
	Site    string `json:"site"`
	Email   string `json:"email,omitempty"`
	Token   string `json:"token"`
	JQL     string `json:"jql,omitempty"`
	BotID   string `json:"botId"`
	BotName string `json:"botName"`
}

// cloudSite is the only Jira Cloud site shape a tenant may connect to; it
// excludes ports, paths, user info and look-alike hosts.
var cloudSite = regexp.MustCompile(`^https://([a-z0-9][a-z0-9-]*)\.atlassian\.net/?$`)

func normalizeCloudSite(raw string) (string, bool) {
	site := strings.ToLower(strings.TrimSpace(raw))
	if !cloudSite.MatchString(site) {
		return "", false
	}
	return strings.TrimSuffix(site, "/"), true
}

var (
	cloudFields = []*pb.ConnectField{
		{Name: "siteUrl", Label: "Site URL (https://<name>.atlassian.net)", Required: true},
		{Name: "email", Label: "E-mail of the technical user", Required: true},
		{Name: "token", Label: "API token", Secret: true, Required: true},
		{Name: "jql", Label: "JQL filter (optional)"},
	}
	dataCenterFields = []*pb.ConnectField{
		{Name: "token", Label: "Personal access token", Secret: true, Required: true},
		{Name: "jql", Label: "JQL filter (optional)"},
	}
)

// conn is one tenant connection, rebuilt from the credential on every call.
type conn struct {
	http  *http.Client
	base  *url.URL
	cloud bool
	cred  credential
}

func (c conn) request(ctx context.Context, method, path string, query url.Values, in, out any) error {
	return request(ctx, c.http, c.base, "/rest/api/2"+path, method, query, func(r *http.Request) {
		if c.cloud {
			r.SetBasicAuth(c.cred.Email, c.cred.Token)
		} else {
			r.Header.Set("Authorization", "Bearer "+c.cred.Token)
		}
	}, in, out)
}

func (c conn) username(u user) string {
	if c.cloud {
		return u.AccountID
	}
	return u.Name
}

func (c conn) userID(u user) string {
	if !c.cloud && u.Key != "" {
		return u.Key
	}
	return c.username(u)
}

func (c conn) actor(u user) *pb.Actor {
	id := c.userID(u)
	if id == "" {
		return nil
	}
	return &pb.Actor{Id: id, Username: c.username(u), Email: u.Email, Name: u.DisplayName}
}

// isBot drops the technical user's own comments and changes (no loops).
func (c conn) isBot(u user) bool {
	return (u.Key != "" && u.Key == c.cred.BotID) || (u.AccountID != "" && u.AccountID == c.cred.BotID) ||
		(c.username(u) != "" && strings.EqualFold(c.username(u), c.cred.BotName))
}

// connection decodes a stored credential; errors never echo its data.
func (s *Server) connection(pc *pb.Credential) (conn, error) {
	var cred credential
	if len(pc.GetData()) == 0 {
		return conn{}, status.Error(codes.InvalidArgument, "credential required")
	}
	if json.Unmarshal(pc.GetData(), &cred) != nil || cred.Token == "" || cred.BotID == "" {
		return conn{}, status.Error(codes.InvalidArgument, "credential malformed")
	}
	c := conn{http: s.http, base: s.base, cloud: s.cloud, cred: cred}
	if s.cloud {
		site, ok := normalizeCloudSite(cred.Site)
		if !ok || cred.Email == "" {
			return conn{}, status.Error(codes.InvalidArgument, "credential malformed")
		}
		c.base, _ = url.Parse(site)
	}
	return c, nil
}

func (s *Server) BeginConnect(context.Context, *pb.BeginConnectRequest) (*pb.BeginConnectResponse, error) {
	return nil, status.Error(codes.Unimplemented, "Jira connects by form; use CompleteConnect with fields")
}

// CompleteConnect checks the technical user's token and returns the stored
// credential. Errors name the field, never its value.
func (s *Server) CompleteConnect(ctx context.Context, req *pb.CompleteConnectRequest) (*pb.CompleteConnectResponse, error) {
	fields := req.GetFields()
	fieldDefs := dataCenterFields
	if s.cloud {
		fieldDefs = cloudFields
	}
	for _, f := range fieldDefs {
		if f.GetRequired() && strings.TrimSpace(fields[f.GetName()]) == "" {
			return nil, status.Errorf(codes.InvalidArgument, "field %s is required", f.GetName())
		}
	}
	c := conn{http: s.http, base: s.base, cloud: s.cloud,
		cred: credential{Token: strings.TrimSpace(fields["token"]), JQL: strings.TrimSpace(fields["jql"])}}
	if s.cloud {
		site, ok := normalizeCloudSite(fields["siteUrl"])
		if !ok {
			return nil, status.Error(codes.InvalidArgument, "field siteUrl: expected https://<name>.atlassian.net")
		}
		c.base, _ = url.Parse(site)
		c.cred.Email = strings.TrimSpace(fields["email"])
	}
	c.cred.Site = c.base.String()

	var me user
	if err := c.request(ctx, http.MethodGet, "/myself", nil, nil, &me); err != nil {
		if errors.Is(err, apiError(http.StatusUnauthorized)) || errors.Is(err, apiError(http.StatusForbidden)) {
			return nil, status.Error(codes.InvalidArgument, "field token: token rejected")
		}
		return nil, grpcError(err)
	}
	c.cred.BotID, c.cred.BotName = c.userID(me), c.username(me)
	if c.cred.BotID == "" || c.cred.BotName == "" {
		return nil, status.Error(codes.Unavailable, "Jira did not return the technical user's identity")
	}
	var info struct{ ServerID string }
	if err := c.request(ctx, http.MethodGet, "/serverInfo", nil, nil, &info); err != nil {
		return nil, grpcError(err)
	}
	externalID := c.base.Host
	if !s.cloud {
		if info.ServerID == "" {
			return nil, status.Error(codes.Unavailable, "Jira did not return a server id")
		}
		externalID = info.ServerID
	}
	data, _ := json.Marshal(c.cred) // strings only: cannot fail
	// Static tokens: no refresh_after.
	return &pb.CompleteConnectResponse{
		Credential: &pb.Credential{Data: data},
		Label:      c.base.Host + " (" + me.DisplayName + ")",
		ExternalId: externalID,
	}, nil
}

// RefreshCredential returns the static token unchanged; a revoked token shows
// up as UNAUTHENTICATED on the next call.
func (s *Server) RefreshCredential(_ context.Context, req *pb.RefreshCredentialRequest) (*pb.Credential, error) {
	if _, err := s.connection(req.GetCredential()); err != nil {
		return nil, err
	}
	return req.GetCredential(), nil
}

// Disconnect has nothing to revoke: the tenant owns the token.
func (s *Server) Disconnect(context.Context, *pb.DisconnectRequest) (*pb.DisconnectResponse, error) {
	return &pb.DisconnectResponse{}, nil
}

// CheckAccess denies project keys the technical user cannot see.
func (s *Server) CheckAccess(ctx context.Context, req *pb.CheckAccessRequest) (*pb.CheckAccessResponse, error) {
	if len(req.GetCredentials()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "credentials required")
	}
	c, err := s.connection(req.GetCredentials()[0]) // one Jira connection per tenant
	if err != nil {
		return nil, err
	}
	resp := &pb.CheckAccessResponse{}
	for _, item := range req.GetItems() {
		key := strings.ToUpper(item)
		if !projectPattern.MatchString(key) {
			resp.Denied = append(resp.Denied, item)
			continue
		}
		err := c.request(ctx, http.MethodGet, "/project/"+key, nil, nil, nil)
		switch {
		case errors.Is(err, apiError(http.StatusNotFound)), errors.Is(err, apiError(http.StatusForbidden)):
			resp.Denied = append(resp.Denied, item)
		case err != nil:
			return nil, grpcError(err)
		}
	}
	return resp, nil
}
