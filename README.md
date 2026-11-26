# Orchivio Jira adapter

A `plugin.v1` tracker adapter for Jira Data Center 11 and Jira Cloud. Tenants connect their own technical user by form; the adapter keeps no tenant access itself, every call carries the connection's credential.

- **Data Center:** one process per instance (`JIRA_URL`), personal access token as Bearer.
- **Cloud:** one process for all sites; the site comes from the connection and must be `https://<name>.atlassian.net`. API token plus e-mail as Basic auth.

Register each process under its own name in the orchestrator's `ORCHIVIO_ADAPTERS` (for example `jira-cloud=…,jira-acme-dc=…`).

## Configuration (operator)

| Variable | Value |
| --- | --- |
| `JIRA_DEPLOYMENT` | `datacenter` or `cloud` (required) |
| `JIRA_URL` | Data Center base URL including any context path; required for `datacenter`, must be empty for `cloud` |
| `JIRA_POLL_INTERVAL` | Go duration, default `30s` |
| `ORCHIVIO_GRPC_ADDR` | gRPC listen address, default `127.0.0.1:7301` |

## Connection form (tenant)

| Field | Cloud | Data Center |
| --- | --- | --- |
| `siteUrl` | required, `https://<name>.atlassian.net` | – |
| `email` | required | – |
| `token` | required, secret (API token) | required, secret (PAT) |
| `jql` | optional | optional |

Connecting calls `GET /rest/api/2/myself` (a rejected token fails with `token rejected`, the value is never echoed), lets Jira parse a custom `jql` (`search` with `maxResults=0`; a rejection fails with `field jql: rejected by Jira`) and calls `GET /rest/api/2/serverInfo`. The connection's `external_id` is the Data Center `serverId` or the Cloud site host; its label is `<host> (<technical user>)`. A token rejected later (HTTP 401) makes every call fail with `UNAUTHENTICATED`, and the orchestrator marks the connection `needs_reauth`.

## Events

Each `StreamEvents` call polls its project keys with `project in (<keys>) [AND (<jql>)] AND updated >= <cursor> ORDER BY updated ASC`. The cursor is epoch milliseconds; every poll rereads a two-minute overlap for Jira's minute-resolution dates and delayed indexing, and events are deduplicated by `external_id`. Read state is kept in memory per connection and project key and starts at the first subscription (no replay).

- `ASSIGNED`: a changelog entry assigns the issue to the technical user.
- `COMMENTED` with `mentions_bot`: a comment mentions the technical user (`[~username]` on Data Center, `[~accountid:…]` on Cloud).
- The technical user's own comments and changes never produce events.
- An issue that fails to load (other than HTTP 401) is logged and skipped for that poll; its events in that window are lost.

Actors carry `id` (DC user key or Cloud accountId), `username` (DC username or Cloud accountId), `name` and, when Jira returns it, `email`.

`SetStatus` runs the transition whose target status matches the requested name (case-insensitive); with none available it fails with `FAILED_PRECONDITION`.

## Network

Outbound HTTP honours `HTTPS_PROXY`, `HTTP_PROXY` and `NO_PROXY`. `SSL_CERT_FILE` adds a CA bundle to the system roots on every platform; `SSL_CERT_DIR` works on Linux through the Go standard library.

## Build and test

`go.mod` requires the published contract; until `contract/v0.4.0` is tagged, build inside the parent workspace whose `go.work` uses `./orchestrator/contract`:

```sh
go vet ./... && go test -race ./...
```
