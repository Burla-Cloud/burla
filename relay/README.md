# Burla Relay

Reverse-tunnel relay every Burla cluster connects through: user projects need
**zero inbound firewall rules**. Nodes and the head VM dial out to this relay
with [frp](https://github.com/fatedier/frp); clients and browsers connect to
`https://<subdomain>.relay.burla.dev`, and frps routes each connection down
the matching tunnel by SNI. TLS is never terminated here: node traffic is
encrypted with cluster-CA certs, dashboard traffic with the head's ACME cert
(issued through the tunnel via TLS-ALPN-01).

Hostname convention (enforced by `auth_plugin.py`):

- `<project-id>.relay.burla.dev` -> deployed head VM (dashboard + API)
- `<instance-name>--<project-id>.relay.burla.dev` -> one node
- `head-<random>--<project-id>.relay.burla.dev` -> a client-hosted head
  (main_service running inside the `burla` pip package on a user's machine;
  the default mode). Same `--<project-id>` ownership rule as nodes.

Only client-facing traffic rides the relay. Node-to-node input transfers,
head-to-node status polls, and node-to-head state pushes stay inside the VPC
(on GCP this relies on the default network's `default-allow-internal` rule;
on AWS the installer creates group-to-group security group rules). Dev
clusters point at a test relay via `BURLA_RELAY_HOST` (see the makefile).

## Deploy (Burla-operated)

One VM per region, in a Burla-owned project:

```bash
./deploy_relay.sh --project burla-prod --region us-central1 \
    --subdomain-host relay.burla.dev
```

Then create the printed DNS records (`*.relay.burla.dev` and apex A records).
Local stack for development: `docker compose up` (see `e2e/run_e2e.sh` for the
end-to-end test).

## Remaining integration points outside this repo

1. **DNS**: wildcard `*.relay.burla.dev` A record per relay IP.
2. **Backend**: `POST /v1/clusters/{project_id}/dashboard` now receives an
   optional `"dashboard_url"` field in relay mode; store and return it from
   `GET .../dashboard_url` so `burla login` sends clients to the relay URL.
   (Token validation needs nothing new: the plugin authenticates cluster
   tokens with `GET /v1/clusters/{project_id}/dashboard_url` - a 409
   "registered but no dashboard" response must count as valid, since
   client-hosted clusters never register a dashboard.)
3. **Let's Encrypt rate limits**: each deployed cluster head requests a cert
   for `<project-id>.relay.burla.dev`. burla.dev is one registered domain, so
   the default 50 certs/week cap applies; request a rate-limit increase before
   this exceeds ~40 new clusters/week. (Client-hosted heads use cluster-CA
   certs, not ACME, so they don't count against this.)
4. **Backend, for multi-machine token recovery**: the cluster token now lives
   in Burla's local state dir instead of Secret Manager/SSM. A user's second
   machine gets credentials via `burla login`, but recovering the *cluster*
   token (e.g. laptop wiped) currently requires support. A
   `GET /v1/clusters/{id}/token` endpoint gated on authorized-user auth would
   close this gap.

## Scaling notes

- One `e2-standard-4` (~10 Gbps NIC) per region is the starting point; frps is
  a dumb byte-shoveler so CPU is rarely the limit. Add VMs + DNS round-robin
  (per-region names like `us-central1.relay.burla.dev`) when saturated.
- Deploy relays in every region clusters run in, and route each cluster to its
  regional relay: a cross-region relay hop multiplies client latency several
  times over.
- Every byte of results downloaded through the relay is internet egress billed
  to the relay project (~$0.08-0.12/GB). Since all clusters relay all traffic,
  monitor this per cluster (frp supports per-proxy bandwidth limits if a cap
  is ever needed).
