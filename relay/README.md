# Burla Relay

Reverse-tunnel relay every Burla cluster connects through: user projects need
**zero inbound firewall rules**. Nodes and the head VM dial out to this relay
with [frp](https://github.com/fatedier/frp); clients and browsers connect to
`https://<subdomain>.relay.burla.dev`, and frps routes each connection down
the matching tunnel by SNI. TLS is never terminated here: node traffic is
encrypted with cluster-CA certs, while dashboard and node-to-head traffic use
the head's ACME cert (issued through the tunnel via TLS-ALPN-01).

Hostname convention (enforced by `auth_plugin.py`):

- `head--<project-id>.relay.burla.dev` -> deployed head VM (dashboard + API)
- `burla-node-<8 hex>--<project-id>.relay.burla.dev` -> one node
- `head-<random>--<project-id>.relay.burla.dev` -> a client-hosted head
  (main_service running inside the `burla` pip package on a user's machine;
  the default mode). The relay accepts only these exact shapes, preventing
  project IDs containing `--` from colliding with another project's tunnels.

Nodes use the relay for head traffic and node-to-node input transfers in every
cloud mode. This keeps regional networking uniform and requires no inbound
firewall rules or private-network peering. Dev clusters point at a test relay
via `BURLA_RELAY_HOST` (see the makefile).

## Deployments

**The prod relay is live**: `relay.burla.dev` (EC2 `burla-relay` in the
burla-prod AWS account, us-east-1, elastic IP 100.57.81.195, validating tokens
against backend.burla.dev). DNS is two static A records in the burla.dev zone
at Namecheap: `relay` and `*.relay`, both pointing at the elastic IP. The IP
never changes across redeploys (the elastic IP is tagged `burla-relay` and
reused), so DNS is one-time setup.

**The test relay is live**: `relay.test-clusters.burla.dev` (EC2 `burla-relay`
in the burla-test AWS account, elastic IP 35.174.220.176, validating tokens
against test.backend.burla.dev). Its DNS lives in the Route53 zone for
`test-clusters.burla.dev` in the same account, so no registrar access is
needed. Dev clusters point at it via `BURLA_RELAY_HOST` (the makefile's
remote-dev default).

### Deploying / updating

Every GitHub release re-runs `deploy_relay_aws.sh` against burla-prod (the
`deploy-relay` job in `.github/workflows/pypi-on-release.yml`, authenticated
via the GitHub OIDC role `burla-relay-deployer` in burla-prod). The script
fingerprints everything the VM derives from this repo (auth plugin, frps.toml,
frp version, backend URL, instance type) and tags the instance with it: a
relay already running the current config is left alone, so the job is a no-op
unless something under `relay/` changed. When it did change, a replacement VM
boots first and the elastic IP moves only once the new frps accepts
connections, so live tunnels see a seconds-long blip and frpc's retry loop
reconnects them.

Manual run (same script CI uses):

```bash
# AWS (prod):
./deploy_relay_aws.sh --profile burla-prod --region us-east-1 \
    --subdomain-host relay.burla.dev
# GCP (no deployment exists; script predates the AWS one):
./deploy_relay.sh --project burla-prod --region us-central1 \
    --subdomain-host relay.burla.dev
```

Local stack for development: `docker compose up` (see `e2e/run_e2e.sh` for the
end-to-end test).

## Remaining integration points outside this repo

1. **Backend**: `POST /v1/clusters/{project_id}/dashboard` now receives an
   optional `"dashboard_url"` field in relay mode; store and return it from
   `GET .../dashboard_url` so `burla login` sends clients to the relay URL.
   (Token validation needs nothing new: the plugin authenticates cluster
   tokens with `GET /v1/clusters/{project_id}/dashboard_url` - a 409
   "registered but no dashboard" response must count as valid, since
   client-hosted clusters never register a dashboard.)
2. **Let's Encrypt rate limits**: each deployed cluster head requests a cert
   for `head--<project-id>.relay.burla.dev`. burla.dev is one registered domain, so
   the default 50 certs/week cap applies; request a rate-limit increase before
   this exceeds ~40 new clusters/week. (Client-hosted heads use cluster-CA
   certs, not ACME, so they don't count against this.)
3. **Backend, for multi-machine token recovery**: the cluster token now lives
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
