# Deployment templates

These files are shareable templates. They intentionally contain only
documentation hostnames, a dedicated service account, and placeholder tokens.
Never replace those placeholders in the repository.

## Secret files

Copy the relevant example to the path referenced by its systemd template,
replace the token, and keep the result root-readable only:

```bash
sudo install -d -m 0700 /etc/agent-registry
openssl rand -hex 32
sudo install -m 0600 agentregistry-artifact-relay.env.example /etc/agent-registry/artifact-relay.env
sudo install -m 0600 agentregistry-tunnel.env.example /etc/agent-registry/tunnel.env
sudo install -m 0600 agentregistry-tcp-tunnel.env.example /etc/agent-registry/tcp-tunnel.env
sudo install -m 0600 agentregistry-stream-proxy.env.example /etc/agent-registry/stream-proxy.env
sudo install -m 0600 agentregistry-groupchat.env.example /etc/agent-registry/groupchat.env
sudo chmod 0600 /etc/agent-registry/*.env
```

The generated token belongs in the installed environment file or a secret
manager, not in source control, shell history, documentation, or a systemd
unit. The repository `.gitignore` blocks common local environment and private
key filenames as a second line of defense.

## Service layout

| Template | Process | Default/example listener |
|---|---|---|
| `agentregistry-artifact-relay.conf` | `agent-registry` | backend HTTP listener, normally `8000` |
| `agentregistry-tunnel.conf` | `agent-registry` | WebSocket listener `8001` |
| `agentregistry-tcp-tunnel.conf` | `agent-registry` | TCP control/data listener `8003`; proxy ports `10000-11000` |
| `agentregistry-stream-proxy.service` | standalone `agent-stream-proxy` | code default `8002`; deployment example `18002` |

Group chat rides the `agent-registry` process and its existing HTTP listener:
it needs no extra port, but it does need a PostgreSQL database, configured
through `agentregistry-groupchat.env`. It stays disabled unless
`A2X_GROUPCHAT_ENABLED=true`, so the template can be installed unconditionally.

Before installation, replace `*.example.com` with the deployment's public
hostname and adapt `/opt/agent-registry` only in the installed systemd unit.
Keep those environment-specific values out of the repository.
