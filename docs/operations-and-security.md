# Local authentication, ACLs and operational safety

E01A adds AUTH/ACL and E01B adds optional TLS and authenticated private replication.
All listeners and upstream addresses remain restricted to literal loopback IPs;
this is not production certification. Without TLS, passwords cross TCP in plaintext.
Do not expose or proxy this service to untrusted networks.

## Development and protected startup

Without an ACL file, the existing local development user remains
`default on nopass +@all ~* &*`. New connections are authenticated as that user.
Creating a named user alone does **not** protect the default account.

For protected local use, create a new private bootstrap file interactively:

```text
python -m pyredis_engine.security.credentials build/local-acl.json
python -m pyredis_engine --acl-file build/local-acl.json
```

The helper prompts twice without echo, aborts if a private prompt is unavailable,
and never overwrites an existing file. It creates an enabled password-bearing
`admin` and leaves the implicit default user disabled. Use strong, unique passwords
and a prompting client; do not put credentials in shell arguments/history. Keep the
file out of Git and protect its parent directory. POSIX creation requests 0600;
Windows file ACLs remain the operator's responsibility and are not managed here.

`ServerConfig.acl_file` stores only a filename. The version-1 JSON format is:

```text
{"version":1,"users":[{"name":"admin",
 "rules":["on","+@all","~*","&*"],
 "passwords":["pbkdf2-sha256$600000$<32 hex salt characters>$<64 hex digest characters>"]}]}
```

The placeholders above are not usable verifiers; use the helper or PasswordHash API.
File rules cannot contain plaintext password operations. Parsing rejects unknown or
duplicate fields/users, invalid versions/verifiers, oversized/nonregular files and,
on POSIX, symlinks or group/other permissions. Protected policy requires at least
one enabled password-bearing user with SETUSER/DELUSER capabilities and forbids
enabled nopass users. Missing or unsafe configuration prevents listening; there is
no fallback to development mode. No password-strength estimator is implemented.

## Supported wire subset

| Command | Behavior |
|---|---|
| AUTH password | Authenticate as default. An enabled default with nopass reports Redis's no-password-configured error. |
| AUTH username password | Authenticate as an enabled named user. Unknown/disabled users and incorrect passwords share WRONGPASS. |
| ACL WHOAMI | Current username as a bulk string; requires its command permission. |
| ACL USERS | Sorted usernames; requires the explicit administrative capability. No hashes returned. |
| ACL SETUSER username [rules ...] | Atomically create/update a user; new users start off with no passwords or permissions. |
| ACL DELUSER username [username ...] | Atomically remove users and close their sessions. The default user cannot be deleted. |
| ACL DRYRUN username command [arg ...] | Check current command/key/channel policy without accessing data. OK or a bulk denial description. |

AUTH, QUIT and a valid HELLO with AUTH are available before authentication. Normal framing/arity limits
still apply. All ACL administration requires explicit command capabilities;
`+acl|setuser`, `+acl|deluser` and `+acl` are powerful administrative grants, and
`+@all` includes them. Use explicit application grants rather than +@all.

Supported SETUSER rules (incremental, ordered):

- `on`, `off`, `reset`, `nopass`, `resetpass`.
- `>password`, `<password` add/remove a password. At most one such operation per
  request and four password slots per user. Empty/oversized passwords are rejected.
- `+command`, `-command`; `+acl|subcommand`, `-acl|subcommand` for supported verbs;
  `+@all`, `-@all`, `allcommands`, `nocommands`.
- `~pattern`, `allkeys`, `resetkeys`; `&pattern`, `allchannels`, `resetchannels`.
  Binary patterns support `*`, `?`, character classes/ranges, `^` class negation
  and backslash escaping. Invalid/unclosed patterns are rejected explicitly.

Rule keywords/command names are case-insensitive; usernames, keys, passwords and
channel patterns retain their bytes/case. Usernames are printable non-whitespace
ASCII (1-64 bytes). File rules are UTF-8 JSON text; wire keys/channels/passwords and
patterns remain binary-safe. Global commands such as FLUSHDB/DBSIZE are not scoped
to a namespace by key patterns: do not grant them to namespace-limited users.

Unsupported syntax returns an error: other categories, selectors, `%R~`/`%W~`,
subcommand/first-argument grants outside the supported ACL verbs, Redis SHA-256
`#hash`/`!hash` imports, ACL GETUSER/LIST/LOAD/SAVE/LOG/CAT/GENPASS and other unlisted
subcommands. GETUSER/LIST are not faked with empty/redacted password data. Diagnostics
for unsupported/unsafe policy omit supplied rule text, credentials and hash values;
not all administrative error wording matches Redis.

## Transactions, revocation and persistence

Authorization occurs before key lookup (including passive expiry), queueing,
persistence preflight and special INFO/private-sync dispatch. Every command has
an explicit key specification; missing metadata prevents startup. All keys and
channels in multi-argument operations must be authorized before any effects.

EXEC checks current queued-command permissions again. As independently observed in
Redis 7.4.12, a revoked command returns a NOPERM element without its effects while
authorized siblings may still commit. Denied commands are excluded from journal
capacity estimates. E00 still rejects the entire provisional batch if its complete
reply exceeds the configured cap. Revoked access to a watched key aborts before
reading/expiring it and clears WATCH state. See [transactions](transactions.md).
AUTH/ACL controls are explicitly unqueueable in this subset; they poison a MULTI
batch without retaining credential arguments. This differs from general Redis
transaction support for connection/administrative controls.

Password changes and off affect future authentication; existing identities remain
authenticated, following Redis. Command/key/channel changes apply to existing
identities immediately. DELUSER closes sessions and clears queues/watches; recreating
a name cannot revive an old identity. Revoked channel access disconnects affected
subscribers and clears queued output. Bytes already sent cannot be recalled.
Successful AUTH clears WATCH state; failed AUTH preserves the previous identity.

**Runtime ACL changes are volatile.** They are not database/journal/snapshot effects
or a replicated control plane. Restart reloads the private bootstrap file, including
its old permissions. Update that file offline for durable policy changes; do not
treat a runtime-only revocation as persistent. ACL LOAD/SAVE are unsupported.
Policy updates cannot remove the last usable protected administrator. Already
admitted writes can finish their durability barrier after a later revocation;
requests still waiting for admission are checked against the new policy.

## Credentials and bounds

Verifiers use random 16-byte salts and PBKDF2-HMAC-SHA256 with 600,000 iterations;
digest comparison uses hmac.compare_digest and checks all configured slots without
early match exit. This follows the [OWASP PBKDF2 work-factor guidance](https://cheatsheetseries.owasp.org/cheatsheets/Password_Storage_Cheat_Sheet.html),
not a FIPS certification or identical total login timing promise. Verifiers have
redacted repr; no hashes/passwords are exposed through ACL inspection, metrics,
ordinary errors, configuration objects or journal effects. Python cannot guarantee
credential-buffer zeroization. Bootstrap verifiers still need filesystem protection.

One owned worker performs credential work outside the event loop, with at most four
outstanding jobs; saturation returns BUSY. After waiting, the execution thread
rechecks actor permissions and immutable target/authentication snapshots. Deleted
clients cannot be revived by completed jobs. Five failed credential checks close a
connection; this is not distributed/IP-based brute-force protection.

Embedding callers use `await Executor.security_command(args, client)` for AUTH/ACL
preparation. Ordinary execute remains synchronous and does not perform expensive
password work. The TCP server routes security requests through this gateway.

Other policy caps: 64 users, 64 KiB policy source/file budget, 64 rules/16 KiB per
update, 32 patterns per key/channel domain, 128 bytes per pattern and 1,024 bytes
per password. Matching uses a non-backtracking state machine and a shared 1,000,000
byte-work budget per authorization; over-budget matching denies access. Literal
prefix-plus-star patterns use bounded prefix checks. Policy/worker/transient memory
has separate bounds and is not included in M12 datastore accounting or an RSS ceiling.

## TLS and authenticated replication

TLS is optional and replaces plaintext on the configured listener; the listener
never auto-detects or falls back to plaintext. Configure both `--tls-cert-file` and
`--tls-key-file`, with an optional `--tls-key-password-file`. Password files contain
the exact secret bytes, with no automatic newline removal. The minimum is TLS 1.2;
the standard library and system OpenSSL choose the remaining security settings.
Use `--tls-ca-file` and `--tls-client-auth optional` or `required` to verify client
certificates. `none` (the default) does not authenticate client certificates; TLS
alone does not enable AUTH. The ACL file remains necessary for protected access.
Handshake slots count against `--max-clients` and have a configurable
`--tls-handshake-timeout`. Invalid certificates, keys or trust files fail startup;
bad client handshakes close without a plaintext response.

Secure replication requires `--replication-secure`, a TLS listener and a protected
ACL file on both peers. The primary must grant a dedicated password-bearing user
`+pyredis.sync` (a powerful capability; `+@all` includes it). The replica needs
`--replica-username`, `--replica-password-file`, `--tls-ca-file` and
`--replica-server-name`: the server name is verified against the primary certificate,
including its subject alternative names. The file contains the exact password bytes.
For mutual TLS supply `--replica-tls-cert-file` and `--replica-tls-key-file` plus
optional `--replica-tls-key-password-file`; the primary must use client verification.
Optionally `--replication-client-cert-sha256` pins the SHA-256 fingerprint of the
DER client certificate on the primary; it requires verified client certificates.
Certificate pins are shared peer constraints, not per-user certificate mappings.

For example, with a preconfigured private ACL bootstrap containing a `replica` user
with `+pyredis.sync`, separately provisioned certificates and private password file:

```powershell
python -m pyredis_engine --port 6380 --appendonly .pyredis-data/primary.aof --snapshots --replication-recovery --acl-file private/primary-acl.json --tls-cert-file private/primary.pem --tls-key-file private/primary.key --tls-ca-file private/ca.pem --tls-client-auth required --replication-secure
python -m pyredis_engine --port 6381 --appendonly .pyredis-data/replica.aof --snapshots --replication-recovery --replicaof 127.0.0.1 6380 --acl-file private/replica-acl.json --tls-cert-file private/replica-server.pem --tls-key-file private/replica-server.key --tls-ca-file private/ca.pem --replication-secure --replica-username replica --replica-password-file private/replica.pass --replica-server-name primary.example --replica-tls-cert-file private/replica-client.pem --replica-tls-key-file private/replica-client.key
```

Use certificate names appropriate to the local deployment; `primary.example` must
match the primary certificate. Protect private files and their parent directories.
On POSIX, secret and private-key files must be regular, non-symlink files without
group/other permissions; Windows ACLs are the operator's responsibility. Paths to
private files and secret bytes are excluded from configuration repr and diagnostics.
Startup loads credentials and TLS contexts once; changing files requires restart.
TLS contexts do not provide certificate revocation checking or automatic rotation.

Authentication occurs inside verified TLS before snapshots or effects are sent.
Changing any ACL rule for an active replica identity closes that peer so reconnect
must authenticate again; bytes already sent cannot be recalled. Runtime ACL changes
remain volatile across restart. Replica ACKs are not proof of durable storage.
Legacy unauthenticated replication remains available only as explicit trusted-loopback
development mode and continues to reject ACL mutations; it is not secure replication.
Neither mode lifts the loopback restriction or adds Redis PSYNC interoperability.

Existing protocol, collection, transaction, Pub/Sub and replication limits remain;
see [resource policy](resources-and-operations.md) and [protocol](protocol.md).
Graceful stop drains owned credential work and preserves existing persistence
barriers. Kernel I/O/CPU scheduling and lost post-commit replies retain documented
limitations. Validation does not establish production security.
Reference wire semantics: [Redis AUTH](https://redis.io/docs/latest/commands/auth/)
and [Redis ACL](https://redis.io/docs/latest/operate/oss_and_stack/management/security/acl/).
