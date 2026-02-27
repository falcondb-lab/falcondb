# Authentication

FalconDB supports PostgreSQL-compatible authentication methods for client connections.

## Authentication Methods

### Trust (default)

No authentication required. Any client can connect with any username.

```toml
[server.auth]
method = "trust"
```

> **Warning:** Trust mode should only be used in development. Production Safety Mode (PS-3) warns when trust is enabled.

### Cleartext Password

Client sends password in cleartext. Simple but not recommended for production.

```toml
[server.auth]
method = "password"
password = "mysecret"
```

### MD5

Client sends MD5-hashed password (PostgreSQL auth type 5).

```toml
[server.auth]
method = "md5"
password = "mysecret"
username = "falcon"
```

### SCRAM-SHA-256 (recommended)

PostgreSQL 10+ standard. The password never crosses the wire in plaintext.
Uses PBKDF2-HMAC-SHA-256 key derivation with stored verifiers.

```toml
[server.auth]
method = "scram-sha-256"

[[server.auth.users]]
username = "alice"
scram = "SCRAM-SHA-256$4096:c2FsdHNhbHQ=$/STORED_KEY_B64:SERVER_KEY_B64"

[[server.auth.users]]
username = "bob"
password = "bobspassword"   # ephemeral verifier generated at startup
```

## SCRAM-SHA-256 Details

### Protocol Flow

1. Client sends **StartupMessage** with `user` parameter
2. Server sends **AuthenticationSASL** with mechanism `SCRAM-SHA-256`
3. Client sends **SASLInitialResponse** with client-first-message
4. Server sends **AuthenticationSASLContinue** with server-first-message
5. Client sends **SASLResponse** with client-final-message (includes proof)
6. Server verifies proof, sends **AuthenticationSASLFinal** with server signature
7. Server sends **AuthenticationOk**

### Verifier Format

Stored verifiers follow the PostgreSQL format:

```
SCRAM-SHA-256$<iterations>:<salt_base64>$<stored_key_base64>:<server_key_base64>
```

- **iterations**: PBKDF2 iteration count (default: 4096)
- **salt**: 16 random bytes, base64-encoded
- **stored_key**: SHA-256(HMAC(SaltedPassword, "Client Key")), base64-encoded
- **server_key**: HMAC(SaltedPassword, "Server Key"), base64-encoded

The plaintext password is never stored. Only the derived keys are kept.

### User Credential Lookup

The server resolves credentials in this order:

1. **Config users list**: `server.auth.users[]` entries with matching username
   - If `scram` field is set: parse as stored verifier
   - If `password` field is set: generate ephemeral verifier at connection time
2. **Catalog role**: `effective_password` from the role catalog
3. **Fallback**: `server.auth.password` (legacy single-user mode)

### Security Properties

- **No plaintext on wire**: SCRAM exchanges only proofs and signatures
- **Stored verifiers**: Server never needs the original password after setup
- **Constant-time comparison**: Proof verification uses constant-time byte comparison
- **Cryptographic nonce**: Server nonce is 24 bytes from OS CSPRNG
- **No credential logging**: Passwords, proofs, and keys are never logged

## Configuration Reference

```toml
[server.auth]
# Authentication method: "trust", "password", "md5", or "scram-sha-256"
method = "scram-sha-256"

# Legacy single-user password (used by password/md5 methods, or as SCRAM fallback)
# password = ""

# Legacy single-user username filter (empty = accept any)
# username = ""

# Named user credentials for SCRAM-SHA-256
[[server.auth.users]]
username = "admin"
scram = "SCRAM-SHA-256$4096:..."   # pre-computed verifier

[[server.auth.users]]
username = "readonly"
password = "readonlypass"           # server generates verifier at startup

# CIDR allowlist (empty = allow all)
# allow_cidrs = ["127.0.0.1/32", "::1/128", "10.0.0.0/8"]
```

## Connecting with psql

```bash
psql -h 127.0.0.1 -p 5433 -U alice -d falcon
# Enter password when prompted
```

## Connecting with pgAdmin

1. Open pgAdmin → **Add New Server**
2. **General** tab: Name = `FalconDB`
3. **Connection** tab:
   - Host: `127.0.0.1`
   - Port: `5433`
   - Username: `alice`
   - Password: *(your password)*
4. Click **Save**

pgAdmin automatically negotiates SCRAM-SHA-256 when the server offers it.

## Generating Verifiers

To pre-compute a SCRAM verifier for use in configuration:

```sql
-- From a PostgreSQL instance (for reference):
SELECT 'SCRAM-SHA-256' || '$' || '4096' || ':' || encode(gen_random_bytes(16), 'base64')
       || '$' || ... ;
```

Or use the FalconDB built-in verifier generation (planned):

```bash
falcon user add --username alice --password
# Prompts for password, outputs verifier string
```
