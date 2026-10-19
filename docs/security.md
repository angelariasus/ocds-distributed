# Security: identities, channels, authorization and integrity

Phase 8 answers four questions for a system whose parts talk over a network they do not control:
who is on the other end of a connection, can anyone read or change what travels on it, what is
each caller allowed to do, and how would anyone notice if stored history were altered.

```
client ──JWT──▶ Envoy ──mTLS──▶ api ×3 ──mTLS──▶ ml ×2
                  │ verifies signature, issuer,      │
                  │ audience, expiry (JWKS)          └──▶ authorizes by role
extractor ──mTLS──▶ catalog ×5 ◀──mTLS──▶ catalog (gossip, replication, anti-entropy)
extract.results, gold.events ──▶ ledger (Merkle log, signed heads) ◀── auditor (independent state)
```

## Identities: an internal PKI

`scripts/gen_pki.sh` creates a certificate authority and one identity per service (`catalog`,
`api`, `ml`, `envoy`, `extractor`). Each certificate's subject alternative names are the compose
DNS names the service is reached by, and each is valid for both TLS server and client use, which
is what mutual TLS needs. It also creates a second, unrelated CA with an "intruder" certificate
that claims to be `catalog`, for negative tests; the ES256 key that signs API tokens together with
the JWKS Envoy verifies them against; and the Ed25519 key that signs the ledger's tree heads.
Nothing under `deploy/pki/` is committed.

## Channels: mutual TLS

Plain TLS proves the server's identity to the client. Mutual TLS also proves the client's identity
to the server, which is what a service mesh needs: a catalog node should only accept gossip, replica
writes and anti-entropy exchanges from other catalog nodes, not from anything that reached the
port.

| Connection | Server requires | Client presents |
|---|---|---|
| catalog ↔ catalog (gossip, replication, Merkle sync) | certificate from the internal CA, TLS 1.3 | the node's `catalog` identity |
| extractor → catalog (checkpoints) | same | `extractor` |
| Envoy → api (REST and gRPC) | same | `envoy` |
| api → ml (predictions) | same | `api` |

With mTLS between Envoy and the API, the API is reachable only through Envoy: a container on the
same Docker network that tries to call it directly does not complete the handshake.

## Authentication at the edge, authorization in the service

Envoy authenticates. Its `jwt_authn` filter verifies every request's bearer token against the
JWKS: ES256 signature, issuer `ocds-distributed`, audience `ocds-api`, expiry. A `header_mutation`
filter first removes any `x-jwt-payload` header the client sent, then `jwt_authn` forwards the
*verified* payload in that header to the API.

The API authorizes. A policy maps each operation to the roles allowed to perform it (`reader`
and `analyst` may query; only `analyst` may request predictions) and is enforced on both the REST
handlers and the gRPC methods. Separating the two keeps key material and token parsing in one
place (the edge) and business rules in the service that owns them; it is safe only because the
first hop is the only way in, which is what the mTLS above guarantees.

Tokens are minted with `scripts/mint_token.sh <role> [ttl] [subject]`.

## Measured checks

`scripts/security_checks.sh` exercises every boundary. Results on 2026-10-19:

```
== catalog: mutual TLS between nodes and clients
  PASS  node certificate accepted
  PASS  gossip over mTLS keeps 5 members alive
  PASS  plaintext client rejected
  PASS  certificate from another CA rejected
== extractor -> catalog with its own identity
  PASS  extractor writes and reads through mTLS
== direct access to internal services, bypassing Envoy
  PASS  API without client certificate                      (handshake fails)
  PASS  API with Envoy's certificate (health)               200
  PASS  API with a certificate but no verified identity     401
  PASS  API over plaintext HTTP                             400
  PASS  ML without client certificate                       (handshake fails)
== edge: authentication at Envoy, authorization in the API
  PASS  no token                                            401
  PASS  reader reads                                        200
  PASS  reader predicts                                     403
  PASS  analyst predicts (API -> ML over mTLS)              200
  PASS  expired token                                       401  "Jwt is expired"
  PASS  payload edited to claim analyst                     401  "Jwt verification fails"
  PASS  reader smuggling an analyst identity header         403
  PASS  health needs no token                               200
```

Two of these are worth reading closely. The forged token kept a valid signature but changed one
claim (`roles: [reader]` → `[analyst]`): the signature covers the payload, so the edit invalidates
it. The smuggled header carried a genuine analyst payload copied from another token; Envoy dropped
it before authentication and replaced it with the reader's verified payload, so the API saw a
reader and refused. The API with a valid client certificate but no forwarded identity still
answered 401: transport authentication says *which service* is calling, not *which user*.

## Integrity: a transparency log for the pipeline

Encryption protects data in transit; it says nothing about whether stored history was changed
after the fact. In public procurement that question matters: anyone using these figures should be
able to check that the Bronze file a number came from, and the Gold load that produced it, are the
ones the system recorded at the time.

`services/ledger` is an append-only log in the style of Certificate Transparency (RFC 6962/9162):

- **What it attests.** It consumes `extract.results` and `gold.events` and appends one entry per
  Bronze object stored (`ref` = object key, `digest` = SHA-256 of the inner JSON, the same value
  the portal publishes) and per committed Gold load (`ref` = fencing token, `digest` = SHA-256 of
  the rows that load wrote, computed inside the swap transaction). Appends are idempotent on
  (kind, ref, digest), so Kafka redelivery never duplicates an entry.
- **How.** Entries are leaves of a Merkle tree (leaf and interior hashes domain-separated). Every
  state of the log is summarised by a **signed tree head**: size, root and timestamp, signed with
  Ed25519.
- **Inclusion proof.** Anyone holding a signed head can check that a given entry is in it with
  ⌈log₂ n⌉ hashes, without downloading the log.
- **Consistency proof.** Anyone holding an *older* head can check that the current tree extends it,
  i.e. that the log only ever appended. This is what makes rewriting history detectable.
- **Auditors.** `ledger audit` is an independent party with its own state: it verifies the
  signature, asks for a consistency proof against the last head it accepted, recomputes the root
  from the served entries and stores the new head. The log operator can sign anything it wants;
  it cannot make a rewritten log consistent with a head an auditor already holds. More auditors
  holding heads means more places a rewrite has to fool at once.

The Merkle code is checked against the RFC 6962 reference vectors and, exhaustively, for every
leaf and every pair of sizes up to 70.

### Measured run

```
1. replay from Kafka      attested bronze.object ocds/…/seace_v3-2026-09.json.zip   STH size 1
2. Gold load token=281    attested gold.load token:281 digest 70ac848f… rows 7255     STH size 2
3. first audit            signature valid · entries hash to the signed root · passed  (head size 2 stored)
4. load token=287         STH size 3
   second audit           append-only since last audit (2 → 3) · passed
                          inclusion proved: bronze.object …seace_v3-2026-09.json.zip
                          digest 9799538a… index 0, tree size 3, proof of 2 hashes
5. operator edits the digest of entry 1 in the log file and restarts the ledger
                          new STH size 3, root 07379649…, signature VALID
   third audit            AUDIT FAILED consistency: tree of size 3 (root 45f60777…) is not a
                          prefix of the tree of size 3 (root 07379649…): history was rewritten
6. honest file restored   root 45f60777… again · audit passed
```

The digest proved in step 4, `9799538a…`, is the SHA-256 the portal itself published for the
September 2026 file and that the extractor verified before storing it: the chain of evidence runs
from the government's checksum to a signed, audited entry. Step 5 is the point of the whole
construction: the forged head carried a perfectly valid signature, because the operator holds the
key. Only an auditor who remembered an earlier head could tell, and it did.
