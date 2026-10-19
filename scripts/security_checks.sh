#!/usr/bin/env bash
# Exercises every security boundary of the stack and prints one line per check.
# Expects the core, gold, serve and secure profiles to be running.
set -uo pipefail
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1
PKI="$(pwd -W 2>/dev/null || pwd)/deploy/pki"
NET=ocds_default

check() { # name expected actual
  if [ "$2" = "$3" ]; then echo "  PASS  $1 (got $3)"; else echo "  FAIL  $1 (expected $2, got $3)"; fi
}
curl_in_net() { docker run --rm --network "$NET" -v "$PKI:/pki:ro" curlimages/curl:8.10.1 "$@"; }

echo "== catalog: mutual TLS between nodes and clients"
out=$(docker compose exec -T catalog1 /catalog ctl -addr catalog2:7100 status 2>&1)
check "node certificate accepted" ok "$(echo "$out" | grep -q '"self"' && echo ok || echo rejected)"
alive=$(echo "$out" | grep -c '"state": "alive"')
check "gossip over mTLS keeps 5 members alive" 5 "$alive"
out=$(docker compose exec -T catalog1 /catalog ctl -addr catalog2:7100 -cert "" status 2>&1)
check "plaintext client rejected" rejected "$(echo "$out" | grep -q '"self"' && echo ok || echo rejected)"
out=$(docker compose exec -T catalog1 /catalog ctl -addr catalog2:7100 -cert /pki/intruder.crt -key /pki/intruder.key status 2>&1)
check "certificate from another CA rejected" rejected "$(echo "$out" | grep -q '"self"' && echo ok || echo rejected)"
echo "    reason: $(echo "$out" | grep -oE "rpc error: code = [A-Za-z]+ desc = [^\"]{0,120}" | head -1)"

echo "== extractor -> catalog with its own identity"
out=$(docker compose run --rm -T --entrypoint python extractor -c "
from extractor.catalog import Catalog, mtls_credentials
c = Catalog(['catalog3:7100'], credentials=mtls_credentials('/pki/ca.crt', '/pki/extractor.crt', '/pki/extractor.key'))
ctx = c.put('security/check', {'pages': 1})
e = c.get('security/check')
print('ok' if ctx and e and e.value['pages'] == 1 else 'failed')" 2>/dev/null | tail -1)
check "extractor writes and reads through mTLS" ok "$out"

echo "== direct access to internal services, bypassing Envoy"
code=$(curl_in_net -s -o /dev/null -w '%{http_code}' --cacert /pki/ca.crt https://api:8000/healthz 2>/dev/null; true)
check "API without client certificate" 000 "${code:-000}"
code=$(curl_in_net -s -o /dev/null -w '%{http_code}' --cacert /pki/ca.crt --cert /pki/envoy.crt --key /pki/envoy.key https://api:8000/healthz)
check "API with Envoy's certificate (health)" 200 "$code"
code=$(curl_in_net -s -o /dev/null -w '%{http_code}' --cacert /pki/ca.crt --cert /pki/envoy.crt --key /pki/envoy.key https://api:8000/v1/top-buyers)
check "API with a certificate but no verified identity" 401 "$code"
code=$(curl_in_net -s -o /dev/null -w '%{http_code}' http://api:8000/healthz 2>/dev/null; true)
check "API over plaintext HTTP" 400 "$code"
code=$(curl_in_net -s -o /dev/null -w '%{http_code}' --http2-prior-knowledge --cacert /pki/ca.crt https://ml:9000/ 2>/dev/null; true)
check "ML without client certificate" 000 "${code:-000}"

echo "== edge: authentication at Envoy, authorization in the API"
READER=$(bash scripts/mint_token.sh reader 600 ana@ocds)
ANALYST=$(bash scripts/mint_token.sh analyst 600 luis@ocds)
EXPIRED=$(bash scripts/mint_token.sh analyst -60 old@ocds)
# A reader token whose payload was edited to claim the analyst role, original signature kept.
FORGED=$(python -X utf8 -I -c "
import base64,json,sys
h,p,s='$READER'.split('.')
c=json.loads(base64.urlsafe_b64decode(p+'='*(-len(p)%4))); c['roles']=['analyst']
print(h+'.'+base64.urlsafe_b64encode(json.dumps(c,separators=(',',':')).encode()).rstrip(b'=').decode()+'.'+s)")
ANALYST_PAYLOAD=$(echo "$ANALYST" | cut -d. -f2)
PRED='{"monto_referencial":250000,"metodo":"open","categoria":"goods","departamento":"LIMA","es_essalud":true}'
api() { curl -s -o .body -w '%{http_code}' "$@"; }

check "no token" 401 "$(api localhost:8090/v1/top-buyers)"
check "reader reads" 200 "$(api -H "Authorization: Bearer $READER" localhost:8090/v1/top-buyers)"
check "reader predicts" 403 "$(api -X POST -H "Authorization: Bearer $READER" -d "$PRED" localhost:8090/v1/predict/lead-time)"
check "analyst predicts (API -> ML over mTLS)" 200 "$(api -X POST -H "Authorization: Bearer $ANALYST" -d "$PRED" localhost:8090/v1/predict/lead-time)"
echo "    prediction: $(cat .body)"
check "expired token" 401 "$(api -H "Authorization: Bearer $EXPIRED" localhost:8090/v1/top-buyers)"
echo "    envoy says: $(cat .body)"
check "payload edited to claim analyst" 401 "$(api -X POST -H "Authorization: Bearer $FORGED" -d "$PRED" localhost:8090/v1/predict/lead-time)"
echo "    envoy says: $(cat .body)"
check "reader smuggling an analyst identity header" 403 "$(api -X POST -H "Authorization: Bearer $READER" -H "x-jwt-payload: $ANALYST_PAYLOAD" -d "$PRED" localhost:8090/v1/predict/lead-time)"
check "health needs no token" 200 "$(api localhost:8090/healthz)"

rm -f .body
