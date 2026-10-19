import json
import os
import time
import urllib.error
import urllib.request

TOKEN = os.environ["GARAGE_ADMIN_TOKEN"]
NODES = os.environ["GARAGE_NODES"].split(",")
ACCESS_KEY = os.environ["S3_ACCESS_KEY"]
SECRET_KEY = os.environ["S3_SECRET_KEY"]
BUCKETS = [b.strip() for b in os.environ.get("S3_BUCKETS", os.environ.get("S3_BUCKET", "bronze")).split(",") if b.strip()]
ZONE = os.environ.get("GARAGE_ZONE", "dc1")
CAPACITY = int(os.environ.get("GARAGE_NODE_CAPACITY", str(10 * 1024**3)))


def call(host, method, path, body=None):
    req = urllib.request.Request(
        f"http://{host}:3903/v2/{path}",
        method=method,
        data=None if body is None else json.dumps(body).encode(),
        headers={"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        raw = resp.read()
        return json.loads(raw) if raw else None


def wait_ready(host):
    for _ in range(60):
        try:
            return call(host, "GET", "GetNodeInfo?node=self")
        except (urllib.error.URLError, ConnectionError, TimeoutError):
            time.sleep(1)
    raise SystemExit(f"{host} did not become ready")


def node_id(host):
    info = wait_ready(host)["success"]
    return next(iter(info))


def main():
    seed = NODES[0]
    ids = {host: node_id(host) for host in NODES}
    print("nodes:", ids)

    call(seed, "POST", "ConnectClusterNodes", [f"{nid}@{host}:3901" for host, nid in ids.items() if host != seed])

    layout = call(seed, "GET", "GetClusterLayout")
    assigned = {role["id"] for role in layout.get("roles", [])}
    missing = [nid for nid in ids.values() if nid not in assigned]
    if missing:
        call(seed, "POST", "UpdateClusterLayout", {
            "roles": [{"id": nid, "zone": ZONE, "capacity": CAPACITY, "tags": []} for nid in missing],
        })
        call(seed, "POST", "ApplyClusterLayout", {"version": layout["version"] + 1})
        print("layout applied for", missing)

    try:
        call(seed, "GET", f"GetKeyInfo?id={ACCESS_KEY}")
    except urllib.error.HTTPError:
        call(seed, "POST", "ImportKey", {"accessKeyId": ACCESS_KEY, "secretAccessKey": SECRET_KEY, "name": "ocds"})
        print("key imported")

    for name in BUCKETS:
        try:
            bucket = call(seed, "GET", f"GetBucketInfo?globalAlias={name}")
        except urllib.error.HTTPError:
            bucket = call(seed, "POST", "CreateBucket", {"globalAlias": name})
            print("bucket created:", name)
        call(seed, "POST", "AllowBucketKey", {
            "bucketId": bucket["id"],
            "accessKeyId": ACCESS_KEY,
            "permissions": {"read": True, "write": True, "owner": True},
        })
    print("garage ready")


if __name__ == "__main__":
    main()
