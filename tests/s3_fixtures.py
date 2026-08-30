#!/usr/bin/env python3
"""Creates the test bucket and uploads a fixture table's warehouse into it.

Deliberately does not use the code under test: a warehouse uploaded with our
own S3 client would make a bug in that client show up as a mysterious read
failure. This is a self-contained SigV4 signer over stdlib only.

    python tests/s3_fixtures.py <endpoint> <bucket> <key> <secret> \
        <fixtures-dir> <table> [<table> ...]

Objects land at `<bucket>/db/<table>/{metadata,data}/...`, mirroring the
warehouse layout the fixture metadata was written against, so the reader only
needs one prefix rewrite to find them.
"""
import datetime
import hashlib
import hmac
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

REGION = "us-east-1"


def encode_path(path):
    """SigV4 signs the URI-encoded path, and S3 keys really do contain `=`
    (Hive-style partition directories). Encoding it in only one of the two
    places produces a SignatureDoesNotMatch that looks like a credential
    problem."""
    return urllib.parse.quote(path, safe="/")


def sign(method, endpoint, path, body, key, secret):
    host = endpoint.split("://", 1)[1]
    now = datetime.datetime.now(datetime.timezone.utc)
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    stamp = now.strftime("%Y%m%d")
    payload_hash = hashlib.sha256(body).hexdigest()
    canonical = "\n".join([
        method, encode_path(path), "",
        "host:%s" % host,
        "x-amz-content-sha256:%s" % payload_hash,
        "x-amz-date:%s" % amz_date,
        "", "host;x-amz-content-sha256;x-amz-date", payload_hash,
    ])
    scope = "%s/%s/s3/aws4_request" % (stamp, REGION)
    to_sign = "\n".join([
        "AWS4-HMAC-SHA256", amz_date, scope,
        hashlib.sha256(canonical.encode()).hexdigest(),
    ])
    signing = ("AWS4" + secret).encode()
    for part in (stamp, REGION, "s3", "aws4_request"):
        signing = hmac.new(signing, part.encode(), hashlib.sha256).digest()
    signature = hmac.new(signing, to_sign.encode(), hashlib.sha256).hexdigest()
    return {
        "Host": host,
        "x-amz-date": amz_date,
        "x-amz-content-sha256": payload_hash,
        "Authorization": (
            "AWS4-HMAC-SHA256 Credential=%s/%s, "
            "SignedHeaders=host;x-amz-content-sha256;x-amz-date, "
            "Signature=%s" % (key, scope, signature)
        ),
    }


class PutFailed(Exception):
    def __init__(self, code, detail):
        super().__init__("%d %s" % (code, detail[:400]))
        self.code = code
        self.detail = detail


def put(endpoint, path, body, key, secret, attempts=8):
    """One signed PUT, retried while the server is still coming up.

    A MinIO that answers `/minio/health/live` can still reject writes for a
    second or two while its object layer initialises, and it does so with a
    403, not a 503 — so the retry has to cover that status too.
    """
    last = None
    for attempt in range(attempts):
        headers = sign("PUT", endpoint, path, body, key, secret)
        req = urllib.request.Request(
            endpoint + encode_path(path), method="PUT", data=body,
            headers=headers)
        try:
            urllib.request.urlopen(req).read()
            return
        except urllib.error.HTTPError as e:
            detail = e.read().decode("utf-8", "replace")
            last = PutFailed(e.code, detail)
            if e.code in (403, 500, 503) and "AlreadyOwnedByYou" not in detail:
                time.sleep(0.5 * (attempt + 1))
                continue
            raise last from None
        except urllib.error.URLError:
            time.sleep(0.5 * (attempt + 1))
            last = PutFailed(0, "connection refused")
    raise last


def main():
    endpoint, bucket, key, secret, fixtures = sys.argv[1:6]
    tables = sys.argv[6:]

    try:
        put(endpoint, "/" + bucket, b"", key, secret)
        print("created bucket", bucket)
    except PutFailed as e:
        if "BucketAlreadyOwnedByYou" not in e.detail and e.code != 409:
            print("bucket creation failed:", e.code, e.detail[:300])
            raise SystemExit(1)
        print("bucket", bucket, "already exists")

    n = 0
    total = 0
    for table in tables:
        root = os.path.join(fixtures, table)
        for sub in ("metadata", "data"):
            d = os.path.join(root, sub)
            if not os.path.isdir(d):
                continue
            for dirpath, _, names in os.walk(d):
                for name in sorted(names):
                    local = os.path.join(dirpath, name)
                    rel = os.path.relpath(local, root)
                    with open(local, "rb") as f:
                        blob = f.read()
                    try:
                        put(endpoint,
                            "/%s/db/%s/%s"
                            % (bucket, table, rel.replace(os.sep, "/")),
                            blob, key, secret)
                    except PutFailed as e:
                        print("upload failed for %s: %s" % (rel, e))
                        raise SystemExit(1)
                    n += 1
                    total += len(blob)
    print("uploaded %d objects (%d bytes) for %s" % (n, total, ", ".join(tables)))


if __name__ == "__main__":
    main()
