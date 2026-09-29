# ValSmith outbound proxy

This image is the proxy component of the ValSmith network restrictions. It does
not change AWS routes, security groups, services or Lambda configuration. The
callers still need AWS endpoints and security groups that prevent direct
Internet access. Deploy this image only behind the internal load balancer with
separate caller rules for each listener.

| Listener | Callers | Allowed destinations |
| --- | --- | --- |
| 3128 | Generation and evaluation services | Exact names in `service-hosts.txt` |
| 3129 | Dataset-view Lambda | Exact name in `view-hosts.txt` |

The bucket-policy Lambda must not have access to either listener.

Squid accepts CONNECT to port 443 only. It rejects private destination addresses
and inspects the TLS ClientHello before forwarding encrypted bytes. The external
ACL requires actual SNI to match the original CONNECT authority. Missing SNI,
IP authorities, wildcard subdomains, other ports and other listener destinations
are denied. The helper receives Squid's two fields plus its empty `%DATA` marker.
A failed helper does not open a tunnel. AWS security groups must also prevent
callers from bypassing the proxy.

The certificate in the image only initializes Squid's TLS inspection context.
It is never installed in callers, and no `bump` action is configured. Allowed
connections retain the origin certificate and encrypted application traffic.
Destination decisions are in `ssl_bump`: a CONNECT denial in `http_access` makes
Squid generate a TLS error page instead of closing the connection. The runtime
tests check that denied TLS handshakes close and allowed certificates pass
through unchanged. A CONNECT 200 response alone is not an access decision.

Caching is disabled. Access logs contain time, listener port, approved destination
(or `-` for an unknown name), decision, byte count and error category. Diagnostic
logs are disabled because they can contain raw requests. Do not enable debug
logging on customer traffic. Monitor container health and access decisions.

This controls destination hosts, not encrypted HTTP paths or behavior within an
approved remote service. It does not prevent DNS exfiltration or control remote
Daytona sandbox traffic.

## Qualification

From the repository root:

```sh
docker build -t valsmith-outbound-proxy:test infra/outbound_proxy
VALSMITH_PROXY_TEST_IMAGE=valsmith-outbound-proxy:test \
  PYTHONPATH=infra uv run python -m unittest \
  infra/tests/test_outbound_proxy_acl.py \
  infra/tests/test_outbound_proxy_runtime.py -v
```

Docker tests create and remove their own containers and networks. The TLS origin
uses a public-format address inside an internal Docker network; no request goes
to that address on the Internet. A second network permits the host test client
to reach the proxy's loopback-published ports. All tested destination names map
to the controlled origin, including denied names. No customer token is needed.
The fixture certificate is trusted only by the test client.

Tests exercise all allowed destinations, listener separation, TLS name matching,
missing SNI, private DNS answers, metadata addresses, helper suspension and
recovery, WebSocket traffic, redirects and secret markers in request fields.
Infrastructure CI runs these Docker tests on every infrastructure change. Ordinary
infrastructure tests skip the Docker cases unless the image variable is set.

The Ubuntu base manifest and Squid package version are pinned. Rebuilds install
current Ubuntu updates for supporting packages. Qualify each built image and
reference its final ECR digest during deployment; a source tag is not a release
identity. Keep the previous image digest for rollback.
