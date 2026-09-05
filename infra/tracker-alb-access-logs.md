# Tracker ALB access logs

The dev, bench, and external production Tracker load balancers each write restricted request evidence to their own S3 bucket. Each bucket uses SSE-S3, blocks public access, and requires TLS. Dev objects expire after 7 days; bench and external production objects expire after 365 days. The bucket policy permits only the Elastic Load Balancing delivery service for load balancers in the same account and Region. The logs are not copied to CloudWatch Logs.

ALB access logs include client or upstream-proxy IP addresses, full request URIs, user agents, status codes, target addresses, latency fields, and `X-Amzn-Trace-Id`. Treat the bucket and Athena results as restricted operational data. Access comes from the responder's existing AWS IAM permissions; this stack grants no new read access.

AWS delivers access logs approximately every five minutes with eventual consistency. Delivery is best effort and can include duplicate coverage, so these logs support incident diagnosis rather than complete request accounting.

## Find the log resources

1. Open the Tracker stack outputs.

   **Why** -- Each deployed account has its own generated bucket name. The stack output identifies the bucket without relying on a copied value.

   In the AWS console, open **CloudFormation → Stacks**, choose `ValkDevTrackerStack` for dev, `TrackerStack` for bench, or `ValkProdTrackerStack` for external production, then open **Outputs**. Find `TrackerAlbAccessLogBucketName`, `TrackerAlbAccessLogDatabaseName`, and `TrackerAlbAccessLogWorkGroupName`.

   CLI alternative:

   ```bash
   aws cloudformation describe-stacks \
     --stack-name TrackerStack \
     --query 'Stacks[0].Outputs[?starts_with(OutputKey, `TrackerAlbAccessLog`)].[OutputKey,OutputValue]' \
     --output table
   ```

   Use `ValkDevTrackerStack` for dev or `ValkProdTrackerStack` for external production, and select the profile for the intended account.

   **Done when** -- The outputs show a dedicated access-log bucket, the `tracker_alb_access_logs` database, and the `tracker-alb-access-logs` workgroup in the expected account and Region.

2. Confirm log delivery when investigating a recent event.

   **Why** -- Enabling access logs causes the load balancer to write a permission test object immediately, but request logs arrive asynchronously.

   In the S3 console, open the output bucket and browse to `tracker-alb/AWSLogs/<account-id>/`. Confirm `ELBAccessLogTestFile` exists, then browse through `elasticloadbalancing/<region>/<yyyy>/<mm>/<dd>/` for `.log.gz` objects covering the UTC incident window.

   CLI alternative:

   ```bash
   aws s3api list-objects-v2 \
     --bucket <TrackerAlbAccessLogBucketName> \
     --prefix tracker-alb/AWSLogs/<account-id>/ \
     --query 'Contents[-10:].[LastModified,Key]' \
     --output table
   ```

   **Done when** -- The test object exists and at least one compressed access-log object has arrived for a request made after deployment.

## Query a UTC incident window

1. Open the saved Athena query.

   **Why** -- The provisioned Glue table follows AWS's ALB log schema and projects date partitions automatically. Responders do not need a crawler, `ALTER TABLE`, or local parsing.

   In the AWS console, open **Athena → Query editor**, select the `tracker-alb-access-logs` workgroup and `tracker_alb_access_logs` database, then choose **Saved queries → tracker-alb-request-window**. The workgroup enforces SSE-S3 query results under `athena-results/` in the same restricted bucket; responders cannot redirect results to another location.

   CLI alternative: use `aws athena get-named-query` to retrieve the saved SQL and `aws athena start-query-execution --work-group tracker-alb-access-logs` after setting the intended UTC bounds. The enforced workgroup supplies the result location and encryption.

   **Done when** -- The saved query opens against the `requests` table in the enforced workgroup and its `bounds` CTE shows a one-hour UTC window ending now.

2. Set and run the incident window.

   **Why** -- Filtering both the projected `day` partition and the parsed event timestamp limits scanned data while preserving exact UTC boundaries. The query includes the next delivery-date partition because a request completed just before midnight can be delivered in a log object dated just after midnight.

   For a fixed window, replace the two expressions in the `bounds` CTE with ISO-8601 UTC timestamps:

   ```sql
   WITH bounds AS (
       SELECT
           from_iso8601_timestamp('2026-09-03T08:45:00Z') AS start_utc,
           from_iso8601_timestamp('2026-09-03T09:15:00Z') AS end_utc
   )
   ```

   Run the full saved query. It returns request time, client or proxy IP, method and URI, user agent, ALB and target status, target address, the three latency segments, and the ALB trace ID.

   **Done when** -- Results cover the intended UTC window, or the empty result is interpreted alongside the approximately five-minute delivery delay and best-effort limitation.

3. Correlate a known request with Tracker logs.

   **Why** -- The ALB entry shows the network path and supplies the durable trace ID; Tracker's existing access record confirms that the request reached the application without requiring a Tracker rollout.

   Send a request to a unique nonexistent path and record the UTC time. A `404` response is expected and does not mutate Tracker state:

   ```bash
   curl -i "https://<tracker-host>/alb-log-correlation-<UTC-timestamp>"
   ```

   Find that path in the Athena result. In **CloudWatch → Log groups**, open the Tracker log group for the same stage and match the existing access record by its tight UTC window, method, unique path, and `404` status. Record the ALB row's `trace_id` with the incident evidence. Do not attempt to match IP addresses: the ALB `target_address` is the Tracker task, while Tracker sees an ALB node as the connecting client.

   CLI alternative:

   ```bash
   aws logs filter-log-events \
     --log-group-name /valkyrie/tracker \
     --filter-pattern '"<METHOD> <PATH>"' \
     --start-time <window-start-epoch-ms> \
     --end-time <window-end-epoch-ms>
   ```

   Use the stage-qualified Tracker log group name for dev or external production.

   **Done when** -- One ALB row and one Tracker access record match on UTC time, method, unique path, and status, and the ALB trace ID is recorded with the evidence.

## Request-URI safety boundary

ALB logging preserves the request URI, including its query string. Tracker authentication and secret material use headers or request bodies. The query parameters audited when this logging was introduced are identifiers, filters, pagination, ordering, connection and retry controls, task selections, model and dataset selectors, labels, and UTC date bounds. No Tracker route accepts credentials, tokens, API keys, secret values, or secret names in its query string.

Do not introduce a query parameter carrying credential or secret material. Use an authorization header, `X-Api-Key`, or a validated request-body field appropriate to the existing endpoint contract.
