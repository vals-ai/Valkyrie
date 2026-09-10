"""Restricted ALB access-log storage and query resources for Tracker."""

from typing import cast

import aws_cdk as cdk
from aws_cdk import aws_athena, aws_elasticloadbalancingv2, aws_glue, aws_iam, aws_s3
from constructs import Construct

from stage import Stage

ACCESS_LOG_PREFIX = "tracker-alb"
PRODUCTION_ACCESS_LOG_RETENTION_DAYS = 365
DEV_ACCESS_LOG_RETENTION_DAYS = 7
ATHENA_DATABASE_NAME = "tracker_alb_access_logs"
ATHENA_TABLE_NAME = "requests"
ATHENA_WORKGROUP_NAME = "tracker-alb-access-logs"

_ALB_LOG_COLUMNS = (
    ("type", "string"),
    ("time", "string"),
    ("elb", "string"),
    ("client_ip", "string"),
    ("client_port", "int"),
    ("target_ip", "string"),
    ("target_port", "int"),
    ("request_processing_time", "double"),
    ("target_processing_time", "double"),
    ("response_processing_time", "double"),
    ("elb_status_code", "int"),
    ("target_status_code", "string"),
    ("received_bytes", "bigint"),
    ("sent_bytes", "bigint"),
    ("request_verb", "string"),
    ("request_url", "string"),
    ("request_proto", "string"),
    ("user_agent", "string"),
    ("ssl_cipher", "string"),
    ("ssl_protocol", "string"),
    ("target_group_arn", "string"),
    ("trace_id", "string"),
    ("domain_name", "string"),
    ("chosen_cert_arn", "string"),
    ("matched_rule_priority", "string"),
    ("request_creation_time", "string"),
    ("actions_executed", "string"),
    ("redirect_url", "string"),
    ("lambda_error_reason", "string"),
    ("target_port_list", "string"),
    ("target_status_code_list", "string"),
    ("classification", "string"),
    ("classification_reason", "string"),
    ("conn_trace_id", "string"),
)

_ALB_LOG_INPUT_REGEX = (
    r"([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*):([0-9]*) ([^ ]*)[:-]([0-9]*) "
    r"([-.0-9]*) ([-.0-9]*) ([-.0-9]*) (|[-0-9]*) (-|[-0-9]*) ([-0-9]*) ([-0-9]*) "
    r'"([^ ]*) (.*) (- |[^ ]*)" "([^"]*)" ([A-Z0-9-_]+) ([A-Za-z0-9.-]*) ([^ ]*) '
    r'"([^"]*)" "([^"]*)" "([^"]*)" ([-.0-9]*) ([^ ]*) "([^"]*)" "([^"]*)" '
    r'"([^ ]*)" "([^\s]+?)" "([^\s]+)" "([^ ]*)" "([^ ]*)" ?([^ ]*)? ?( .*)?'
)


def create_tracker_access_logs(
    scope: Construct,
    *,
    stage: Stage,
    load_balancer: aws_elasticloadbalancingv2.ApplicationLoadBalancer,
) -> None:
    """Create deployed Tracker ALB access-log storage and its Athena catalog."""
    if stage.is_release_test:
        return

    stack = cdk.Stack.of(scope)
    retention_days = PRODUCTION_ACCESS_LOG_RETENTION_DAYS if stage.is_production else DEV_ACCESS_LOG_RETENTION_DAYS
    bucket = aws_s3.Bucket(
        scope,
        "TrackerAlbAccessLogBucket",
        block_public_access=aws_s3.BlockPublicAccess.BLOCK_ALL,
        encryption=aws_s3.BucketEncryption.S3_MANAGED,
        enforce_ssl=True,
        object_ownership=aws_s3.ObjectOwnership.BUCKET_OWNER_ENFORCED,
        lifecycle_rules=[
            aws_s3.LifecycleRule(
                expiration=cdk.Duration.days(retention_days),
            )
        ],
        removal_policy=cdk.RemovalPolicy.RETAIN,
    )
    bucket.add_to_resource_policy(
        aws_iam.PolicyStatement(
            sid="AllowTrackerAlbLogDelivery",
            principals=[
                cast(
                    aws_iam.IPrincipal,
                    aws_iam.ServicePrincipal("logdelivery.elasticloadbalancing.amazonaws.com"),
                )
            ],
            actions=["s3:PutObject"],
            resources=[bucket.arn_for_objects(f"{ACCESS_LOG_PREFIX}/AWSLogs/{stack.account}/*")],
            conditions={
                "StringEquals": {"aws:SourceAccount": stack.account},
                "ArnLike": {
                    "aws:SourceArn": (
                        f"arn:{stack.partition}:elasticloadbalancing:{stack.region}:{stack.account}:loadbalancer/*"
                    )
                },
            },
        )
    )

    load_balancer.set_attribute("access_logs.s3.enabled", "true")
    load_balancer.set_attribute("access_logs.s3.bucket", bucket.bucket_name)
    load_balancer.set_attribute("access_logs.s3.prefix", ACCESS_LOG_PREFIX)
    bucket_policy = bucket.policy
    cfn_load_balancer = load_balancer.node.default_child
    cfn_bucket_policy = bucket_policy.node.default_child if bucket_policy is not None else None
    if not isinstance(cfn_load_balancer, cdk.CfnResource) or not isinstance(cfn_bucket_policy, cdk.CfnResource):
        raise RuntimeError("Tracker ALB access logging requires synthesized load balancer and bucket policy resources")
    cfn_load_balancer.add_dependency(cfn_bucket_policy)

    database = aws_glue.CfnDatabase(
        scope,
        "TrackerAlbAccessLogDatabase",
        catalog_id=stack.account,
        database_input=aws_glue.CfnDatabase.DatabaseInputProperty(
            name=ATHENA_DATABASE_NAME,
            description="Restricted Tracker Application Load Balancer access logs",
        ),
    )
    log_root = (
        f"s3://{bucket.bucket_name}/{ACCESS_LOG_PREFIX}/AWSLogs/{stack.account}/elasticloadbalancing/{stack.region}/"
    )
    table = aws_glue.CfnTable(
        scope,
        "TrackerAlbAccessLogTable",
        catalog_id=stack.account,
        database_name=ATHENA_DATABASE_NAME,
        table_input=aws_glue.CfnTable.TableInputProperty(
            name=ATHENA_TABLE_NAME,
            description="Tracker ALB request evidence with projected UTC day partitions",
            table_type="EXTERNAL_TABLE",
            parameters={
                "EXTERNAL": "TRUE",
                "projection.enabled": "true",
                "projection.day.type": "date",
                "projection.day.range": "2026/09/01,NOW",
                "projection.day.format": "yyyy/MM/dd",
                "projection.day.interval": "1",
                "projection.day.interval.unit": "DAYS",
                "storage.location.template": f"{log_root}${{day}}/",
            },
            partition_keys=[aws_glue.CfnTable.ColumnProperty(name="day", type="string")],
            storage_descriptor=aws_glue.CfnTable.StorageDescriptorProperty(
                columns=[
                    aws_glue.CfnTable.ColumnProperty(name=name, type=column_type)
                    for name, column_type in _ALB_LOG_COLUMNS
                ],
                compressed=True,
                input_format="org.apache.hadoop.mapred.TextInputFormat",
                output_format="org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                location=log_root,
                serde_info=aws_glue.CfnTable.SerdeInfoProperty(
                    serialization_library="org.apache.hadoop.hive.serde2.RegexSerDe",
                    parameters={
                        "serialization.format": "1",
                        "input.regex": _ALB_LOG_INPUT_REGEX,
                    },
                ),
            ),
        ),
    )
    table.add_dependency(database)

    workgroup = aws_athena.CfnWorkGroup(
        scope,
        "TrackerAlbAccessLogWorkGroup",
        name=ATHENA_WORKGROUP_NAME,
        description="Restricted Tracker ALB incident queries",
        work_group_configuration=aws_athena.CfnWorkGroup.WorkGroupConfigurationProperty(
            enforce_work_group_configuration=True,
            publish_cloud_watch_metrics_enabled=False,
            result_configuration=aws_athena.CfnWorkGroup.ResultConfigurationProperty(
                encryption_configuration=aws_athena.CfnWorkGroup.EncryptionConfigurationProperty(
                    encryption_option="SSE_S3",
                ),
                expected_bucket_owner=stack.account,
                output_location=f"s3://{bucket.bucket_name}/athena-results/",
            ),
        ),
    )

    query = aws_athena.CfnNamedQuery(
        scope,
        "TrackerAlbRequestWindowQuery",
        database=ATHENA_DATABASE_NAME,
        name="tracker-alb-request-window",
        description="Tracker ALB requests in an editable UTC time window",
        work_group=workgroup.ref,
        query_string=f"""WITH bounds AS (
    SELECT
        current_timestamp - interval '1' hour AS start_utc,
        current_timestamp AS end_utc
)
SELECT
    from_iso8601_timestamp(time) AS request_time_utc,
    client_ip,
    request_verb,
    request_url,
    user_agent,
    elb_status_code,
    target_status_code,
    concat(target_ip, ':', cast(target_port AS varchar)) AS target_address,
    request_processing_time,
    target_processing_time,
    response_processing_time,
    trace_id
FROM {ATHENA_TABLE_NAME}
CROSS JOIN bounds
WHERE day BETWEEN date_format(start_utc, '%Y/%m/%d') AND date_format(end_utc + interval '1' day, '%Y/%m/%d')
  AND from_iso8601_timestamp(time) BETWEEN start_utc AND end_utc
ORDER BY request_time_utc
""",
    )
    query.add_dependency(table)

    cdk.CfnOutput(scope, "TrackerAlbAccessLogBucketName", value=bucket.bucket_name)
    cdk.CfnOutput(scope, "TrackerAlbAccessLogDatabaseName", value=ATHENA_DATABASE_NAME)
    cdk.CfnOutput(scope, "TrackerAlbAccessLogWorkGroupName", value=ATHENA_WORKGROUP_NAME)
