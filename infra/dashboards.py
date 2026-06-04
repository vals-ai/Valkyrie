"""Factory functions for Valkyrie CloudWatch dashboards.

Each function takes a scope (the MonitoringStack) plus the resource refs
it needs and returns a configured ``aws_cloudwatch.Dashboard``.
"""

from __future__ import annotations

import aws_cdk as cdk
from aws_cdk import (
    aws_cloudwatch,
    aws_ecs,
    aws_elasticache,
    aws_elasticloadbalancingv2 as aws_elb,
    aws_rds,
)
from constructs import Construct
from stage import Stage

# Default widget height for dashboard layouts. CloudWatch grids are 24 units wide.
_WIDGET_HEIGHT = 6
_SINGLE_VALUE_HEIGHT = 3


def create_overview_dashboard(
    scope: Construct,
    *,
    stage: Stage,
    tracker_service: aws_ecs.FargateService,
    worker_service: aws_ecs.FargateService,
    load_balancer: aws_elb.ApplicationLoadBalancer,
    target_group: aws_elb.ApplicationTargetGroup,
    database: aws_rds.DatabaseInstance,
    redis_cluster: aws_elasticache.CfnCacheCluster,
) -> aws_cloudwatch.Dashboard:
    """`Valkyrie-Overview` - Single-value widgets + sparklines."""
    dashboard = aws_cloudwatch.Dashboard(
        scope,
        "ValkyrieOverviewDashboard",
        dashboard_name=stage.phys("Valkyrie-Overview"),
        default_interval=cdk.Duration.hours(3),
        period_override=aws_cloudwatch.PeriodOverride.AUTO,
    )

    dashboard.add_widgets(
        aws_cloudwatch.TextWidget(
            markdown=(
                "# Valkyrie Overview\n"
                "Infra health at a glance. Red/green widgets show current alarm state.\n"
                "For drill-downs: **Valkyrie-ECS**, **Valkyrie-ALB**, "
                "**Valkyrie-RDS**, **Valkyrie-Redis**."
            ),
            width=24,
            height=2,
        ),
    )

    # Row 1: availability signals (single-value)
    dashboard.add_widgets(
        aws_cloudwatch.SingleValueWidget(
            title="Tracker Healthy Hosts",
            metrics=[
                target_group.metric_healthy_host_count(
                    period=cdk.Duration.minutes(1),
                    statistic="Minimum",
                )
            ],
            width=6,
            height=_SINGLE_VALUE_HEIGHT,
            sparkline=True,
        ),
        aws_cloudwatch.SingleValueWidget(
            title="Worker Running Tasks",
            metrics=[
                aws_cloudwatch.Metric(
                    namespace="ECS/ContainerInsights",
                    metric_name="RunningTaskCount",
                    dimensions_map={
                        "ClusterName": worker_service.cluster.cluster_name,
                        "ServiceName": worker_service.service_name,
                    },
                    statistic="Maximum",
                    period=cdk.Duration.minutes(1),
                )
            ],
            width=6,
            height=_SINGLE_VALUE_HEIGHT,
            sparkline=True,
        ),
        aws_cloudwatch.SingleValueWidget(
            title="API p99 Latency (s)",
            metrics=[
                load_balancer.metric_target_response_time(
                    period=cdk.Duration.minutes(1),
                    statistic="p99",
                )
            ],
            width=6,
            height=_SINGLE_VALUE_HEIGHT,
            sparkline=True,
        ),
        aws_cloudwatch.SingleValueWidget(
            title="API 5XX / 5min",
            metrics=[
                load_balancer.metric_http_code_target(
                    code=aws_elb.HttpCodeTarget.TARGET_5XX_COUNT,
                    period=cdk.Duration.minutes(5),
                    statistic="Sum",
                )
            ],
            width=6,
            height=_SINGLE_VALUE_HEIGHT,
            sparkline=True,
        ),
    )

    # Row 2: traffic + ECS saturation
    dashboard.add_widgets(
        aws_cloudwatch.GraphWidget(
            title="API Request Rate",
            left=[
                load_balancer.metric_request_count(
                    period=cdk.Duration.minutes(1),
                    statistic="Sum",
                )
            ],
            width=8,
            height=_WIDGET_HEIGHT,
        ),
        aws_cloudwatch.GraphWidget(
            title="Tracker CPU / Memory (%)",
            left=[
                tracker_service.metric_cpu_utilization(
                    period=cdk.Duration.minutes(1),
                    statistic="Average",
                )
            ],
            right=[
                tracker_service.metric_memory_utilization(
                    period=cdk.Duration.minutes(1),
                    statistic="Average",
                )
            ],
            width=8,
            height=_WIDGET_HEIGHT,
        ),
        aws_cloudwatch.GraphWidget(
            title="Worker CPU / Memory (%)",
            left=[
                worker_service.metric_cpu_utilization(
                    period=cdk.Duration.minutes(1),
                    statistic="Average",
                )
            ],
            right=[
                worker_service.metric_memory_utilization(
                    period=cdk.Duration.minutes(1),
                    statistic="Average",
                )
            ],
            width=8,
            height=_WIDGET_HEIGHT,
        ),
    )

    # Row 3: DB health
    dashboard.add_widgets(
        aws_cloudwatch.SingleValueWidget(
            title="DB Connections",
            metrics=[
                database.metric_database_connections(
                    period=cdk.Duration.minutes(1),
                    statistic="Maximum",
                )
            ],
            width=6,
            height=_SINGLE_VALUE_HEIGHT,
            sparkline=True,
        ),
        aws_cloudwatch.SingleValueWidget(
            title="DB CPU (%)",
            metrics=[
                database.metric_cpu_utilization(
                    period=cdk.Duration.minutes(1),
                    statistic="Average",
                )
            ],
            width=6,
            height=_SINGLE_VALUE_HEIGHT,
            sparkline=True,
        ),
        aws_cloudwatch.SingleValueWidget(
            title="DB Free Storage (bytes)",
            metrics=[
                database.metric_free_storage_space(
                    period=cdk.Duration.minutes(5),
                    statistic="Minimum",
                )
            ],
            width=6,
            height=_SINGLE_VALUE_HEIGHT,
            sparkline=True,
        ),
        aws_cloudwatch.SingleValueWidget(
            title="Redis Memory Usage (%)",
            metrics=[
                redis_metric(
                    redis_cluster,
                    "DatabaseMemoryUsagePercentage",
                    statistic="Average",
                )
            ],
            width=6,
            height=_SINGLE_VALUE_HEIGHT,
            sparkline=True,
        ),
    )

    # Row 4: Redis evictions (stand-alone so it's visually isolated - should be zero)
    dashboard.add_widgets(
        aws_cloudwatch.GraphWidget(
            title="Redis Evictions (should be 0)",
            left=[
                redis_metric(
                    redis_cluster,
                    "Evictions",
                    statistic="Sum",
                    period=cdk.Duration.minutes(5),
                )
            ],
            width=24,
            height=_WIDGET_HEIGHT,
        ),
    )

    return dashboard


def create_ecs_dashboard(
    scope: Construct,
    *,
    stage: Stage,
    tracker_service: aws_ecs.FargateService,
    worker_service: aws_ecs.FargateService,
) -> aws_cloudwatch.Dashboard:
    """`Valkyrie-ECS` -- detailed Tracker + Worker metrics."""
    dashboard = aws_cloudwatch.Dashboard(
        scope,
        "ValkyrieEcsDashboard",
        dashboard_name=stage.phys("Valkyrie-ECS"),
        default_interval=cdk.Duration.hours(24),
        period_override=aws_cloudwatch.PeriodOverride.AUTO,
    )

    for service, name in ((tracker_service, "Tracker"), (worker_service, "Worker")):
        dashboard.add_widgets(
            aws_cloudwatch.TextWidget(
                markdown=f"## {name} Service",
                width=24,
                height=1,
            ),
        )
        dashboard.add_widgets(
            aws_cloudwatch.GraphWidget(
                title=f"{name} CPU Utilization (%)",
                left=[
                    service.metric_cpu_utilization(
                        period=cdk.Duration.minutes(1),
                        statistic="Average",
                    )
                ],
                width=8,
                height=_WIDGET_HEIGHT,
            ),
            aws_cloudwatch.GraphWidget(
                title=f"{name} Memory Utilization (%)",
                left=[
                    service.metric_memory_utilization(
                        period=cdk.Duration.minutes(1),
                        statistic="Average",
                    )
                ],
                width=8,
                height=_WIDGET_HEIGHT,
            ),
            aws_cloudwatch.GraphWidget(
                title=f"{name} Running vs Desired Tasks",
                left=[
                    aws_cloudwatch.Metric(
                        namespace="ECS/ContainerInsights",
                        metric_name="RunningTaskCount",
                        dimensions_map={
                            "ClusterName": service.cluster.cluster_name,
                            "ServiceName": service.service_name,
                        },
                        statistic="Maximum",
                        period=cdk.Duration.minutes(1),
                        label="Running",
                    ),
                    aws_cloudwatch.Metric(
                        namespace="ECS/ContainerInsights",
                        metric_name="DesiredTaskCount",
                        dimensions_map={
                            "ClusterName": service.cluster.cluster_name,
                            "ServiceName": service.service_name,
                        },
                        statistic="Maximum",
                        period=cdk.Duration.minutes(1),
                        label="Desired",
                    ),
                ],
                width=8,
                height=_WIDGET_HEIGHT,
            ),
        )
        dashboard.add_widgets(
            aws_cloudwatch.GraphWidget(
                title=f"{name} Network In/Out (bytes)",
                left=[
                    ecs_container_insights_metric(
                        service,
                        "NetworkRxBytes",
                        statistic="Sum",
                    )
                ],
                right=[
                    ecs_container_insights_metric(
                        service,
                        "NetworkTxBytes",
                        statistic="Sum",
                    )
                ],
                width=12,
                height=_WIDGET_HEIGHT,
            ),
            aws_cloudwatch.GraphWidget(
                title=f"{name} Storage Read/Write (bytes)",
                left=[
                    ecs_container_insights_metric(
                        service,
                        "StorageReadBytes",
                        statistic="Sum",
                    )
                ],
                right=[
                    ecs_container_insights_metric(
                        service,
                        "StorageWriteBytes",
                        statistic="Sum",
                    )
                ],
                width=12,
                height=_WIDGET_HEIGHT,
            ),
        )

    return dashboard


def create_alb_dashboard(
    scope: Construct,
    *,
    stage: Stage,
    load_balancer: aws_elb.ApplicationLoadBalancer,
    target_group: aws_elb.ApplicationTargetGroup,
) -> aws_cloudwatch.Dashboard:
    """`Valkyrie-ALB` -- request flow, latencies, errors, connections."""
    dashboard = aws_cloudwatch.Dashboard(
        scope,
        "ValkyrieAlbDashboard",
        dashboard_name=stage.phys("Valkyrie-ALB"),
        default_interval=cdk.Duration.hours(24),
        period_override=aws_cloudwatch.PeriodOverride.AUTO,
    )

    # Row 1: traffic and latency
    dashboard.add_widgets(
        aws_cloudwatch.GraphWidget(
            title="Request Count",
            left=[
                load_balancer.metric_request_count(
                    period=cdk.Duration.minutes(1),
                    statistic="Sum",
                )
            ],
            width=12,
            height=_WIDGET_HEIGHT,
        ),
        aws_cloudwatch.GraphWidget(
            title="Target Response Time (p50 / p90 / p99)",
            left=[
                load_balancer.metric_target_response_time(
                    period=cdk.Duration.minutes(1),
                    statistic="p50",
                    label="p50",
                ),
                load_balancer.metric_target_response_time(
                    period=cdk.Duration.minutes(1),
                    statistic="p90",
                    label="p90",
                ),
                load_balancer.metric_target_response_time(
                    period=cdk.Duration.minutes(1),
                    statistic="p99",
                    label="p99",
                ),
            ],
            width=12,
            height=_WIDGET_HEIGHT,
        ),
    )

    # Row 2: HTTP status code breakdown
    dashboard.add_widgets(
        aws_cloudwatch.GraphWidget(
            title="HTTP 2XX",
            left=[
                load_balancer.metric_http_code_target(
                    code=aws_elb.HttpCodeTarget.TARGET_2XX_COUNT,
                    period=cdk.Duration.minutes(1),
                    statistic="Sum",
                )
            ],
            width=8,
            height=_WIDGET_HEIGHT,
        ),
        aws_cloudwatch.GraphWidget(
            title="HTTP 4XX",
            left=[
                load_balancer.metric_http_code_target(
                    code=aws_elb.HttpCodeTarget.TARGET_4XX_COUNT,
                    period=cdk.Duration.minutes(1),
                    statistic="Sum",
                )
            ],
            width=8,
            height=_WIDGET_HEIGHT,
        ),
        aws_cloudwatch.GraphWidget(
            title="HTTP 5XX (target + ALB-generated)",
            left=[
                load_balancer.metric_http_code_target(
                    code=aws_elb.HttpCodeTarget.TARGET_5XX_COUNT,
                    period=cdk.Duration.minutes(1),
                    statistic="Sum",
                    label="Target 5XX",
                ),
                load_balancer.metric_http_code_elb(
                    code=aws_elb.HttpCodeElb.ELB_5XX_COUNT,
                    period=cdk.Duration.minutes(1),
                    statistic="Sum",
                    label="ALB-generated 5XX",
                ),
            ],
            width=8,
            height=_WIDGET_HEIGHT,
        ),
    )

    # Row 3: target health + connections
    dashboard.add_widgets(
        aws_cloudwatch.GraphWidget(
            title="Healthy vs Unhealthy Hosts",
            left=[
                target_group.metric_healthy_host_count(
                    period=cdk.Duration.minutes(1),
                    statistic="Minimum",
                    label="Healthy",
                ),
                target_group.metric_unhealthy_host_count(
                    period=cdk.Duration.minutes(1),
                    statistic="Maximum",
                    label="Unhealthy",
                ),
            ],
            width=12,
            height=_WIDGET_HEIGHT,
        ),
        aws_cloudwatch.GraphWidget(
            title="Connections (active / new / rejected)",
            left=[
                load_balancer.metric_active_connection_count(
                    period=cdk.Duration.minutes(1),
                    statistic="Sum",
                    label="Active",
                ),
                load_balancer.metric_new_connection_count(
                    period=cdk.Duration.minutes(1),
                    statistic="Sum",
                    label="New",
                ),
                load_balancer.metric_rejected_connection_count(
                    period=cdk.Duration.minutes(1),
                    statistic="Sum",
                    label="Rejected",
                ),
            ],
            width=12,
            height=_WIDGET_HEIGHT,
        ),
    )

    # Row 4: target connection errors
    dashboard.add_widgets(
        aws_cloudwatch.GraphWidget(
            title="Target Connection Errors",
            left=[
                load_balancer.metric_target_connection_error_count(
                    period=cdk.Duration.minutes(1),
                    statistic="Sum",
                )
            ],
            width=24,
            height=_WIDGET_HEIGHT,
        ),
    )

    return dashboard


def create_rds_dashboard(
    scope: Construct,
    *,
    stage: Stage,
    database: aws_rds.DatabaseInstance,
    region: str,
) -> aws_cloudwatch.Dashboard:
    """`Valkyrie-RDS` -- PostgreSQL metrics + Performance Insights link."""
    dashboard = aws_cloudwatch.Dashboard(
        scope,
        "ValkyrieRdsDashboard",
        dashboard_name=stage.phys("Valkyrie-RDS"),
        default_interval=cdk.Duration.hours(24),
        period_override=aws_cloudwatch.PeriodOverride.AUTO,
    )

    perf_insights_url = (
        f"https://console.aws.amazon.com/rds/home?region={region}"
        f"#performance-insights-v20206:/resourceId/{database.instance_resource_id}"
    )
    dashboard.add_widgets(
        aws_cloudwatch.TextWidget(
            markdown=(
                "# Valkyrie RDS\n"
                f"For query-level drill-down, open "
                f"[Performance Insights]({perf_insights_url}) in the AWS console."
            ),
            width=24,
            height=2,
        ),
    )

    # Row 1: CPU / memory / connections / storage
    dashboard.add_widgets(
        aws_cloudwatch.GraphWidget(
            title="CPU Utilization (%)",
            left=[
                database.metric_cpu_utilization(
                    period=cdk.Duration.minutes(1),
                    statistic="Average",
                )
            ],
            width=6,
            height=_WIDGET_HEIGHT,
        ),
        aws_cloudwatch.GraphWidget(
            title="Freeable Memory (bytes)",
            left=[
                database.metric_freeable_memory(
                    period=cdk.Duration.minutes(1),
                    statistic="Minimum",
                )
            ],
            width=6,
            height=_WIDGET_HEIGHT,
        ),
        aws_cloudwatch.GraphWidget(
            title="Database Connections",
            left=[
                database.metric_database_connections(
                    period=cdk.Duration.minutes(1),
                    statistic="Maximum",
                )
            ],
            width=6,
            height=_WIDGET_HEIGHT,
        ),
        aws_cloudwatch.GraphWidget(
            title="Free Storage Space (bytes)",
            left=[
                database.metric_free_storage_space(
                    period=cdk.Duration.minutes(5),
                    statistic="Minimum",
                )
            ],
            width=6,
            height=_WIDGET_HEIGHT,
        ),
    )

    # Row 2: IOPS
    dashboard.add_widgets(
        aws_cloudwatch.GraphWidget(
            title="Read IOPS",
            left=[
                database.metric_read_iops(
                    period=cdk.Duration.minutes(1),
                    statistic="Average",
                )
            ],
            width=12,
            height=_WIDGET_HEIGHT,
        ),
        aws_cloudwatch.GraphWidget(
            title="Write IOPS",
            left=[
                database.metric_write_iops(
                    period=cdk.Duration.minutes(1),
                    statistic="Average",
                )
            ],
            width=12,
            height=_WIDGET_HEIGHT,
        ),
    )

    def rds_metric(name: str, statistic: str = "Average") -> aws_cloudwatch.Metric:
        return aws_cloudwatch.Metric(
            namespace="AWS/RDS",
            metric_name=name,
            dimensions_map={"DBInstanceIdentifier": database.instance_identifier},
            statistic=statistic,
            period=cdk.Duration.minutes(1),
        )

    # Row 3: latency (stubs lack metric_read_latency/metric_write_latency helpers)
    dashboard.add_widgets(
        aws_cloudwatch.GraphWidget(
            title="Read Latency (seconds)",
            left=[rds_metric("ReadLatency")],
            width=12,
            height=_WIDGET_HEIGHT,
        ),
        aws_cloudwatch.GraphWidget(
            title="Write Latency (seconds)",
            left=[rds_metric("WriteLatency")],
            width=12,
            height=_WIDGET_HEIGHT,
        ),
    )

    # Row 4: throughput
    dashboard.add_widgets(
        aws_cloudwatch.GraphWidget(
            title="Read Throughput (bytes/s)",
            left=[rds_metric("ReadThroughput")],
            width=12,
            height=_WIDGET_HEIGHT,
        ),
        aws_cloudwatch.GraphWidget(
            title="Write Throughput (bytes/s)",
            left=[rds_metric("WriteThroughput")],
            width=12,
            height=_WIDGET_HEIGHT,
        ),
    )

    # Row 5: burst balance, network, swap, disk queue
    dashboard.add_widgets(
        aws_cloudwatch.GraphWidget(
            title="Burst Balance (%)",
            left=[rds_metric("BurstBalance", "Minimum")],
            width=6,
            height=_WIDGET_HEIGHT,
        ),
        aws_cloudwatch.GraphWidget(
            title="Network Throughput (RX / TX bytes/s)",
            left=[rds_metric("NetworkReceiveThroughput")],
            right=[rds_metric("NetworkTransmitThroughput")],
            width=6,
            height=_WIDGET_HEIGHT,
        ),
        aws_cloudwatch.GraphWidget(
            title="Swap Usage (bytes)",
            left=[rds_metric("SwapUsage", "Average")],
            width=6,
            height=_WIDGET_HEIGHT,
        ),
        aws_cloudwatch.GraphWidget(
            title="Disk Queue Depth",
            left=[rds_metric("DiskQueueDepth", "Average")],
            width=6,
            height=_WIDGET_HEIGHT,
        ),
    )

    return dashboard


def create_redis_dashboard(
    scope: Construct,
    *,
    stage: Stage,
    redis_cluster: aws_elasticache.CfnCacheCluster,
) -> aws_cloudwatch.Dashboard:
    """`Valkyrie-Redis` -- ElastiCache metrics including command breakdown."""
    dashboard = aws_cloudwatch.Dashboard(
        scope,
        "ValkyrieRedisDashboard",
        dashboard_name=stage.phys("Valkyrie-Redis"),
        default_interval=cdk.Duration.hours(24),
        period_override=aws_cloudwatch.PeriodOverride.AUTO,
    )

    # Row 1: CPU utilization (instance-level + engine-level)
    dashboard.add_widgets(
        aws_cloudwatch.GraphWidget(
            title="CPU Utilization (%) - Instance",
            left=[redis_metric(redis_cluster, "CPUUtilization", statistic="Average")],
            width=12,
            height=_WIDGET_HEIGHT,
        ),
        aws_cloudwatch.GraphWidget(
            title="Engine CPU Utilization (%)",
            left=[redis_metric(redis_cluster, "EngineCPUUtilization", statistic="Average")],
            width=12,
            height=_WIDGET_HEIGHT,
        ),
    )

    # Row 2: memory
    dashboard.add_widgets(
        aws_cloudwatch.GraphWidget(
            title="Memory Used for Cache (bytes)",
            left=[redis_metric(redis_cluster, "BytesUsedForCache", statistic="Average")],
            width=12,
            height=_WIDGET_HEIGHT,
        ),
        aws_cloudwatch.GraphWidget(
            title="Memory Usage (%)",
            left=[
                redis_metric(
                    redis_cluster,
                    "DatabaseMemoryUsagePercentage",
                    statistic="Average",
                )
            ],
            width=12,
            height=_WIDGET_HEIGHT,
        ),
    )

    # Row 3: connections
    dashboard.add_widgets(
        aws_cloudwatch.GraphWidget(
            title="Current Connections",
            left=[redis_metric(redis_cluster, "CurrConnections", statistic="Maximum")],
            width=12,
            height=_WIDGET_HEIGHT,
        ),
        aws_cloudwatch.GraphWidget(
            title="New Connections per Minute",
            left=[redis_metric(redis_cluster, "NewConnections", statistic="Sum")],
            width=12,
            height=_WIDGET_HEIGHT,
        ),
    )

    # Row 4: network + evictions
    dashboard.add_widgets(
        aws_cloudwatch.GraphWidget(
            title="Network Bytes In/Out",
            left=[redis_metric(redis_cluster, "NetworkBytesIn", statistic="Sum")],
            right=[redis_metric(redis_cluster, "NetworkBytesOut", statistic="Sum")],
            width=12,
            height=_WIDGET_HEIGHT,
        ),
        aws_cloudwatch.GraphWidget(
            title="Evictions / Reclaimed",
            left=[redis_metric(redis_cluster, "Evictions", statistic="Sum")],
            right=[redis_metric(redis_cluster, "Reclaimed", statistic="Sum")],
            width=12,
            height=_WIDGET_HEIGHT,
        ),
    )

    # Row 5: hit/miss + command breakdown
    dashboard.add_widgets(
        aws_cloudwatch.GraphWidget(
            title="Cache Hits / Misses",
            left=[
                redis_metric(redis_cluster, "CacheHits", statistic="Sum"),
                redis_metric(redis_cluster, "CacheMisses", statistic="Sum"),
            ],
            width=12,
            height=_WIDGET_HEIGHT,
        ),
        aws_cloudwatch.GraphWidget(
            title="Command Breakdown (per-second rate)",
            left=[
                redis_metric(redis_cluster, "GetTypeCmds", statistic="Sum"),
                redis_metric(redis_cluster, "SetTypeCmds", statistic="Sum"),
                redis_metric(redis_cluster, "ListBasedCmds", statistic="Sum"),
                redis_metric(redis_cluster, "StreamBasedCmds", statistic="Sum"),
            ],
            width=12,
            height=_WIDGET_HEIGHT,
        ),
    )

    # Row 6: replication bytes (single-node shows 0; kept for future HA consistency)
    dashboard.add_widgets(
        aws_cloudwatch.GraphWidget(
            title="Replication Bytes",
            left=[redis_metric(redis_cluster, "ReplicationBytes", statistic="Average")],
            width=24,
            height=_WIDGET_HEIGHT,
        ),
    )

    return dashboard


def redis_metric(
    redis_cluster: aws_elasticache.CfnCacheCluster,
    metric_name: str,
    *,
    statistic: str = "Average",
    period: cdk.Duration | None = None,
) -> aws_cloudwatch.Metric:
    """Helper: ElastiCache metric with CacheClusterId dimension."""
    return aws_cloudwatch.Metric(
        namespace="AWS/ElastiCache",
        metric_name=metric_name,
        dimensions_map={"CacheClusterId": redis_cluster.ref},
        statistic=statistic,
        period=period or cdk.Duration.minutes(1),
    )


def ecs_container_insights_metric(
    service: aws_ecs.FargateService,
    metric_name: str,
    *,
    statistic: str = "Average",
    period: cdk.Duration | None = None,
) -> aws_cloudwatch.Metric:
    """Helper: Container Insights metric for a Fargate service."""
    return aws_cloudwatch.Metric(
        namespace="ECS/ContainerInsights",
        metric_name=metric_name,
        dimensions_map={
            "ClusterName": service.cluster.cluster_name,
            "ServiceName": service.service_name,
        },
        statistic=statistic,
        period=period or cdk.Duration.minutes(1),
    )
