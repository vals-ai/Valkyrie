from __future__ import annotations

from typing import Any

import aws_cdk as cdk
from aws_cdk import (
    aws_cloudwatch,
    aws_cloudwatch_actions,
    aws_ecs,
    aws_elasticache,
    aws_elasticloadbalancingv2 as aws_elb,
    aws_rds,
    aws_sns,
)
from constructs import Construct
from dashboards import (
    create_alb_dashboard,
    create_ecs_dashboard,
    create_overview_dashboard,
    create_rds_dashboard,
    create_redis_dashboard,
)


class MonitoringStack(cdk.Stack):
    """CloudWatch dashboards and alarms for Valkyrie infrastructure.

    Depends on SharedStack (cluster, Redis), TrackerStack (RDS, ALB,
    Tracker service), and WorkerStack (Worker service).

    Alarms publish to the ``Valkyrie-Alerts`` SNS topic.
    """

    def __init__(
        self,
        scope: Construct,
        id: str,
        *,
        cluster: aws_ecs.ICluster,
        tracker_service: aws_ecs.FargateService,
        worker_service: aws_ecs.FargateService,
        load_balancer: aws_elb.ApplicationLoadBalancer,
        target_group: aws_elb.ApplicationTargetGroup,
        database: aws_rds.DatabaseInstance,
        redis_cluster: aws_elasticache.CfnCacheCluster,
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, id, **kwargs)

        self.cluster = cluster
        self.tracker_service = tracker_service
        self.worker_service = worker_service
        self.load_balancer = load_balancer
        self.target_group = target_group
        self.database = database
        self.redis_cluster = redis_cluster

        # SNS alerts topic. No subscribers yet.
        self.alerts_topic = aws_sns.Topic(
            self,
            "ValkyrieAlertsTopic",
            topic_name="Valkyrie-Alerts",
            display_name="Valkyrie infrastructure alerts",
        )

        self._create_alarms(
            tracker_service=tracker_service,
            worker_service=worker_service,
            load_balancer=load_balancer,
            target_group=target_group,
            database=database,
            redis_cluster=redis_cluster,
        )

        self.overview_dashboard = create_overview_dashboard(
            self,
            tracker_service=tracker_service,
            worker_service=worker_service,
            load_balancer=load_balancer,
            target_group=target_group,
            database=database,
            redis_cluster=redis_cluster,
        )
        self.ecs_dashboard = create_ecs_dashboard(
            self,
            tracker_service=tracker_service,
            worker_service=worker_service,
        )
        self.alb_dashboard = create_alb_dashboard(
            self,
            load_balancer=load_balancer,
            target_group=target_group,
        )
        self.rds_dashboard = create_rds_dashboard(
            self,
            database=database,
            region=self.region,
        )
        self.redis_dashboard = create_redis_dashboard(
            self,
            redis_cluster=redis_cluster,
        )

    def _create_alarms(
        self,
        *,
        tracker_service: aws_ecs.FargateService,
        worker_service: aws_ecs.FargateService,
        load_balancer: aws_elb.ApplicationLoadBalancer,
        target_group: aws_elb.ApplicationTargetGroup,
        database: aws_rds.DatabaseInstance,
        redis_cluster: aws_elasticache.CfnCacheCluster,
    ) -> None:
        sns_action: aws_cloudwatch.IAlarmAction = aws_cloudwatch_actions.SnsAction(
            self.alerts_topic  # type: ignore[arg-type]
        )

        # Alarm 1: Tracker unhealthy hosts
        aws_cloudwatch.Alarm(
            self,
            "TrackerUnhealthyAlarm",
            alarm_name="Valkyrie-Tracker-Unhealthy",
            alarm_description="Tracker ALB has at least one unhealthy target for 2+ minutes",
            metric=target_group.metric_unhealthy_host_count(
                period=cdk.Duration.minutes(1),
                statistic="Maximum",
            ),
            threshold=1,
            evaluation_periods=2,
            datapoints_to_alarm=2,
            comparison_operator=aws_cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            treat_missing_data=aws_cloudwatch.TreatMissingData.NOT_BREACHING,
        ).add_alarm_action(sns_action)

        # Alarm 2: Worker service down (min=1 autoscale floor breached)
        # Note: this alarm assumes min=1 autoscaling.
        aws_cloudwatch.Alarm(
            self,
            "WorkerServiceDownAlarm",
            alarm_name="Valkyrie-Worker-Service-Down",
            alarm_description=(
                "Worker Fargate running task count = 0 for 3+ minutes. "
                "Worker containers not running (min=1 autoscale floor breached)."
            ),
            metric=aws_cloudwatch.Metric(
                namespace="ECS/ContainerInsights",
                metric_name="RunningTaskCount",
                dimensions_map={
                    "ClusterName": worker_service.cluster.cluster_name,
                    "ServiceName": worker_service.service_name,
                },
                period=cdk.Duration.minutes(1),
                statistic="Maximum",
            ),
            threshold=1,
            evaluation_periods=3,
            datapoints_to_alarm=3,
            comparison_operator=aws_cloudwatch.ComparisonOperator.LESS_THAN_THRESHOLD,
            treat_missing_data=aws_cloudwatch.TreatMissingData.BREACHING,
        ).add_alarm_action(sns_action)

        # Alarm 3: High 5xx rate
        aws_cloudwatch.Alarm(
            self,
            "HighFiveXXRateAlarm",
            alarm_name="Valkyrie-API-5XX-Rate-High",
            alarm_description="API returning 10+ 5XX responses in a 5-minute window",
            metric=load_balancer.metric_http_code_target(
                code=aws_elb.HttpCodeTarget.TARGET_5XX_COUNT,
                period=cdk.Duration.minutes(5),
                statistic="Sum",
            ),
            threshold=10,
            evaluation_periods=1,
            comparison_operator=aws_cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            treat_missing_data=aws_cloudwatch.TreatMissingData.NOT_BREACHING,
        ).add_alarm_action(sns_action)

        # Alarm 4: High API latency (p99)
        aws_cloudwatch.Alarm(
            self,
            "HighApiLatencyAlarm",
            alarm_name="Valkyrie-API-Latency-High",
            alarm_description="API target response time p99 >= 5s for 5+ minutes",
            metric=load_balancer.metric_target_response_time(
                period=cdk.Duration.minutes(1),
                statistic="p99",
            ),
            threshold=5,
            evaluation_periods=5,
            datapoints_to_alarm=5,
            comparison_operator=aws_cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            treat_missing_data=aws_cloudwatch.TreatMissingData.NOT_BREACHING,
        ).add_alarm_action(sns_action)

        # Alarm 5: DB connections high (> 80% of max for t4g.micro: ~83 connections)
        # Max connections on t4g.micro is ~83 (LEAST(DBInstanceClassMemory/9531392, 5000))
        # Set threshold to 65 (roughly 80% of 83)
        aws_cloudwatch.Alarm(
            self,
            "DbConnectionsHighAlarm",
            alarm_name="Valkyrie-DB-Connections-High",
            alarm_description="RDS database connections >= 65 (~80% of t4g.micro max)",
            metric=database.metric_database_connections(
                period=cdk.Duration.minutes(1),
                statistic="Maximum",
            ),
            threshold=65,
            evaluation_periods=5,
            datapoints_to_alarm=5,
            comparison_operator=aws_cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            treat_missing_data=aws_cloudwatch.TreatMissingData.NOT_BREACHING,
        ).add_alarm_action(sns_action)

        # Alarm 6: DB storage low
        aws_cloudwatch.Alarm(
            self,
            "DbStorageLowAlarm",
            alarm_name="Valkyrie-DB-Storage-Low",
            alarm_description="RDS free storage space <= 2 GB",
            metric=database.metric_free_storage_space(
                period=cdk.Duration.minutes(5),
                statistic="Minimum",
            ),
            threshold=2 * 1024 * 1024 * 1024,  # 2 GB in bytes
            evaluation_periods=1,
            comparison_operator=aws_cloudwatch.ComparisonOperator.LESS_THAN_OR_EQUAL_TO_THRESHOLD,
            treat_missing_data=aws_cloudwatch.TreatMissingData.NOT_BREACHING,
        ).add_alarm_action(sns_action)

        # Alarm 7: DB CPU high
        aws_cloudwatch.Alarm(
            self,
            "DbCpuHighAlarm",
            alarm_name="Valkyrie-DB-CPU-High",
            alarm_description="RDS CPU utilization >= 80% for 10+ minutes",
            metric=database.metric_cpu_utilization(
                period=cdk.Duration.minutes(1),
                statistic="Average",
            ),
            threshold=80,
            evaluation_periods=10,
            datapoints_to_alarm=10,
            comparison_operator=aws_cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            treat_missing_data=aws_cloudwatch.TreatMissingData.NOT_BREACHING,
        ).add_alarm_action(sns_action)

        # Alarm 8: Redis memory high (DatabaseMemoryUsagePercentage >= 80 for 5 min)
        aws_cloudwatch.Alarm(
            self,
            "RedisMemoryHighAlarm",
            alarm_name="Valkyrie-Redis-Memory-High",
            alarm_description="Redis memory usage >= 80% for 5+ minutes",
            metric=aws_cloudwatch.Metric(
                namespace="AWS/ElastiCache",
                metric_name="DatabaseMemoryUsagePercentage",
                dimensions_map={"CacheClusterId": redis_cluster.ref},
                period=cdk.Duration.minutes(1),
                statistic="Average",
            ),
            threshold=80,
            evaluation_periods=5,
            datapoints_to_alarm=5,
            comparison_operator=aws_cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            treat_missing_data=aws_cloudwatch.TreatMissingData.NOT_BREACHING,
        ).add_alarm_action(sns_action)

        # Alarm 9: Redis evictions
        aws_cloudwatch.Alarm(
            self,
            "RedisEvictionsAlarm",
            alarm_name="Valkyrie-Redis-Evictions",
            alarm_description="Redis evicted 1+ keys in a 5-minute window (data loss in queue)",
            metric=aws_cloudwatch.Metric(
                namespace="AWS/ElastiCache",
                metric_name="Evictions",
                dimensions_map={"CacheClusterId": redis_cluster.ref},
                period=cdk.Duration.minutes(5),
                statistic="Sum",
            ),
            threshold=1,
            evaluation_periods=1,
            comparison_operator=aws_cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            treat_missing_data=aws_cloudwatch.TreatMissingData.NOT_BREACHING,
        ).add_alarm_action(sns_action)
