from __future__ import annotations

from typing import Any

import aws_cdk as cdk
from aws_cdk import (
    aws_chatbot,
    aws_cloudwatch,
    aws_cloudwatch_actions,
    aws_ecs,
    aws_elasticache,
    aws_elasticloadbalancingv2 as aws_elb,
    aws_rds,
    aws_sns,
)
from constants import (
    VALKYRIE_ALERTS_SLACK_CHANNEL_ID_ENV,
    get_slack_notification_config,
)
from constructs import Construct
from dashboards import (
    create_alb_dashboard,
    create_ecs_dashboard,
    create_overview_dashboard,
    create_rds_dashboard,
    create_redis_dashboard,
)
from stage import Stage
from stage_config import config_for


class MonitoringStack(cdk.Stack):
    """CloudWatch dashboards and alarms for Valkyrie infrastructure.

    Depends on SharedStack (cluster, Redis) and TrackerStack (RDS, ALB,
    Tracker service).

    Alarms publish to the ``Valkyrie-Alerts`` SNS topic.
    """

    def __init__(
        self,
        scope: Construct,
        id: str,
        *,
        stage: Stage,
        cluster: aws_ecs.ICluster,
        tracker_service: aws_ecs.FargateService,
        load_balancer: aws_elb.ApplicationLoadBalancer,
        target_group: aws_elb.ApplicationTargetGroup,
        database: aws_rds.DatabaseInstance,
        redis_cluster: aws_elasticache.CfnCacheCluster,
        **kwargs: Any,
    ) -> None:
        super().__init__(scope, id, **kwargs)
        self.stage = stage
        self.stage_config = config_for(stage)

        self.cluster = cluster
        self.tracker_service = tracker_service
        self.load_balancer = load_balancer
        self.target_group = target_group
        self.database = database
        self.redis_cluster = redis_cluster

        self.alerts_topic = aws_sns.Topic(
            self,
            "ValkyrieAlertsTopic",
            topic_name=self.stage.phys("Valkyrie-Alerts"),
            display_name="Valkyrie infrastructure alerts",
        )
        slack_config = get_slack_notification_config(VALKYRIE_ALERTS_SLACK_CHANNEL_ID_ENV)
        if slack_config is not None:
            slack_workspace_id, slack_channel_id = slack_config
            self.alerts_slack = aws_chatbot.SlackChannelConfiguration(
                self,
                "ValkyrieAlertsSlackChannel",
                slack_channel_configuration_name=self.stage.phys("valkyrie-alerts"),
                slack_workspace_id=slack_workspace_id,
                slack_channel_id=slack_channel_id,
            )
            self.alerts_slack.add_notification_topic(self.alerts_topic)  # type: ignore[arg-type]

        self._create_alarms(
            load_balancer=load_balancer,
            target_group=target_group,
            database=database,
            redis_cluster=redis_cluster,
        )

        self.overview_dashboard = create_overview_dashboard(
            self,
            stage=self.stage,
            tracker_service=tracker_service,
            load_balancer=load_balancer,
            target_group=target_group,
            database=database,
            redis_cluster=redis_cluster,
        )
        self.ecs_dashboard = create_ecs_dashboard(
            self,
            stage=self.stage,
            tracker_service=tracker_service,
        )
        self.alb_dashboard = create_alb_dashboard(
            self,
            stage=self.stage,
            load_balancer=load_balancer,
            target_group=target_group,
        )
        self.rds_dashboard = create_rds_dashboard(
            self,
            stage=self.stage,
            database=database,
            region=self.region,
        )
        self.redis_dashboard = create_redis_dashboard(
            self,
            stage=self.stage,
            redis_cluster=redis_cluster,
        )

    def _create_alarms(
        self,
        *,
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
            alarm_name=self.stage.phys("Valkyrie-Tracker-Unhealthy"),
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

        # Alarm 2: High 5xx rate
        aws_cloudwatch.Alarm(
            self,
            "HighFiveXXRateAlarm",
            alarm_name=self.stage.phys("Valkyrie-API-5XX-Rate-High"),
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

        # Alarm 3: High API latency (p99)
        aws_cloudwatch.Alarm(
            self,
            "HighApiLatencyAlarm",
            alarm_name=self.stage.phys("Valkyrie-API-Latency-High"),
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

        # Alarm 4: DB connections high
        db_connection_threshold = self.stage_config.database.connection_alarm_threshold
        aws_cloudwatch.Alarm(
            self,
            "DbConnectionsHighAlarm",
            alarm_name=self.stage.phys("Valkyrie-DB-Connections-High"),
            alarm_description=f"RDS database connections >= {db_connection_threshold}",
            metric=database.metric_database_connections(
                period=cdk.Duration.minutes(1),
                statistic="Maximum",
            ),
            threshold=db_connection_threshold,
            evaluation_periods=5,
            datapoints_to_alarm=5,
            comparison_operator=aws_cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            treat_missing_data=aws_cloudwatch.TreatMissingData.NOT_BREACHING,
        ).add_alarm_action(sns_action)

        # Alarm 5: DB storage low
        aws_cloudwatch.Alarm(
            self,
            "DbStorageLowAlarm",
            alarm_name=self.stage.phys("Valkyrie-DB-Storage-Low"),
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

        # Alarm 6: DB CPU high
        aws_cloudwatch.Alarm(
            self,
            "DbCpuHighAlarm",
            alarm_name=self.stage.phys("Valkyrie-DB-CPU-High"),
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

        # Alarm 7: Redis memory high (DatabaseMemoryUsagePercentage >= 80 for 5 min)
        aws_cloudwatch.Alarm(
            self,
            "RedisMemoryHighAlarm",
            alarm_name=self.stage.phys("Valkyrie-Redis-Memory-High"),
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

        # Alarm 8: Redis evictions
        aws_cloudwatch.Alarm(
            self,
            "RedisEvictionsAlarm",
            alarm_name=self.stage.phys("Valkyrie-Redis-Evictions"),
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
