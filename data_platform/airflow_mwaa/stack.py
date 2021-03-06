from aws_cdk import core
from aws_cdk import (
    aws_mwaa as mwaa,
    aws_logs as logs,
    aws_ec2 as ec2,
    aws_s3 as s3,
    aws_iam as iam,
    aws_s3_deployment as s3deploy,
)

from common_stack import CommonResourcesStack
from data_platform.data_lake.base import BaseDataLakeBucket
from data_platform.active_environment import active_environment
import os
from zipfile import ZipFile


class AirflowStack(core.Stack):
    def __init__(
        self,
        scope: core.Construct,
        common_stack: CommonResourcesStack,
        data_lake_bronze_bucket: BaseDataLakeBucket,
        data_lake_silver_bucket: BaseDataLakeBucket,
        **kwargs,
    ) -> None:
        self.deploy_env = active_environment
        self.common_stack = common_stack
        self.data_lake_bronze_bucket = data_lake_bronze_bucket
        self.data_lake_silver_bucket = data_lake_silver_bucket
        super().__init__(scope, id=f"{self.deploy_env.value}-airflow-stack", **kwargs)

        self.log_group = logs.LogGroup(
            self,
            id=f"{self.deploy_env.value}-airflow-log-group",
            log_group_name=f"{self.deploy_env.value}-airflow-log-group",
            retention=logs.RetentionDays.THREE_MONTHS,
            removal_policy=core.RemovalPolicy.DESTROY,
        )

        self.logging_configuration = (
            mwaa.CfnEnvironment.ModuleLoggingConfigurationProperty(
                cloud_watch_log_group_arn=self.log_group.log_group_arn,
                enabled=True,
                log_level="WARNING",
            )
        )

        self.security_group = ec2.SecurityGroup(
            self,
            f"airflow-{self.deploy_env.value}-sg",
            vpc=self.common_stack.custom_vpc,
            allow_all_outbound=True,
            security_group_name=f"airflow-{self.deploy_env.value}-sg",
        )

        self.security_group.add_ingress_rule(
            peer=ec2.Peer.ipv4("0.0.0.0/0"), connection=ec2.Port.tcp(5432)
        )

        self.airflow_bucket = s3.Bucket(
            self,
            id=f"airflow-{self.deploy_env.value}-bucket",
            bucket_name=f"airflow-{self.deploy_env.value}-{self.account}-{self.region}",
            removal_policy=core.RemovalPolicy.DESTROY,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            versioned=True
        )

        self.execution_role = iam.Role(
            self,
            id=f"iam-{self.deploy_env.value}-data-lake-raw-airflow-role",
            description="Role to allow Airflow to access resources",
            assumed_by=iam.ServicePrincipal("airflow.amazonaws.com"),
        )
        self.execution_role.assume_role_policy.add_statements(
            iam.PolicyStatement(
                principals=[iam.ServicePrincipal("airflow-env.amazonaws.com")],
                actions=["sts:AssumeRole"],
            )
        )

        self.execution_policy = iam.Policy(
            self,
            id=f"iam-{self.deploy_env.value}-airflow-execution-policy",
            policy_name=f"iam-{self.deploy_env.value}-airflow-execution-policy",
            statements=[
                iam.PolicyStatement(
                    actions=[
                        "s3:PutObjectTagging",
                        "s3:DeleteObject",
                        "s3:ListBucket",
                        "s3:GetObject",
                        "s3:PutObject",
                    ],
                    resources=[
                        self.data_lake_bronze_bucket.bucket_arn,
                        f"{self.data_lake_bronze_bucket.bucket_arn}/*",
                        self.data_lake_silver_bucket.bucket_arn,
                        f"{self.data_lake_silver_bucket.bucket_arn}/*",
                    ],
                ),
                iam.PolicyStatement(
                    actions=["airflow:PublishMetrics"],
                    resources=[
                        f"arn:aws:airflow:{self.region}:{self.account}:environment/{self.deploy_env.value}-airflow"
                    ],
                ),
                iam.PolicyStatement(
                    actions=["airflow:PublishMetrics"],
                    resources=[
                        f"arn:aws:airflow:{self.region}:{self.account}:environment/{self.deploy_env.value}-airflow"
                    ],
                ),
                iam.PolicyStatement(
                    actions=["s3:*"],
                    resources=[
                        f"{self.airflow_bucket.bucket_arn}/*",
                        f"{self.airflow_bucket.bucket_arn}",
                    ],
                ),
                iam.PolicyStatement(
                    actions=[
                        "logs:CreateLogStream",
                        "logs:CreateLogGroup",
                        "logs:PutLogEvents",
                        "logs:GetLogEvents",
                        "logs:GetLogRecord",
                        "logs:GetLogGroupFields",
                        "logs:GetQueryResults",
                    ],
                    resources=[
                        self.log_group.log_group_arn,
                        f"{self.log_group.log_group_arn}*",
                    ],
                ),
                iam.PolicyStatement(actions=["logs:DescribeLogGroups"], resources=["*"]),
                iam.PolicyStatement(
                    actions=["cloudwatch:PutMetricData"], resources=["*"]
                ),
                iam.PolicyStatement(
                    actions=[
                        "sqs:ChangeMessageVisibility",
                        "sqs:DeleteMessage",
                        "sqs:GetQueueAttributes",
                        "sqs:GetQueueUrl",
                        "sqs:ReceiveMessage",
                        "sqs:SendMessage",
                    ],
                    resources=[f"arn:aws:sqs:{self.region}:*:airflow-celery-*"],
                ),
                iam.PolicyStatement(
                    actions=[
                        "cloudformation:Describe*",
                        "cloudformation:EstimateTemplateCost",
                        "cloudformation:Get*",
                        "cloudformation:List*",
                        "cloudformation:ValidateTemplate",
                        "cloudformation:Detect*"
                    ],
                    resources=["*"]
                ),
            ],
        )

        self.execution_role.attach_inline_policy(self.execution_policy)
        self.execution_role.add_managed_policy(
            policy=iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonElasticMapReduceFullAccess"
                )
        )

        with ZipFile("data_platform/airflow_mwaa/resources.zip", "w") as zipObj2:
            zipObj2.write(
                "data_platform/airflow_mwaa/requirements.txt", arcname="requirements.txt"
            )
            for file in os.listdir("data_platform/airflow_mwaa/dags"):
                zipObj2.write(
                    f"data_platform/airflow_mwaa/dags/{file}", arcname=f"dags/{file}"
                )

        self.deploy_files = s3deploy.BucketDeployment(
            self,
            id=f"{self.deploy_env.value}-airflow-content",
            destination_bucket=self.airflow_bucket,
            sources=[s3deploy.Source.asset("data_platform/airflow_mwaa/resources.zip")],
        )

        self.airflow = mwaa.CfnEnvironment(
            self,
            id=f"mwaa-{self.deploy_env.value}",
            name=f"mwaa-{self.deploy_env.value}",
            airflow_version="1.10.12",
            dag_s3_path="dags",
            environment_class="mw1.small",
            execution_role_arn=self.execution_role.role_arn,
            logging_configuration=mwaa.CfnEnvironment.LoggingConfigurationProperty(
                dag_processing_logs=self.logging_configuration,
                scheduler_logs=self.logging_configuration,
                task_logs=self.logging_configuration,
                webserver_logs=self.logging_configuration,
                worker_logs=self.logging_configuration,
            ),
            max_workers=2,
            min_workers=1,
            network_configuration=mwaa.CfnEnvironment.NetworkConfigurationProperty(
                security_group_ids=[self.security_group.security_group_id],
                subnet_ids=[
                    subnet.subnet_id
                    for subnet in self.common_stack.custom_vpc.private_subnets
                ],
            ),
            webserver_access_mode="PUBLIC_ONLY",
            weekly_maintenance_window_start="WED:01:00",
            source_bucket_arn=self.airflow_bucket.bucket_arn,
            requirements_s3_path="requirements.txt",
        )

        self.airflow.node.add_dependency(self.execution_role)
        self.airflow.node.add_dependency(self.log_group)
        self.airflow.node.add_dependency(self.security_group)
        self.airflow.node.add_dependency(self.airflow_bucket)
        self.airflow.node.add_dependency(self.deploy_files)
