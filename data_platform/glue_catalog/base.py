from enum import Enum
from aws_cdk import core
from aws_cdk import aws_glue as glue, aws_iam as iam
from data_platform.data_lake.base import BaseDataLakeBucket


class BaseDataLakeGlueDatabase(glue.Database):
    """
    Creates a glue database associated to a data lake bucket
    """

    def __init__(
        self, scope: core.Construct, data_lake_bucket: BaseDataLakeBucket, **kwargs
    ) -> None:
        self.data_lake_bucket = data_lake_bucket
        self.deploy_env = scope.deploy_env
        self.obj_name = f"glue-ecommerce-{self.deploy_env.value}-data-lake-{self.data_lake_bucket.layer.value}"

        super().__init__(
            scope,
            self.obj_name,
            database_name=self.database_name,
            location_uri=self.location_uri,
        )

    @property
    def database_name(self):
        """
        Returns the glue database name
        """
        return self.obj_name.replace("-", "_")

    @property
    def location_uri(self):
        """
        Returns the database location
        """
        return f"s3://{self.data_lake_bucket.bucket_name}"


class BaseDataLakeGlueRole(iam.Role):
    def __init__(
        self,
        scope: core.Construct,
        data_lake_bronze_bucket: BaseDataLakeBucket,
        data_lake_silver_bucket: BaseDataLakeBucket,
        data_lake_gold_bucket: BaseDataLakeBucket,
        **kwargs,
    ) -> None:
        self.data_lake_bronze_bucket = data_lake_bronze_bucket
        self.data_lake_silver_bucket = data_lake_silver_bucket
        self.data_lake_gold_bucket = data_lake_gold_bucket
        self.deploy_env = scope.deploy_env
        super().__init__(
            scope,
            id=f"iam-{self.deploy_env.value}-glue-data-lake-role",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            description=f"Allows using Glue on Data Lake",
        )
        self.bronze_bucket_arn = self.data_lake_bronze_bucket.bucket_arn
        self.silver_bucket_arn = self.data_lake_silver_bucket.bucket_arn
        self.gold_bucket_arn = self.data_lake_gold_bucket.bucket_arn

        self.add_policy()
        self.add_instance_profile()

    def add_policy(self):
        policy = iam.Policy(
            self,
            id=f"iam-{self.deploy_env.value}-glue-data-lake-policy",
            policy_name=f"iam-{self.deploy_env.value}-glue-data-lake-policy",
            statements=[
                iam.PolicyStatement(
                    actions=["s3:ListBucket", "s3:GetObject", "s3:PutObject"],
                    resources=[
                        self.bronze_bucket_arn,
                        f"{self.bronze_bucket_arn}/*",
                        self.silver_bucket_arn,
                        f"{self.silver_bucket_arn}/*",
                        self.gold_bucket_arn,
                        f"{self.gold_bucket_arn}/*",
                    ],
                ),
                iam.PolicyStatement(
                    actions=["cloudwatch:PutMetricData"],
                    resources=["arn:aws:cloudwatch:*"],
                ),
                iam.PolicyStatement(actions=["glue:*"], resources=["arn:aws:glue:*"]),
                iam.PolicyStatement(
                    actions=[
                        "logs:CreateLogGroup",
                        "logs:CreateLogStream",
                        "logs:PutLogEvents",
                    ],
                    resources=["arn:aws:logs:*:*:/aws-glue/*"],
                ),
            ],
        )
        self.attach_inline_policy(policy)

    def add_instance_profile(self):
        iam.CfnInstanceProfile(
            self,
            id=f"iam-{self.deploy_env.value}-glue-data-lake-instance-profile",
            instance_profile_name=f"iam-{self.deploy_env.value}-glue-data-lake-instance-profile",
            roles=[self.role_name],
        )


class BaseGlueCrawler(glue.CfnCrawler):
    def __init__(
        self,
        scope: core.Construct,
        table_name: str,
        glue_database: BaseDataLakeGlueDatabase,
        schedule_expression: str,
        glue_role: BaseDataLakeGlueRole,
        **kwargs,
    ) -> None:

        self.glue_database = glue_database
        self.glue_role = glue_role
        self.schedule_expression = schedule_expression
        self.table_name = table_name
        self.deploy_env = self.glue_database.deploy_env
        self.data_lake_bucket = self.glue_database.data_lake_bucket
        self.obj_name = f"glue-{self.deploy_env.value}-{self.data_lake_bucket.layer.value}-{self.table_name}-crawler"
        super().__init__(
            scope,
            id=self.obj_name,
            name=self.obj_name,
            description=f"Crawler that detects the schema located at "
            f"Data Lake {self.data_lake_bucket.layer.value}.{self.table_name}",
            schedule=self.crawler_schedule,
            role=self.glue_role.role_arn,
            database_name=self.glue_database.database_name,
            targets=self.targets,
            **kwargs,
        )

    @property
    def crawler_schedule(self):
        return glue.CfnCrawler.ScheduleProperty(
            schedule_expression=self.schedule_expression
        )

    @property
    def targets(self):
        return glue.CfnCrawler.TargetsProperty(
            s3_targets=[
                glue.CfnCrawler.S3TargetProperty(
                    path=f"s3://{self.data_lake_bucket.bucket_name}/{self.table_name}"
                ),
            ]
        )
