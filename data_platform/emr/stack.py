from aws_cdk import core
from aws_cdk import (
    aws_emr as emr,
    aws_ec2 as ec2,
    aws_iam as iam,
)
from data_platform.common_stack import CommonResourcesStack
from data_platform.active_environment import active_environment


class EMRStack(core.Stack):
    def __init__(
        self,
        scope: core.Construct,
        common_stack: CommonResourcesStack,
        spark_script: str,
        **kwargs,
    ) -> None:
        self.deploy_env = active_environment
        self.s3_script_bucket = common_stack.s3_script_bucket
        self.s3_log_bucket = common_stack.s3_log_bucket

        super().__init__(scope, id=f"{self.deploy_env.value}-emr-stack", **kwargs)

        # enable reading scripts from s3 bucket
        read_scripts_policy = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "s3:GetObject",
            ],
            resources=[f"arn:aws:s3:::{self.s3_script_bucket.bucket_name}/*"],
        )
        read_scripts_document = iam.PolicyDocument()
        read_scripts_document.add_statements(read_scripts_policy)

        # emr service role
        emr_service_role = iam.Role(
            self,
            "emr_service_role",
            assumed_by=iam.ServicePrincipal("elasticmapreduce.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AmazonElasticMapReduceRole"
                )
            ],
            inline_policies=[read_scripts_document],
        )

        # emr job flow role
        emr_job_flow_role = iam.Role(
            self,
            "emr_job_flow_role",
            assumed_by=iam.ServicePrincipal("ec2.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AmazonElasticMapReduceforEC2Role"
                )
            ],
        )
        # emr job flow profile
        emr_job_flow_profile = iam.CfnInstanceProfile(
            self,
            "emr_job_flow_profile",
            roles=[emr_job_flow_role.role_name],
            instance_profile_name="emrJobFlowProfile",
        )

        # # create emr cluster
        # emr.CfnCluster(
        #     self,
        #     "emr_cluster",
        #     instances=emr.CfnCluster.JobFlowInstancesConfigProperty(
        #         core_instance_group=emr.CfnCluster.InstanceGroupConfigProperty(
        #             instance_count=2, instance_type="m5.xlarge", market="ON_DEMAND"
        #         ),
        #         ec2_subnet_id=common_stack.custom_vpc.public_subnets[0].subnet_id,
        #         hadoop_version="Amazon",
        #         keep_job_flow_alive_when_no_steps=False,
        #         termination_protected=False,
        #         master_instance_group=emr.CfnCluster.InstanceGroupConfigProperty(
        #             instance_count=1, instance_type="m5.xlarge", market="ON_DEMAND"
        #         ),
        #     ),
        #     bootstrap_actions=[
        #         emr.CfnCluster.BootstrapActionConfigProperty(
        #             name="install_python_libraries",
        #             script_bootstrap_action=emr.CfnCluster.ScriptBootstrapActionConfigProperty(
        #                 path="s3://script-bucket-production-034832733803-us-east-1/bootstrap_emr.sh"
        #             ),
        #         )
        #     ],
        #     # note job_flow_role is an instance profile (not an iam role)
        #     job_flow_role=emr_job_flow_profile.ref,
        #     name=f"emr-cluster-{self.deploy_env}",
        #     applications=[emr.CfnCluster.ApplicationProperty(name="Spark")],
        #     service_role=emr_service_role.role_name,
        #     configurations=[
        #         # use python3 for pyspark
        #         emr.CfnCluster.ConfigurationProperty(
        #             classification="spark-env",
        #             configurations=[
        #                 emr.CfnCluster.ConfigurationProperty(
        #                     classification="export",
        #                     configuration_properties={
        #                         "PYSPARK_PYTHON": "/usr/bin/python3",
        #                         "PYSPARK_DRIVER_PYTHON": "/usr/bin/python3",
        #                     },
        #                 )
        #             ],
        #         ),
        #         # enable apache arrow
        #         emr.CfnCluster.ConfigurationProperty(
        #             classification="spark-defaults",
        #             configuration_properties={
        #                 "spark.sql.execution.arrow.enabled": "true"
        #             },
        #         ),
        #         # dedicate cluster to single jobs
        #         emr.CfnCluster.ConfigurationProperty(
        #             classification="spark",
        #             configuration_properties={"maximizeResourceAllocation": "true"},
        #         ),
        #     ],
        #     log_uri=f"s3://{self.s3_log_bucket.bucket_name}/elasticmapreduce/",
        #     release_label="emr-6.0.0",
        #     visible_to_all_users=True,
        #     # the job to be done
        #     steps=[
        #         emr.CfnCluster.StepConfigProperty(
        #             hadoop_jar_step=emr.CfnCluster.HadoopJarStepConfigProperty(
        #                 jar="command-runner.jar",
        #                 args=[
        #                     "spark-submit",
        #                     "--deploy-mode",
        #                     "cluster",
        #                     f"s3://{self.s3_script_bucket.bucket_name}/{spark_script}",
        #                 ],
        #             ),
        #             name="bronze_to_silver",
        #             action_on_failure="CONTINUE",
        #         ),
        #     ],
        # )
