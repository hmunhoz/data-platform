from airflow import DAG

from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor

# from airflow.providers.amazon.aws.operators.emr_create_job_flow import EmrCreateJobFlowOperator
# from airflow.providers.amazon.aws.operators.emr_add_steps import EmrAddStepsOperator
# from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor
#
from airflow.utils.dates import days_ago
from datetime import timedelta
import os
import boto3

DAG_ID = os.path.basename(__file__).replace(".py", "")

# Get Resources
# Cloudformation client
cf = boto3.client('cloudformation')
# Get subnets
common_stack = cf.describe_stack_resources(
    StackName='production-common-stack'
)
common_resources = common_stack['StackResources']
subnets = [resource for resource in common_resources if resource['ResourceType'] == 'AWS::EC2::Subnet' if 'PublicSubnet' in resource['LogicalResourceId']]
public_subnet = subnets[0]['PhysicalResourceId']

# Get pyspark script bucket
buckets = [resource for resource in common_resources if resource['ResourceType'] == 'AWS::S3::Bucket' if 'script-bucket-production-' in resource['PhysicalResourceId']]
bucket = buckets[0]['PhysicalResourceId']

spark_script_path = f's3://{bucket}/' + 'bronze_to_silver_processing.py'

# EMR Roles
emr_stack = cf.describe_stack_resources(
    StackName='production-emr-stack'
)
emr_resources = emr_stack['StackResources']
emr_roles = [resource for resource in emr_resources if resource['ResourceType'] == 'AWS::IAM::Role']

# EMR ServiceRole
service_roles = [role for role in emr_roles if 'emrservicerole' in role['LogicalResourceId']]
service_role = service_roles[0]['PhysicalResourceId']

# EMR JobFlowRole
job_flow_roles = [role for role in emr_roles if 'emrjobflowrole' in role['LogicalResourceId']]
job_flow_role = service_roles[0]['PhysicalResourceId']


DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
}

JOB_FLOW_OVERRIDES = {
    'Name': 'emr-demo-bronze-silver-cluster',
    'ReleaseLabel': 'emr-5.30.1',
    'Applications': [
        {
            'Name': 'Spark'
        },
    ],
    'Instances': {
        'InstanceGroups': [
            {
                'Name': "Master nodes",
                'Market': 'ON_DEMAND',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 1,
            },
            {
                'Name': "Slave nodes",
                'Market': 'ON_DEMAND',
                'InstanceRole': 'CORE',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 2,
            }
        ],
        'KeepJobFlowAliveWhenNoSteps': False,
        'TerminationProtected': False,
        'Ec2SubnetId': public_subnet,
    },
    'BootstrapActions': [
        {
            'BootstrapActionConfig': {
                'Name': 'install_python_libraries',
                'ScriptBootstrapAction': {
                    'Path': 's3://script-bucket-production-034832733803-us-east-1/bootstrap_emr.sh',
                }
            }
        },
    ],
    'VisibleToAllUsers': True,
    'JobFlowRole': job_flow_role,
    'ServiceRole': service_role
}

SPARK_STEPS = [
    {
        'Name': 'bronze_to_silver_processing',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit', '--deploy-mode', 'cluster', spark_script_path],
        },
    }
]

with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=2),
    start_date=days_ago(1),
    schedule_interval='@once',
    tags=['emr'],
) as dag:

    cluster_creator = EmrCreateJobFlowOperator(
        task_id='create_job_flow',
        emr_conn_id='aws_default',
        job_flow_overrides=JOB_FLOW_OVERRIDES
    )

    step_adder = EmrAddStepsOperator(
        task_id='add_steps',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
        aws_conn_id='aws_default',
        steps=SPARK_STEPS,
    )

    step_checker = EmrStepSensor(
        task_id='watch_step',
        job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[0] }}",
        aws_conn_id='aws_default',
    )

    cluster_creator >> step_adder >> step_checker
