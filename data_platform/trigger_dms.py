import boto3

dms = boto3.client("dms")
replication_tasks = dms.describe_replication_tasks()["ReplicationTasks"]
task_arn = [
    each["ReplicationTaskArn"]
    for each in replication_tasks
    if each["ReplicationTaskIdentifier"] == "production-dms-task-ecommerce-rds"
]
dms.start_replication_task(
    ReplicationTaskArn=task_arn[0],
    StartReplicationTaskType="reload-target",
)
