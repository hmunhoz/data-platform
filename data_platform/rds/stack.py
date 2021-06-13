from aws_cdk import core
from aws_cdk import aws_rds as rds, aws_ec2 as ec2
from data_platform.active_environment import active_environment
from data_platform.definitions import db_name, db_password, db_username


class RDSStack(core.Stack):
    def __init__(self, scope: core.Construct, vpc: ec2.Vpc, **kwargs):
        self.deploy_env = active_environment
        self.custom_vpc = vpc
        super().__init__(scope, id=f"{self.deploy_env.value}-rds-stack", **kwargs)

        # Security Group
        self.sg_ecommerce_rds = ec2.SecurityGroup(
            self,
            id=f"sg-rds-ecommerce-{self.deploy_env.value}",
            vpc=self.custom_vpc,
            allow_all_outbound=True,
            security_group_name=f"rds-ecommerce-{self.deploy_env.value}-sg",
        )

        # Security Group Ingress Rules (Public)
        self.sg_ecommerce_rds.add_ingress_rule(
            peer=ec2.Peer.ipv4("0.0.0.0/0"), connection=ec2.Port.tcp(5432)
        )

        for subnet in self.custom_vpc.private_subnets:
            self.sg_ecommerce_rds.add_ingress_rule(
                peer=ec2.Peer.ipv4(subnet.ipv4_cidr_block),
                connection=ec2.Port.tcp(5432),
            )

        # Parameter Group - parameters to read rds with dms
        self.ecommerce_rds_parameter_group = rds.ParameterGroup(
            self,
            id=f"ecommerce-{self.deploy_env.value}-rds-parameter-group",
            description="Parameter group to allow CDC from RDS using DMS.",
            engine=rds.DatabaseInstanceEngine.postgres(
                version=rds.PostgresEngineVersion.VER_12_4
            ),
            parameters={"rds.logical_replication": "1", "wal_sender_timeout": "0"},
        )

        # Define Credentials
        # Definitely Not best practice, as we should use secrets manager
        # But we want to avoid extra costs in this demonstration
        self.rds_credentials = rds.Credentials.from_password(
            username=db_username, password=core.SecretValue.plain_text(db_password)
        )

        # Postgres DataBase Instance
        self.ecommerce_rds = rds.DatabaseInstance(
            self,
            id=f"rds-ecommerce-{self.deploy_env.value}",
            database_name=db_name,
            engine=rds.DatabaseInstanceEngine.postgres(
                version=rds.PostgresEngineVersion.VER_12_4
            ),
            credentials=self.rds_credentials,
            instance_type=ec2.InstanceType("t3.micro"),
            vpc=self.custom_vpc,
            instance_identifier=f"rds-{self.deploy_env.value}-ecommerce-db",
            port=5432,
            vpc_placement=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC),
            subnet_group=rds.SubnetGroup(
                self,
                f"rds-{self.deploy_env.value}-subnet",
                description="place RDS on public subnet",
                vpc=self.custom_vpc,
                vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC),
            ),
            parameter_group=self.ecommerce_rds_parameter_group,
            security_groups=[self.sg_ecommerce_rds],
            removal_policy=core.RemovalPolicy.DESTROY,
            **kwargs,
        )

        self._rds_host = self.ecommerce_rds.db_instance_endpoint_address

        self._rds_port = self.ecommerce_rds.db_instance_endpoint_port

        @property
        def rds_endpoint_address(self):
            return self._rds_host

        @property
        def rds_endpoint_port(self):
            return self._rds_port
