from aws_cdk import core
from aws_cdk import aws_ec2 as ec2
from data_platform.active_environment import active_environment


class CommonResourcesStack(core.Stack):
    def __init__(self, scope: core.Construct, **kwargs):
        self.deploy_env = active_environment
        super().__init__(scope, id=f"{self.deploy_env.value}-common-stack", **kwargs)

        # VPC
        self.custom_vpc = ec2.Vpc(
            self,
            id=f"vpc-{self.deploy_env.value}",
            vpn_gateway=False,
            nat_gateways=0
        )

