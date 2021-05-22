#!/usr/bin/env python3
import os
from aws_cdk import core as cdk

# For consistency with TypeScript code, `cdk` is the preferred import name for
# the CDK's core module.  The following line also imports it as `core` for use
# with examples from the CDK Developer's Guide, which are in the process of
# being updated to use `cdk`.  You may delete this import if you don't need it.
from aws_cdk import core

from data_platform.common_stack import CommonResourcesStack
from data_platform.data_lake.stack import DataLakeStack
from data_platform.rds.stack import RDSStack


app = core.App()
common_stack = CommonResourcesStack(app)
data_lake_stack = DataLakeStack(app)
rds_stack = RDSStack(app, vpc= common_stack.custom_vpc)

app.synth()
