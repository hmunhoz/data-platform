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
from data_platform.dms.stack import DMSStack
from data_platform.glue_catalog.stack import GlueCatalogStack
from data_platform.athena.stack import AthenaStack

app = core.App()
common_stack = CommonResourcesStack(app)
data_lake_stack = DataLakeStack(app)
rds_stack = RDSStack(app, vpc=common_stack.custom_vpc)
dms_stack = DMSStack(
    app,
    common_stack=common_stack,
    rds_stack=rds_stack,
    data_lake_bronze_bucket=data_lake_stack.data_lake_bronze_bucket,
)
glue_stack = GlueCatalogStack(
    app,
    raw_data_lake_bucket=data_lake_stack.data_lake_bronze_bucket,
    staged_data_lake_bucket=data_lake_stack.data_lake_silver_bucket,
)
athena_stack = AthenaStack(app)

app.synth()
