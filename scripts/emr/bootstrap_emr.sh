#!/bin/bash

# Purpose: EMR bootstrap script
# Author:  Gary A. Stafford (2021-04-05)

# update and install some useful yum packages
sudo yum install -y jq

# install some useful python packages
sudo python3 -m pip install boto3 ec2-metadata unidecode