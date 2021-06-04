import os
from enum import Enum


class Environment(Enum):
    DEVELOP = "develop"
    STAGING = "staging"
    PRODUCTION = "production"


active_environment = Environment[os.environ["ENVIRONMENT"]]
