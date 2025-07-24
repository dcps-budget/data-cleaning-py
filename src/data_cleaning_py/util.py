import os
import sys


def envs_expected() -> list[str]:
    return [
        "YEAR_FISCAL",
        "QB_API",
        "QB_QBREALMHOSTNAME",
        "QB_AUTHORIZATION",
        "QB_TABLEID_BALANCES",
    ]


def get_config(envs_expected: list[str]) -> dict[str, str]:
    config = {}
    for env in envs_expected:
        if (os.getenv(env) is None) or (os.getenv(env) == ""):
            sys.exit(f"Environment variable {env} is missing from .env file.")
        else:
            config[env] = str(os.getenv(env))

    return config
