import os


def path_budget() -> str:
    return os.path.join(
        "C:",
        os.sep,
        "Users",
        str(os.environ.get("USERNAME")),
        "OneDrive - District of Columbia Public Schools",
        "DCPS Budget - Data",
        "Budget",
    )


def path_025() -> str:
    return os.path.join(path_budget(), "R025")


def path_209() -> str:
    return os.path.join(path_budget(), "R209")
