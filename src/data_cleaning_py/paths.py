import os


def path_budget():
    return os.path.join(
        "C:",
        os.sep,
        "Users",
        str(os.environ.get("USERNAME")),
        "OneDrive - District of Columbia Public Schools",
        "DCPS Budget - Data",
        "Budget",
    )


def path_025():
    return os.path.join(path_budget(), "R025")


def path_209():
    return os.path.join(path_budget(), "R209")
