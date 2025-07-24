Steps:

1. Create virtual environment.

`python -m venv "<choose directory name>"`

`cd "<your directory name>"`

`scripts/activate`

2. Install from GitHub.

`pip install git+https://github.com/dcps-budget/data-cleaning-py.git`

3. Create a .env file with the following environment variables.

The application won't run without these environment variables defined:
* YEAR_FISCAL
* QB_API
* QB_QBREALMHOSTNAME
* QB_AUTHORIZATION
* QB_TABLEID_BALANCES

Make sure the .env file is excluded from version control!

For more information, see:

https://developer.quickbase.com/operation/runQuery
https://developer.quickbase.com/operation/upsert

4. Run command line utility, which will be installed as part of the package.

`data-cleaning-py`
