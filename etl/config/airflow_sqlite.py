"""
Connection arguments of the SQLite metadata database of Airflow (standalone).

The scheduler, the dag-processor, the triggerer and the API share one SQLite
file, and SQLite lets one writer in at a time: when a transaction of the
scheduler lasts longer than the five seconds that sqlite3 waits by default,
the dag-processor dies with "database is locked" (it happened on 14.10.2026
at 20:42 and at 23:46) and no change in a DAG file is read again until the
container restarts. A longer wait is the standard fix for a single-machine
installation like this one.

Airflow imports this dict through [database] sql_alchemy_connect_args
(AIRFLOW__DATABASE__SQL_ALCHEMY_CONNECT_ARGS = airflow_sqlite.CONNECT_ARGS in
compose.yaml); /opt/airflow/config is on its path.
"""

CONNECT_ARGS = {"timeout": 60, "check_same_thread": False}
