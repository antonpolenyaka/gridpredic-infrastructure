"""
Every Spark job runs in UTC. SQL Server keeps local wall-clock times without
a zone and Bronze copies them as they come: the JDBC reader turns them into
instants with the zone of the JVM and Debezium sends them as if they were
UTC. The two paths only agree when the JVM and the session are in UTC, so
infra/spark/conf/spark-defaults.conf fixes it for the driver, the executors
and the session, and the Gold and ML jobs set the session again. The test
reads the files, without Spark or Docker.

    python -m pytest tests/03_gold/test_spark_utc.py -q
"""

import os
import re

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


def read(*parts) -> str:
    with open(os.path.join(ROOT, *parts), encoding="utf-8") as file:
        return file.read()


def settings(conf: str) -> dict:
    out = {}
    for line in conf.splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            key, _, value = line.partition(" ")
            out[key] = value.strip()
    return out


def test_the_spark_defaults_fix_utc_for_the_session_and_the_jvm():
    conf = settings(read("infra", "spark", "conf", "spark-defaults.conf"))
    assert conf["spark.sql.session.timeZone"] == "UTC"
    assert "-Duser.timezone=UTC" in conf["spark.driver.extraJavaOptions"]
    assert "-Duser.timezone=UTC" in conf["spark.executor.extraJavaOptions"]


def test_the_jobs_that_build_their_own_session_set_utc_too():
    for path in (("etl", "jobs", "03_gold", "gold_common.py"), ("etl", "jobs", "04_ml", "job_ml_train.py")):
        source = read(*path)
        assert re.search(r'spark\.conf\.set\("spark\.sql\.session\.timeZone",\s*"UTC"\)', source), path
