"""
Settings shared by every test that starts a local Spark.

The JVM of PySpark takes its heap from PYSPARK_SUBMIT_ARGS when it starts,
not from the SparkSession builder (in a process with several sessions the
first one fixes it for all). With the default of 1 GB the Gold tests run
out of heap now and then on the CI runners (dim_ct builds and broadcasts
several tables), so the heap is raised here before any session exists; a
value already in the environment is respected.
"""

import os

if "PYSPARK_SUBMIT_ARGS" not in os.environ:
    os.environ["PYSPARK_SUBMIT_ARGS"] = "--driver-memory 3g pyspark-shell"
