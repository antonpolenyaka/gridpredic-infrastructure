#!/bin/sh
set -eu

mc stat local/datalake >/dev/null 2>&1
mc stat local/spark-events >/dev/null 2>&1