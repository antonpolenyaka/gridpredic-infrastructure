"""
The tables that the change capture of TedisNet follows. Three files name them:
enable_cdc.sql turns on CDC in SQL Server, the healthcheck of SQL Server waits
until all of them are tracked and the connector of Debezium publishes them in
Kafka. The three lists have to be the same, and none of them may hold the
tables of low voltage lines and subscribers, which keep the name, the CUPS, the
meter and the address of every subscriber and that no job reads (dataset card,
Personal and Sensitive Information). The test reads the files, without Docker.

    python -m pytest tests/01_bronze/test_cdc_tablas.py -q
"""

import json
import os
import re

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
PERSONAL = {"SystemElectricElectricalLVLines", "SystemElectricElectricalLVSubscribers"}


def read(*parts) -> str:
    with open(os.path.join(ROOT, *parts), encoding="utf-8") as file:
        return file.read()


def sql_names(text: str, variable: str) -> list:
    block = re.search(rf"INSERT INTO {variable} \(name\)\s+VALUES(.*?);", text, re.S)
    assert block, f"enable_cdc.sql has no list {variable}"
    return re.findall(r"\('(\w+)'\)", block.group(1))


def test_the_three_lists_of_the_change_capture_agree():
    sql = read("infra", "sqlserver", "scripts", "enable_cdc.sql")
    enabled = sql_names(sql, "@tables")

    health = read("infra", "sqlserver", "healthcheck.sh")
    tedisnet = health[health.index("USE TedisNet_EOSA;"):]
    checked = re.findall(r"'(\w+)'", re.search(r"WHERE name IN \((.*?)\)", tedisnet, re.S).group(1))
    expected = int(re.search(r"is_tracked_by_cdc = 1\s*\) <> (\d+)", tedisnet).group(1))

    connector = json.loads(read("infra", "kafka-connect", "connectors", "tedisnet-eosa-connector.json"))
    published = [name.strip().split(".", 1)[1] for name in connector["table.include.list"].split(",")]

    assert len(enabled) == len(set(enabled))
    assert set(enabled) == set(checked) == set(published)
    assert expected == len(enabled) == len(published)


def test_no_table_with_data_of_the_subscribers_is_captured():
    sql = read("infra", "sqlserver", "scripts", "enable_cdc.sql")
    connector = json.loads(read("infra", "kafka-connect", "connectors", "tedisnet-eosa-connector.json"))
    published = {name.strip().split(".", 1)[1] for name in connector["table.include.list"].split(",")}

    assert not PERSONAL & set(sql_names(sql, "@tables"))
    assert not PERSONAL & published
    # A database that already had them with CDC loses it the next time SQL Server starts.
    assert set(sql_names(sql, "@excluded")) == PERSONAL
    assert "sys.sp_cdc_disable_table" in sql
