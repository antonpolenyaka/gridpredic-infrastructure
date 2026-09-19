#!/bin/sh
set -eu

SQLCMD="/opt/mssql-tools18/bin/sqlcmd"

"$SQLCMD" \
    -S localhost \
    -U sa \
    -P "$MSSQL_SA_PASSWORD" \
    -C \
    -b \
    -Q "
        SET NOCOUNT ON;

        IF (
            SELECT COUNT(*)
            FROM sys.databases
            WHERE name IN (
                'Calser_EOSA',
                'Calser_Pitarch',
                'Calser_ValleSantaAna',
                'TedisNet_EOSA'
            )
            AND state_desc = 'ONLINE'
        ) <> 4
            THROW 50000, 'Application databases are not ready', 1;

        USE TedisNet_EOSA;

        IF (
            SELECT COUNT(*)
            FROM sys.tables
            WHERE name IN (
                'SystemBranches',
                'SystemChanges',
                'SystemCommandExecutions',
                'SystemDevices',
                'SystemDeviceStates',
                'SystemElectricElectricalBranches',
                'SystemElectricElectricalCoils',
                'SystemElectricElectricalLVLines',
                'SystemElectricElectricalLVSubscribers',
                'SystemElectricElectricalNodes',
                'SystemElectricElectricalSwitchTerminals',
                'SystemElectricElectricalTransformers',
                'SystemElectricPowerCutElementEvents',
                'SystemElectricPowerCutElementIntervals',
                'SystemElectricPowerCutElementStates',
                'SystemElectricPowerCutEvents',
                'SystemElectricPowerCutIncidents',
                'SystemElectricPowerCutIntervals',
                'SystemElectricProfilePowers',
                'SystemElements',
                'SystemEvents',
                'SystemNetworks',
                'SystemNodes',
                'SystemOperations',
                'SystemTagIntervalSummaries',
                'SystemTagIntervalValues',
                'SystemTagIntervalValuesBig',
                'SystemTags',
                'SystemTagScales',
                'SystemTagValueChanges',
                'SystemTagValues'
            )
            AND schema_id = SCHEMA_ID('dbo')
            AND is_tracked_by_cdc = 1
        ) <> 31
            THROW 50000, 'CDC tables are not ready', 1;
    " \
    >/dev/null 2>&1