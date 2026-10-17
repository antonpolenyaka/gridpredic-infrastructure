USE TedisNet_EOSA;
GO

IF EXISTS (
    SELECT 1
    FROM sys.databases
    WHERE database_id = DB_ID()
      AND is_cdc_enabled = 0
)
    EXEC sys.sp_cdc_enable_db;
GO


DECLARE @tables TABLE (
    name sysname
);

INSERT INTO @tables (name)
VALUES
    ('SystemBranches'),
    ('SystemChanges'),
    ('SystemCommandExecutions'),
    ('SystemDevices'),
    ('SystemDeviceStates'),
    ('SystemElectricElectricalBranches'),
    ('SystemElectricElectricalCoils'),
    ('SystemElectricElectricalNodes'),
    ('SystemElectricElectricalSwitchTerminals'),
    ('SystemElectricElectricalTransformers'),
    ('SystemElectricPowerCutElementEvents'),
    ('SystemElectricPowerCutElementIntervals'),
    ('SystemElectricPowerCutElementStates'),
    ('SystemElectricPowerCutEvents'),
    ('SystemElectricPowerCutIncidents'),
    ('SystemElectricPowerCutIntervals'),
    ('SystemElectricProfilePowers'),
    ('SystemElements'),
    ('SystemEvents'),
    ('SystemNetworks'),
    ('SystemNodes'),
    ('SystemOperations'),
    ('SystemTagIntervalSummaries'),
    ('SystemTagIntervalValues'),
    ('SystemTagIntervalValuesBig'),
    ('SystemTags'),
    ('SystemTagScales'),
    ('SystemTagValueChanges'),
    ('SystemTagValues');


DECLARE @table sysname;

DECLARE table_cursor CURSOR LOCAL FAST_FORWARD FOR
    SELECT name FROM @tables;

OPEN table_cursor;
FETCH NEXT FROM table_cursor INTO @table;

WHILE @@FETCH_STATUS = 0
BEGIN
    IF EXISTS (
        SELECT 1
        FROM sys.tables
        WHERE object_id = OBJECT_ID(N'dbo.' + @table)
          AND is_tracked_by_cdc = 0
    )
        EXEC sys.sp_cdc_enable_table
            @source_schema = N'dbo',
            @source_name = @table,
            @role_name = NULL;

    FETCH NEXT FROM table_cursor INTO @table;
END;

CLOSE table_cursor;
DEALLOCATE table_cursor;
GO


-- Baja tension: las tablas de lineas y de abonados guardan el nombre, el CUPS,
-- el contador y la direccion de cada abonado. Ningun trabajo las lee, asi que
-- no se capturan; en una base que ya las tenia con CDC se desactiva, lo que
-- borra sus tablas de cambios (cdc.dbo_*_CT) pero no toca las de origen.
DECLARE @excluded TABLE (
    name sysname
);

INSERT INTO @excluded (name)
VALUES
    ('SystemElectricElectricalLVLines'),
    ('SystemElectricElectricalLVSubscribers');


DECLARE @excluded_table sysname;

DECLARE excluded_cursor CURSOR LOCAL FAST_FORWARD FOR
    SELECT name FROM @excluded;

OPEN excluded_cursor;
FETCH NEXT FROM excluded_cursor INTO @excluded_table;

WHILE @@FETCH_STATUS = 0
BEGIN
    IF EXISTS (
        SELECT 1
        FROM sys.tables
        WHERE object_id = OBJECT_ID(N'dbo.' + @excluded_table)
          AND is_tracked_by_cdc = 1
    )
        EXEC sys.sp_cdc_disable_table
            @source_schema = N'dbo',
            @source_name = @excluded_table,
            @capture_instance = N'all';

    FETCH NEXT FROM excluded_cursor INTO @excluded_table;
END;

CLOSE excluded_cursor;
DEALLOCATE excluded_cursor;
GO