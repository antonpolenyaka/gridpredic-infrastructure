USE master;
GO

IF DB_ID('Calser_EOSA') IS NULL
BEGIN
    RESTORE DATABASE Calser_EOSA
    FROM DISK = '/var/opt/mssql/backups/Calser_DIST_1_14082026.bak'
    WITH
        MOVE 'Calser_EOSA'
            TO '/var/opt/mssql/data/Calser_EOSA.mdf',
        MOVE 'Calser_EOSA_log'
            TO '/var/opt/mssql/data/Calser_EOSA_log.ldf',
        RECOVERY;
END
GO

IF DB_ID('Calser_Pitarch') IS NULL
BEGIN
    RESTORE DATABASE Calser_Pitarch
    FROM DISK = '/var/opt/mssql/backups/Calser_DIST_2_14082026.bak'
    WITH
        MOVE 'Calser_Pitarch'
            TO '/var/opt/mssql/data/Calser_Pitarch.mdf',
        MOVE 'Calser_Pitarch_log'
            TO '/var/opt/mssql/data/Calser_Pitarch_log.ldf',
        RECOVERY;
END
GO

IF DB_ID('Calser_ValleSantaAna') IS NULL
BEGIN
    RESTORE DATABASE Calser_ValleSantaAna
    FROM DISK = '/var/opt/mssql/backups/Calser_DIST_3_14082026.bak'
    WITH
        MOVE 'Calser_ValleSantaAna'
            TO '/var/opt/mssql/data/Calser_ValleSantaAna.mdf',
        MOVE 'Calser_ValleSantaAna_log'
            TO '/var/opt/mssql/data/Calser_ValleSantaAna_log.ldf',
        RECOVERY;
END
GO

IF DB_ID('TedisNet_EOSA') IS NULL
BEGIN
    RESTORE DATABASE TedisNet_EOSA
    FROM DISK = '/var/opt/mssql/backups/TedisNet_EOSA_backup_2026_08_14_020001_2902307.bak'
    WITH
        MOVE 'TedisNet_EOSA'
            TO '/var/opt/mssql/data/TedisNet_EOSA.mdf',
        MOVE 'Historic'
            TO '/var/opt/mssql/data/TedisNet_EOSA_Historic.ndf',
        MOVE 'Simula'
            TO '/var/opt/mssql/data/TedisNet_EOSA_Simula.ndf',
        MOVE 'TedisNet_EOSA_log'
            TO '/var/opt/mssql/data/TedisNet_EOSA_log.ldf',
        RECOVERY;

    ALTER AUTHORIZATION
        ON DATABASE::TedisNet_EOSA
        TO sa;
END
GO