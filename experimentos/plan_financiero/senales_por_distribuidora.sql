-- Recuento de CT y señales por distribuidora para calibrar la tarifa del plan financiero.
-- Solo lectura. Se ejecuta contra la copia de TedisNet de las tres redes (TedisNet_GridPredic).
-- La base es de collation CS: los nombres de tablas y columnas van con sus mayúsculas exactas.
--
-- Distribuidora = elemento raíz de tipo 115 (DIS). Los Id son los DistributorId de cada Calser:
--   3 -> Distribuidora 1, 2 -> Distribuidora 2, 1366 -> Distribuidora 3.
-- CT = elementos de tipo 145 (TRAFO CT), la clave de cruce con Calser.
-- Señales = tags de SystemTags cuyo elemento cuelga de la distribuidora; aparte, los que tienen
-- serie guardada (StoreInterval) y los tags sin elemento, que Silver asigna por su dispositivo.

SET NOCOUNT ON;

WITH arbol AS (
    SELECT e.Id, e.Id AS RaizId
    FROM dbo.SystemElements AS e
    WHERE e.ElementTypeId = 115 AND e.Id IN (3, 2, 1366)
    UNION ALL
    SELECT h.Id, a.RaizId
    FROM dbo.SystemElements AS h
    JOIN arbol AS a ON h.ParentElementId = a.Id
    WHERE h.ElementTypeId <> 115
)
SELECT
    a.RaizId                                                    AS distribuidora_id,
    COUNT(DISTINCT CASE WHEN e.ElementTypeId = 145 THEN e.Id END) AS ct_trafo,
    COUNT(t.Id)                                                 AS senales,
    SUM(CASE WHEN t.StoreInterval IS NOT NULL AND t.StoreInterval > 0 THEN 1 ELSE 0 END) AS senales_con_serie
FROM arbol AS a
JOIN dbo.SystemElements AS e ON e.Id = a.Id
LEFT JOIN dbo.SystemTags AS t ON t.ElementId = e.Id
GROUP BY a.RaizId
ORDER BY a.RaizId
OPTION (MAXRECURSION 0);

-- Tags sin elemento (estado de comunicación de dispositivos y similares): total, para saber
-- cuánto falta por repartir.
SELECT COUNT_BIG(*) AS senales_sin_elemento
FROM dbo.SystemTags
WHERE ElementId IS NULL;
