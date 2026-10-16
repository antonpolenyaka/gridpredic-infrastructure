"""
Variables de ERA5 que no teníamos (CDS, reanalysis-era5-single-levels) para la
caja de las redes: el índice K, la precipitación convectiva, la tasa máxima de
precipitación y la racha instantánea a 10 m. Un fichero NetCDF por mes.

La clave es la del CDS de .env (COPERNICUS_EWDS_KEY, que sirve en los dos
sitios); no se imprime.

    python bajar_era5_convectivo.py D:\\DockerData\\era5 2017 2026
"""

import os
import sys
from datetime import date

import cdsapi

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
VARIABLES = ["k_index", "convective_precipitation", "total_precipitation",
             "maximum_total_precipitation_rate_since_previous_post_processing", "instantaneous_10m_wind_gust"]
# Norte, oeste, sur, este: las 43 celdas con medio grado de margen.
AREA = [40.75, -7.5, 38.0, -5.25]


def key() -> str:
    with open(os.path.join(ROOT, ".env"), encoding="utf-8") as handle:
        for line in handle:
            if line.strip().startswith("COPERNICUS_EWDS_KEY="):
                return line.split("=", 1)[1].strip().strip('"').strip("'")
    raise SystemExit("Falta COPERNICUS_EWDS_KEY en .env")


def main(argv):
    folder, first, last = argv[0], int(argv[1]), int(argv[2])
    os.makedirs(folder, exist_ok=True)
    client = cdsapi.Client(url="https://cds.climate.copernicus.eu/api", key=key(), quiet=True)
    for year in range(first, last + 1):
        # El año en curso solo hasta el mes pasado: ERA5 va unos días por detrás. Un mes por petición, porque el
        # año entero pasa del límite de tamaño del CDS.
        upto = 12 if year < date.today().year else date.today().month - 1
        for month in range(1, upto + 1):
            target = os.path.join(folder, f"era5_convectivo_{year}_{month:02d}.nc")
            if os.path.exists(target) and os.path.getsize(target) > 0:
                continue
            client.retrieve("reanalysis-era5-single-levels", {
                "product_type": ["reanalysis"], "variable": VARIABLES, "year": [str(year)], "month": [f"{month:02d}"],
                "day": [f"{d:02d}" for d in range(1, 32)], "time": [f"{h:02d}:00" for h in range(24)],
                "area": AREA, "data_format": "netcdf", "download_format": "unarchived"}, target)
            print(year, month, "bajado", round(os.path.getsize(target) / 1e6, 1), "MB", flush=True)

if __name__ == "__main__":
    main(sys.argv[1:])
