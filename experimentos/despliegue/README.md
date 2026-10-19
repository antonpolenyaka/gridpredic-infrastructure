# Coste del despliegue en la nube

`coste_nube.py` calcula el coste mensual de una distribuidora tipo en IaaS y PaaS de OCI, el de una plataforma multicliente y lo que paga la partida de nube del plan financiero (3.7 y 3.9 de la memoria), con el extracto fechado de precios de `precios_nube.csv` (OCI en euros del 08.10.2026; AWS Europe (Spain) en dólares del 11.09 al 18.10.2026):

    python experimentos\despliegue\coste_nube.py

Deja `salida/coste_nube.txt`. El extracto lo saca `extraer_precios.py` de las listas públicas completas (las URL están en su cabecera); para repetir el cálculo con precios de otra fecha se descargan de nuevo y se vuelve a lanzar.
