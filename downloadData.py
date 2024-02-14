import os
import pandas as pd

from sodapy import Socrata

socrata_token = "WgGJrycrk7Ry5vYQZ8otnPhDL"
# $env:SODAPY_APPTOKEN="WgGJrycrk7Ry5vYQZ8otnPhDL"
# set SODAPY_APPTOKEN="WgGJrycrk7Ry5vYQZ8otnPhDL"
# socrata_token = os.environ.get("SODAPY_APPTOKEN")

# Enter the information from those sections here
socrata_domain = "data.cityofchicago.org"
socrata_dataset_identifier = "sxs8-h27x"

# Crear la carpeta "batches" si no existe
batch_folder = "batches"
os.makedirs(batch_folder, exist_ok=True)

# Get all the data
client = Socrata(socrata_domain, socrata_token, timeout=60)

# Parámetros de paginación
offset = 0
batch_number = 1
batch_size = 250000  # Tamaño de cada lote de datos

while True:
    print("Batch: ", batch_number)
    batch = client.get(
        socrata_dataset_identifier,
        where="segment_id >=141 AND segment_id < 142 AND speed>=0",
        select="time, segment_id, speed, street, from_street, to_street, length, hour, day_of_week, month, start_latitude, start_longitude, end_latitude, end_longitude, start_location, end_location",
        limit=batch_size,
        offset=offset
    )

    # Guardar el lote actual en un archivo parquet en la carpeta "batches"
    batch_df = pd.DataFrame.from_dict(batch)
    batch_parquet_path = os.path.join(batch_folder, f"seg-141_Batch{batch_number}.parquet")

    batch_df.to_parquet(batch_parquet_path, index=False)

    print(batch_number, " finished")
    batch_number += 1
    offset += batch_size

    # Si la longitud del lote es menor que el tamaño del lote, hemos alcanzado el final de los datos
    if len(batch) < batch_size:
        break

print("TERMINADO")