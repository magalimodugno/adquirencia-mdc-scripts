import argparse
from datetime import datetime
import time
import logging

try:
    import boto3
    import pandas as pd
    from botocore.exceptions import ClientError
except Exception:
    raise ImportError("Run 'pip install -r requirements.txt'")

# Set up logging
log_filename = f'logfile_{datetime.now().strftime("%Y-%m-%d_%H:%M")}.log'
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger()

# Parse Arguments from file
parser = argparse.ArgumentParser(
    description="Popular tabla que contiene los valores de CFT (costo financiero total)")
parser.add_argument("-f", "--file", required=True, help="archivo CSV de entrada")
parser.add_argument("-env", default="prod", choices=['dev', 'stage', 'prod'], help="ambiente de ejecución")
parser.add_argument("-cuota", default="Cuote",
                    help="nombre de la columna que contiene el nombre y cantidad de la cuota")
parser.add_argument("-valor", default="Valor", help="nombre de la columna que contiene el valor de CFT de la cuota")

args = parser.parse_args()
filename = args.file
env = args.env
key_column = args.cuota
value_column = args.valor

# Create AWS session
profile_name = f"uala-arg-adquirencia-{env}"
session = boto3.Session(profile_name=profile_name, region_name="us-east-1")

# Generate dynamodb clients
dynamodb = session.resource("dynamodb")
table_name = f"{env}-AdquirenciaCft"
table_key_name = 'Cuote'
table_value_name = 'Cft'

# Read CSV file
df = pd.read_csv(filename, dtype={
    key_column: str,
    value_column: str,
})

logging.info("Ejecutando script para popular tabla que contiene los cfts")

BATCH_SIZE = 25
lots = [df[i:i + BATCH_SIZE] for i in range(0, len(df), BATCH_SIZE)]

# Get all existing items in the table to perform an incremental update
table = dynamodb.Table(table_name)
existing_items = table.scan()["Items"]
existing_items_dict = {item[table_key_name]: item for item in existing_items}

# Determine which items to keep and which to delete
csv_items_set = set(df[key_column])
existing_items_set = set(existing_items_dict.keys())

# Identify items to delete (those in DynamoDB but not in the CSV file)
items_to_delete = existing_items_set - csv_items_set

if items_to_delete:
    logging.info(f"Se eliminarán {len(items_to_delete)} elementos.")
    delete_requests = [{
        'DeleteRequest': {
            'Key': {
                table_key_name: cuota
            }
        }
    } for cuota in items_to_delete]

    # Perform batch delete
    try:
        for i in range(0, len(delete_requests), BATCH_SIZE):
            batch = delete_requests[i:i + BATCH_SIZE]
            response = dynamodb.batch_write_item(RequestItems={table_name: batch})
            if response.get('UnprocessedItems'):
                logging.error(f"Algunos elementos no fueron eliminados: {response['UnprocessedItems']}")
            else:
                logging.info(f"{len(batch)} elementos eliminados correctamente.")
    except ClientError as e:
        logging.error(f"Error eliminando elementos: {e.response['Error']['Message']}")
else:
    logging.info("No hay elementos para eliminar.")

for idx, lot in enumerate(lots):
    requests = lot.apply(lambda row: {
        'PutRequest': {
            'Item': {
                table_key_name: row[key_column],
                table_value_name: row[value_column],
            }
        }
    }, axis=1).tolist()

    logging.info(f'Se agregarán {len(requests)} elementos.')

    try:
        res = dynamodb.batch_write_item(
            RequestItems={
                table_name: requests
            }
        )
        if res.get('UnprocessedItems'):
            logging.error(f"Batch {idx + 1} tiene elementos sin procesar: {res['UnprocessedItems']}")
        else:
            logging.info(f"Items procesados correctamente.")

    except ClientError as e:
        error_message = e.response['Error']['Message']
        logging.error(f"Error procesando batch {idx + 1}: {error_message}")

    time.sleep(2)

exit(0)
