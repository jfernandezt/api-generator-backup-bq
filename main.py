from datetime import datetime
from google.cloud import bigquery, storage
import pandas as pd
import json
import numpy as np

def get_table_list(client, project_id, dataset_id, table_list_table):
    query = f"""
    SELECT id_tables_backup, project_name, dataset_name, table_name
    FROM `{project_id}.{dataset_id}.{table_list_table}`
    where status = 'ACTIVE'
    """
    query_job = client.query(query)
    results = query_job.result()
    
    row_iterator = results

    df = row_iterator.to_dataframe()

    return df


def create_bucket_folder(bucket_name, folder_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(f"{folder_name}/")
    if not blob.exists():
        blob.upload_from_string('')
        print(f"Created folder :: {folder_name} in bucket {bucket_name}")


def get_next_id(project_id, dataset_id, row_counts_table):
    query = f"""
    SELECT IFNULL(MAX(id_tables_row_counts), 0) + 1 AS next_id
    FROM `{project_id}.{dataset_id}.{row_counts_table}`
    """
    
    client = bigquery.Client(project=project_id)

    query_job = client.query(query, location="US")  # Especifica la ubicación aquí
    result = query_job.result()
    
    for row in result:
        return row.next_id
    

def insert_tables_backup(project_id_principal, dataset_id_principal, project_id, dataset_id, table_id, table_row_counts, id_tables_backup, process_number):
    
    client = bigquery.Client(project=project_id_principal)

    id_tables_rows_counts = get_next_id(project_id_principal, dataset_id_principal, table_row_counts)

    #print("id_tables_rows_counts :: ", id_tables_rows_counts)

    user_created = 'data'

    #Contar registros de tabla
    row_counts = count_table_rows(client, project_id, dataset_id, table_id)

    print("row_counts :: ", row_counts)

    table_insert = f"{project_id_principal}.{dataset_id_principal}.{table_row_counts}"

    rows_to_insert = [
    {
        'id_tables_row_counts': int(id_tables_rows_counts), 
        'id_tables_backup': int(id_tables_backup),
        'process_number': int(process_number),
        'project_name': project_id,
        'dataset_name': dataset_id,
        'table_name': table_id,
        'row_count': int(row_counts),
        'status_backup': 'PENDING',
        'date_created': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'user_created': user_created
    }
    ]

    #print("rows_to_insert :: ", rows_to_insert)
    
    errors = client.insert_rows_json(table_insert, rows_to_insert)

    #print("errors :: ", errors)
    if errors:
        print("Error inserting rows:", errors)
    #else:
        #print("Row counts successfully saved to BigQuery.")

    return id_tables_rows_counts

'''
def update_status_backup(project_id, dataset_id, row_counts_table, id_tables_row_counts, path_backup, status_backup):
    client = bigquery.Client(project=project_id)

    query = f"""
    UPDATE `{project_id}.{dataset_id}.{row_counts_table}`
    SET path_backup = '{path_backup}',
        status_backup = '{status_backup}'
    WHERE id_tables_row_counts = {id_tables_row_counts}
    """

    print("query :: ", query)

    query_job = client.query(query)
    query_job.result()
    
    return query_job.num_dml_affected_rows


  
def update_status_backup(project_id, dataset_id, row_counts_table, id_tables_row_counts, path_backup, status_backup):
    
    client = bigquery.Client(project=project_id)

    # Descargar la tabla actual a un DataFrame de pandas
    query = f"SELECT * FROM `{project_id}.{dataset_id}.{row_counts_table}`"
    df = client.query(query).to_dataframe()

    # Actualizar los campos en el DataFrame
    df.loc[df['id_tables_row_counts'] == id_tables_row_counts, 'path_backup'] = path_backup
    df.loc[df['id_tables_row_counts'] == id_tables_row_counts, 'status_backup'] = status_backup

    # Crear una nueva tabla con los datos actualizados
    table_id = f"{project_id}.{dataset_id}.tables_row_counts_nueva"
    job = client.load_table_from_dataframe(df, table_id)
    job.result()  # Esperar a que el trabajo de carga termine

    # Eliminar la tabla original
    client.delete_table(f"{project_id}.{dataset_id}.{row_counts_table}")

    # Renombrar la nueva tabla
    consulta_renombrar_tabla = f"ALTER TABLE `{project_id}.{dataset_id}.tables_row_counts_nueva` RENAME TO `{row_counts_table}`;"
    client.query(consulta_renombrar_tabla).result()

    print("Actualización realizada exitosamente")


def update_status_backup(project_id, dataset_id, row_counts_table, id_tables_row_counts, path_backup, status_backup):
    client = bigquery.Client(project=project_id)

    # Paso 1: Descargar la tabla actual a un DataFrame
    query = f"SELECT * FROM `{project_id}.{dataset_id}.{row_counts_table}`"
    df = client.query(query).to_dataframe()

    # Paso 2: Actualizar los campos en el DataFrame
    df.loc[df['id_tables_row_counts'] == id_tables_row_counts, 'path_backup'] = path_backup
    df.loc[df['id_tables_row_counts'] == id_tables_row_counts, 'status_backup'] = status_backup

    # Paso 3: Crear una tabla temporal con los datos actualizados
    temp_table_id = f"{project_id}.{dataset_id}.{row_counts_table}_temp"
    job = client.load_table_from_dataframe(df, temp_table_id)
    job.result()  # Esperar a que el trabajo de carga termine

    # Paso 4: Reemplazar la tabla original por la tabla temporal
    client.query(f"DROP TABLE `{project_id}.{dataset_id}.{row_counts_table}`").result()
    client.query(f"ALTER TABLE `{temp_table_id}` RENAME TO `{row_counts_table}`").result()

    print("Update :: Successfully")
'''

def export_tables_to_gcs(project_id, dataset_name, bucket_name, folder_name, table_name, process_number):

    create_bucket_folder(bucket_name, folder_name)

    destination_uri = f"gs://{bucket_name}/{folder_name}/{table_name}_{process_number}.parquet"

    client = bigquery.Client(project=project_id)
    dataset_ref = client.dataset(dataset_name)
    table_ref = dataset_ref.table(table_name)
    
    extract_job = client.extract_table(
        table_ref,
        destination_uri,
        location="US",  # Ubicación de la tabla
        job_config=bigquery.job.ExtractJobConfig(destination_format="PARQUET")
    )

    extract_job.result()  # Esperar a que el trabajo finalice
    print(f"Exported :: {dataset_name}.{table_name} to {destination_uri}")

    return destination_uri


def count_table_rows(client, project_id, dataset_id, table_id):
    query = f"""
    SELECT COUNT(*) as row_count
    FROM `{project_id}.{dataset_id}.{table_id}`
    """
    query_job = client.query(query)
    result = query_job.result().to_dataframe()

    return result['row_count'][0]


def save_row_counts(project_id_principal, dataset_id_principal, bucket_name, table_row_counts, tables):
    
    #print(" :: Inicio save_row_counts ::")

    for index, row in tables.iterrows():
        print("##############################################")
        id_tables_backup = row['id_tables_backup']
        project_id = row['project_name']
        dataset_id = row['dataset_name']
        table_id = row['table_name']
        process_number = datetime.utcnow().strftime('%Y%m%d%H%M%S')
        
        #print("project_id :: ", project_id)
        #print("dataset_id :: ", dataset_id)
        print("table_id :: ", table_id)

        #Guardar datos en tabla de configuración
        id_tables_rows_counts = insert_tables_backup(project_id_principal, dataset_id_principal, project_id, dataset_id, table_id, table_row_counts, id_tables_backup, process_number)

        #Guardar backup de tablas en GCS
        path_backup = export_tables_to_gcs(project_id, dataset_id, bucket_name, dataset_id, table_id, process_number)

        #Actualizar status de backup
        #update_status_backup(project_id_principal, dataset_id_principal, row_counts_table, id_tables_rows_counts, path_backup, "SUCCESS")
        
        print("##############################################")

def main():
    #Datos para leer y guardar datos en tablas de configuración
    project_id_principal = 'resolute-bloom-451602-h1'
    dataset_id_principal = 'prd_proceso_backup_bq'
    table_list_table = 'tables_backup'
    table_row_counts = 'tables_row_counts'
    bucket_name = 'prd-backup-tables-bq'
    
    client = bigquery.Client(project=project_id_principal)
    
    print("############")
    print(":: INICIO ::")
    print("############")

    project_id = 'resolute-bloom-451602-h1'
    tables = get_table_list(client, project_id_principal, dataset_id_principal, table_list_table)
    
    #print("tables :: ", tables)

    save_row_counts(project_id_principal, dataset_id_principal, bucket_name, table_row_counts, tables)


if __name__ == "__main__":
    main()