import pandas as pd
import hashlib
from google.cloud import bigquery
from datetime import datetime
import logging
import pytz

# Configuración básica de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("SEIA_ETL")

def generar_id_hash(row):
    """
    Genera el ID único replicando la lógica de Terraform:
    SUBSTR(TO_HEX(MD5(CONCAT(nombre, '|', fecha, '|', titular))), 1, 12)
    """
    nombre = str(row['nombre_proyecto']) if pd.notna(row['nombre_proyecto']) else 'n/a'
    titular = str(row['titular']) if pd.notna(row['titular']) else 'n/a'
    
    # Aseguramos formato consistente de fecha (YYYY-MM-DD HH:MM:SS)
    if pd.notna(row['fecha_presentacion']):
        fecha = str(row['fecha_presentacion']) 
    else:
        fecha = 'n/a'

    raw_string = f"{nombre}|{fecha}|{titular}"
    hash_obj = hashlib.md5(raw_string.encode('utf-8'))
    return hash_obj.hexdigest()[:12]

def procesar_y_cargar_excel(file_path, project_id, dataset_id, table_id="seia_limpio"):
    """
    ETL Estricto: Lee Excel -> Ajusta Esquema -> Carga Temporal -> Merge
    """
    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    temp_table_id = f"{project_id}.{dataset_id}.temp_seia_update"

    logger.info(f"--- INICIO ETL STRICT MODE: {file_path} ---")

    # 1. LECTURA Y RENOMBRADO
    try:
        df = pd.read_excel(file_path)
    except Exception as e:
        logger.error(f"Fallo crítico al leer Excel: {e}")
        return False

    column_mapping = {
        "Nombre del Proyecto": "nombre_proyecto",
        "WEB": "web",
        "Tipo de Presentación": "tipo_presentacion",
        "Región": "region",
        "Comuna": "comuna",
        "Provincia": "provincia",
        "Tipo de Proyecto": "tipo_proyecto",
        "Razón de Ingreso": "razon_ingreso",
        "Titular": "titular",
        "Inversión (MMU$)": "inversion_mmu",
        "Fecha Presentación": "fecha_presentacion",
        "Estado del Proyecto": "estado_proyecto",
        "Fecha Calificación": "fecha_calificacion",
        "Sector Productivo": "sector_productivo",
        "Latitud Punto Representativo": "latitud",
        "Longitud Punto Representativo": "longitud"
    }
    
    df = df.rename(columns=column_mapping)
    
    # Filtrar solo columnas conocidas del Excel
    cols_to_keep = [c for c in column_mapping.values() if c in df.columns]
    df = df[cols_to_keep].copy()

    # 2. LIMPIEZA Y TIPOS DE DATOS
    
    # Fechas a datetime (NaT si falla)
    for col in ['fecha_presentacion', 'fecha_calificacion']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    # Números a float
    for col in ['inversion_mmu', 'latitud', 'longitud']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Eliminar duplicados exactos
    df = df.drop_duplicates()

    # Regla de Negocio: Misma llave (nombre+titular) -> Queda la fecha mayor
    if 'nombre_proyecto' in df.columns and 'titular' in df.columns and 'fecha_presentacion' in df.columns:
        df = df.sort_values(by='fecha_presentacion', ascending=False)
        df = df.drop_duplicates(subset=['nombre_proyecto', 'titular'], keep='first')

    if df.empty:
        logger.warning("Sin registros válidos tras limpieza.")
        return True

    # 3. COMPLETAR ESQUEMA (ID + FOLIO)
    
    # Generar ID
    df['id'] = df.apply(generar_id_hash, axis=1)
    
    # COMPLETAR FOLIO CON NULL (Como pediste)
    df['folio'] = None 
    df['folio'] = df['folio'].astype(object) # Asegurar que pandas lo trate como objeto para ir a STRING

    # Nota: No agregamos fecha_creacion/actualizacion al DF, las inyectamos en el SQL MERGE.

    # 4. CARGA A TEMPORAL CON ESQUEMA EXPLÍCITO
    # Esto es vital para que 'folio' (todo null) se reconozca como STRING y no falle.
    
    schema_definition = [
        bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("folio", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("nombre_proyecto", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("web", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("tipo_presentacion", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("region", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("comuna", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("provincia", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("tipo_proyecto", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("razon_ingreso", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("titular", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("inversion_mmu", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("fecha_presentacion", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("estado_proyecto", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("fecha_calificacion", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("sector_productivo", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("latitud", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("longitud", "FLOAT", mode="NULLABLE"),
    ]

    job_config = bigquery.LoadJobConfig(
        schema=schema_definition, # <--- AQUÍ ESTÁ LA MAGIA PARA QUE NO FALLE
        write_disposition="WRITE_TRUNCATE"
    )

    try:
        job = client.load_table_from_dataframe(df, temp_table_id, job_config=job_config)
        job.result()
        logger.info(f"Datos cargados en tabla temporal: {temp_table_id}")
    except Exception as e:
        logger.error(f"Error cargando tabla temporal (Esquema): {e}")
        return False

    # 5. MERGE (UPSERT)
    
    query_merge = f"""
    MERGE `{table_ref}` T
    USING `{temp_table_id}` S
    ON T.id = S.id
    
    WHEN MATCHED THEN
      UPDATE SET
        nombre_proyecto = S.nombre_proyecto,
        web = S.web,
        tipo_presentacion = S.tipo_presentacion,
        region = S.region,
        comuna = S.comuna,
        provincia = S.provincia,
        tipo_proyecto = S.tipo_proyecto,
        razon_ingreso = S.razon_ingreso,
        titular = S.titular,
        inversion_mmu = S.inversion_mmu,
        fecha_presentacion = S.fecha_presentacion,
        estado_proyecto = S.estado_proyecto,
        fecha_calificacion = S.fecha_calificacion,
        sector_productivo = S.sector_productivo,
        latitud = S.latitud,
        longitud = S.longitud,
        -- Actualizamos fecha de modificación
        fecha_actualizacion = TIMESTAMP(CURRENT_DATETIME('America/Santiago'))
        
    WHEN NOT MATCHED THEN
      INSERT (
        id, folio, nombre_proyecto, web, tipo_presentacion, region, comuna, provincia, 
        tipo_proyecto, razon_ingreso, titular, inversion_mmu, fecha_presentacion, 
        estado_proyecto, fecha_calificacion, sector_productivo, latitud, longitud, 
        fecha_creacion, fecha_actualizacion
      )
      VALUES (
        S.id, S.folio, S.nombre_proyecto, S.web, S.tipo_presentacion, S.region, S.comuna, S.provincia, 
        S.tipo_proyecto, S.razon_ingreso, S.titular, S.inversion_mmu, S.fecha_presentacion, 
        S.estado_proyecto, S.fecha_calificacion, S.sector_productivo, S.latitud, S.longitud, 
        -- Fechas de sistema
        TIMESTAMP(CURRENT_DATETIME('America/Santiago')), 
        TIMESTAMP(CURRENT_DATETIME('America/Santiago'))
      )
    """

    try:
        query_job = client.query(query_merge)
        query_job.result() 
        logger.info("✅ MERGE completado exitosamente.")
        
        client.delete_table(temp_table_id, not_found_ok=True)
        return True
        
    except Exception as e:
        logger.error(f"❌ Error ejecutando MERGE SQL: {e}")
        return False