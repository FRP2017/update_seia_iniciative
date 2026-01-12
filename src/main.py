import os
import sys
import io
import logging
import glob
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from google.cloud import bigquery
from google.cloud import storage
import time
# --- IMPORTAMOS TU ETL ---
import etl_seia

# Variables de entorno
PROJECT_ID = os.environ.get("PROJECT_ID")
DATASET_ID = os.environ.get("DATASET_ID")
BUCKET_NAME = os.environ.get("BUCKET_NAME") # Necesario para subir el log
DOWNLOAD_DIR = "/tmp/downloads_excel" # Único lugar escribible en Cloud Run

if not os.path.exists(DOWNLOAD_DIR):
    os.makedirs(DOWNLOAD_DIR)

# ==========================================
# 1. TU SISTEMA DE LOGGING ROBUSTO
# ==========================================
def obtener_logger():
    log_stream = io.StringIO()
    logger = logging.getLogger("SEIA_Scraper_Job")
    logger.setLevel(logging.INFO)
    
    # Limpiamos handlers anteriores
    if logger.handlers:
        logger.handlers = []

    # 1. Guardar en memoria (para subir el txt al bucket después)
    handler_memoria = logging.StreamHandler(log_stream)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler_memoria.setFormatter(formatter)
    logger.addHandler(handler_memoria)

    # 2. Mandar a consola de Cloud Run (Stackdriver)
    handler_consola = logging.StreamHandler(sys.stdout)
    handler_consola.setFormatter(formatter)
    logger.addHandler(handler_consola)

    return logger, log_stream

# Inicializamos el logger globalmente
logger, log_stream = obtener_logger()

# ==========================================
# 2. FUNCIONES DE APOYO
# ==========================================

def subir_log_al_bucket():
    """Sube el contenido del log actual a GCS para debug."""
    try:
        client = storage.Client(project=PROJECT_ID)
        bucket = client.bucket(BUCKET_NAME)
        nombre_log = f"logs/ejecucion_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        blob = bucket.blob(nombre_log)
        blob.upload_from_string(log_stream.getvalue(), content_type="text/plain")
        print(f"📋 Log subido a: gs://{BUCKET_NAME}/{nombre_log}")
    except Exception as e:
        print(f"❌ Error subiendo log de emergencia: {e}")

def obtener_ultima_fecha_bq():
    client = bigquery.Client(project=PROJECT_ID)
    query = f"SELECT MAX(fecha_presentacion) as max_date FROM `{PROJECT_ID}.{DATASET_ID}.seia_limpio`"
    try:
        query_job = client.query(query)
        results = query_job.result()
        for row in results:
            return row.max_date
    except Exception as e:
        logger.warning(f"No se pudo obtener fecha máxima (usando default): {e}")
        return None

def generar_rangos_mensuales(fecha_inicio, fecha_fin):
    if not fecha_inicio:
        fecha_inicio = date(2013, 1, 1) # Fecha default si la tabla está vacía
    if isinstance(fecha_inicio, datetime):
        fecha_inicio = fecha_inicio.date()
    
    rangos = []
    actual = fecha_inicio
    while actual < fecha_fin:
        siguiente_mes = actual + relativedelta(months=1)
        fin_mes = siguiente_mes - relativedelta(days=1)
        if fin_mes > fecha_fin:
            fin_mes = fecha_fin
        
        # Solo agregamos si hay un rango válido
        if actual <= fin_mes:
            rangos.append((actual, fin_mes))
        
        actual = siguiente_mes
    return rangos

def configurar_driver():
    options = Options()
    # Configuración estricta para Cloud Run (Headless)
    options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')
    options.add_argument(f'--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    
    prefs = {
        "download.default_directory": DOWNLOAD_DIR,
        "download.prompt_for_download": False,
        "directory_upgrade": True
    }
    options.add_experimental_option("prefs", prefs)
    
    driver = webdriver.Chrome(options=options)
    
    # Forzar descarga en modo headless
    driver.execute_cdp_cmd("Page.setDownloadBehavior", {
        "behavior": "allow", 
        "downloadPath": DOWNLOAD_DIR
    })
    
    return driver

def descargar_excel_por_rango(driver, fecha_desde, fecha_hasta):
    url = "https://seia.sea.gob.cl/busqueda/buscarProyecto.php"
    logger.info(f"🔎 Navegando a SEIA para rango: {fecha_desde} - {fecha_hasta}")
    driver.get(url)
    wait = WebDriverWait(driver, 30)

    try:
        f_inicio_str = fecha_desde.strftime("%d/%m/%Y")
        f_fin_str = fecha_hasta.strftime("%d/%m/%Y")

        wait.until(EC.presence_of_element_located((By.ID, "startDateFechaP"))).clear()
        driver.find_element(By.ID, "startDateFechaP").send_keys(f_inicio_str)
        driver.find_element(By.ID, "endDateFechaP").clear()
        driver.find_element(By.ID, "endDateFechaP").send_keys(f_fin_str)
        
        # Click en Buscar
        boton = driver.find_element(By.CSS_SELECTOR, "button.sg-btnForm")
        driver.execute_script("arguments[0].click();", boton)
        
        # Esperar resultados
        time.sleep(5) 
        
        # Verificar si hay resultados o botón de Excel
        try:
            link_excel = wait.until(EC.element_to_be_clickable((By.LINK_TEXT, "Descargar en formato Excel")))
            driver.execute_script("arguments[0].click();", link_excel)
            logger.info("⬇️ Click en descargar Excel...")
        except Exception:
             logger.warning(f"⚠️ No se encontró botón Excel para {fecha_desde}-{fecha_hasta} (Posiblemente sin resultados).")
             return False
        
        # Esperar a que el archivo aparezca en disco
        for i in range(60): # 60 segundos timeout
            files = glob.glob(os.path.join(DOWNLOAD_DIR, "*.xlsx"))
            temp_files = glob.glob(os.path.join(DOWNLOAD_DIR, "*.crdownload"))
            if files and not temp_files:
                logger.info("✅ Archivo descargado completamente.")
                return True
            time.sleep(1)
            
        logger.error("❌ Timeout esperando descarga del archivo.")
        return False

    except Exception as e:
        logger.error(f"❌ Error en scraping Selenium: {e}")
        return False

# ==========================================
# 3. LÓGICA PRINCIPAL
# ==========================================
def ejecutar_proceso():
    logger.info("🚀 INICIANDO JOB DE ACTUALIZACIÓN SEIA")
    
    try:
        ultima_fecha = obtener_ultima_fecha_bq()
        hoy = date.today()
        logger.info(f"📅 Última fecha en BigQuery: {ultima_fecha}")
        logger.info(f"📅 Fecha actual: {hoy}")
        
        rangos = generar_rangos_mensuales(ultima_fecha, hoy)
        
        if not rangos:
            logger.info("✅ Todo está actualizado. No hay rangos nuevos.")
            subir_log_al_bucket()
            return

        driver = configurar_driver()
        
        try:
            for inicio, fin in rangos:
                logger.info(f"--- Procesando mes: {inicio} al {fin} ---")
                
                # Limpiar carpeta antes de cada descarga
                for f in glob.glob(os.path.join(DOWNLOAD_DIR, "*")):
                    os.remove(f)

                if descargar_excel_por_rango(driver, inicio, fin):
                    lista_archivos = glob.glob(os.path.join(DOWNLOAD_DIR, "*.xlsx"))
                    if lista_archivos:
                        archivo = max(lista_archivos, key=os.path.getctime)
                        logger.info(f"📂 Archivo detectado: {os.path.basename(archivo)}")
                        
                        # --- LLAMADA A TU ETL (etl_seia.py) ---
                        exito = etl_seia.procesar_y_cargar_excel(
                            file_path=archivo,
                            project_id=PROJECT_ID,
                            dataset_id=DATASET_ID,
                            table_id="seia_limpio"
                        )
                        
                        if exito:
                            logger.info(f"✅ Rango {inicio}-{fin} integrado a BigQuery exitosamente.")
                        else:
                            logger.error(f"❌ Falló la carga a BigQuery para {inicio}-{fin}")
                    else:
                        logger.error("❌ No se encontró el .xlsx tras la descarga.")
        except Exception as e:
            logger.error(f"❌ Error crítico durante el loop de rangos: {e}")
        finally:
            driver.quit()
            logger.info("🛑 Driver cerrado.")

    except Exception as e:
        logger.error(f"❌ Error fatal en el Job: {e}")
    
    # SIEMPRE SUBIMOS EL LOG AL FINAL
    logger.info("🏁 Proceso finalizado. Subiendo log...")
    subir_log_al_bucket()

if __name__ == "__main__":
    ejecutar_proceso()