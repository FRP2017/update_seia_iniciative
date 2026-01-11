import os
import time
import logging
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from google.cloud import bigquery

# ==========================================
# 1. CONFIGURACIÓN Y CONSTANTES (Basado en terraform.tfvars)
# ==========================================

PROJECT_ID = "geo-ambiental-482615"
DATASET_ID = "dataset_ambiental"
TABLE_ID = "raw_seia"
BUCKET_NAME = "geo-ambiental-482615-data-landing"
DOWNLOAD_DIR = os.path.join(os.getcwd(), "downloads_excel")

if not os.path.exists(DOWNLOAD_DIR):
    os.makedirs(DOWNLOAD_DIR)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("SEIA_Scraper")

# ==========================================
# 2. MÓDULO DE CONSULTA BIGQUERY
# ==========================================

def obtener_ultima_fecha_bq():
    """Consulta la fecha máxima en la tabla de BigQuery."""
    client = bigquery.Client(project=PROJECT_ID)
    query = f"SELECT MAX(fecha_presentacion) as max_fecha FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`"
    
    try:
        query_job = client.query(query)
        results = query_job.result()
        for row in results:
            if row.max_fecha:
                # Si es datetime con zona horaria, lo pasamos a date
                return row.max_fecha.date() if hasattr(row.max_fecha, 'date') else row.max_fecha
        return date(2024, 1, 1) # Fecha por defecto si la tabla está vacía
    except Exception as e:
        logger.error(f"Error consultando BigQuery: {e}")
        return date(2024, 1, 1)

# ==========================================
# 3. MÓDULO DE LÓGICA DE TIEMPO
# ==========================================

def generar_rangos_mensuales(fecha_inicio, fecha_fin):
    """Genera pares de (desde, hasta) para cada mes entre dos fechas."""
    rangos = []
    current = fecha_inicio.replace(day=1)
    
    while current <= fecha_fin:
        proximo_mes = current + relativedelta(months=1)
        # El fin de mes es el día anterior al primero del próximo mes
        ultimo_dia_mes = proximo_mes - relativedelta(days=1)
        
        # Ajustar para no pasarse de la fecha actual
        fin_rango = ultimo_dia_mes if ultimo_dia_mes < fecha_fin else fecha_fin
        
        rangos.append({
            "desde": current.strftime('%d/%m/%Y'),
            "hasta": fin_rango.strftime('%d/%m/%Y')
        })
        current = proximo_mes
        
    return rangos

# ==========================================
# 4. MÓDULO SCRAPER (SELENIUM)
# ==========================================

def configurar_driver():
    options = Options()
    # options.add_argument('--headless=new') # Activar para despliegue
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--window-size=1920,1080')
    
    prefs = {
        "download.default_directory": DOWNLOAD_DIR,
        "download.prompt_for_download": False,
        "plugins.always_open_pdf_externally": True
    }
    options.add_experimental_option("prefs", prefs)
    
    driver = webdriver.Chrome(options=options)
    # Comando necesario para permitir descargas en modo headless
    driver.execute_cdp_cmd("Page.setDownloadBehavior", {
        "behavior": "allow", 
        "downloadPath": DOWNLOAD_DIR
    })
    return driver

def descargar_excel_seia(driver, fecha_desde, fecha_hasta):
    """Navega y descarga el Excel del SEIA para un rango de fechas."""
    wait = WebDriverWait(driver, 30)
    url = "https://seia.sea.gob.cl/busqueda/buscarProyecto.php"
    
    try:
        logger.info(f"Procesando rango: {fecha_desde} al {fecha_hasta}")
        driver.get(url)
        
        # Llenar campos de fecha
        wait.until(EC.presence_of_element_located((By.ID, "startDateFechaP"))).send_keys(fecha_desde)
        driver.find_element(By.ID, "endDateFechaP").send_keys(fecha_hasta)
        
        # Click en Buscar
        boton_buscar = driver.find_element(By.CSS_SELECTOR, "button.sg-btnForm")
        driver.execute_script("arguments[0].click();", boton_buscar)
        
        # Esperar resultados (o mensaje de no resultados)
        time.sleep(5)
        
        # Verificar si hay resultados antes de descargar
        if len(driver.find_elements(By.CSS_SELECTOR, "td.dt-empty")) > 0:
            logger.warning(f"Sin resultados para el rango {fecha_desde} - {fecha_hasta}")
            return False

        # Descargar Excel
        link_excel = wait.until(EC.element_to_be_clickable((By.LINK_TEXT, "Descargar en formato Excel")))
        driver.execute_script("arguments[0].click();", link_excel)
        
        # Espera corta para que inicie la descarga
        time.sleep(10)
        logger.info(f"Descarga iniciada para el rango {fecha_desde} - {fecha_hasta}")
        return True

    except Exception as e:
        logger.error(f"Error durante el scraping: {e}")
        return False

# ==========================================
# 5. ORQUESTADOR PRINCIPAL
# ==========================================

def ejecutar_proceso_completado():
    # 1. Obtener fechas clave
    ultima_fecha_bq = obtener_ultima_fecha_bq()
    hoy = date.today()
    
    logger.info(f"Última fecha en BQ: {ultima_fecha_bq} | Hoy: {hoy}")
    
    # 2. Calcular meses a procesar
    rangos = generar_rangos_mensuales(ultima_fecha_bq, hoy)
    
    if not rangos:
        logger.info("No hay rangos nuevos para procesar.")
        return

    # 3. Iniciar Driver
    driver = configurar_driver()
    
    try:
        for rango in rangos:
            descargar_excel_seia(driver, rango['desde'], rango['hasta'])
    finally:
        driver.quit()
        logger.info("Proceso finalizado. Archivos guardados en: " + DOWNLOAD_DIR)

if __name__ == "__main__":
    ejecutar_proceso_completado()