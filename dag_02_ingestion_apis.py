# =============================================================================
# DAG: dag_02_ingestion_apis.py
# Proyecto: Plataforma Moderna de Ingeniería de Datos
# Módulo: Data Engineering — Level Up Academy
# Autor: Data Engineering Team
# Versión: 1.0.0
# =============================================================================
# Descripción:
#   Este DAG orquesta la extracción de datos desde tres APIs externas hacia
#   la capa Raw del Data Lake, almacenando los resultados en formato JSON:
#
#     - Shopify      : Plataforma E-Commerce (pedidos, productos, clientes)
#     - Facebook Ads : Plataforma de Marketing Digital (campañas, métricas)
#     - Zendesk      : Plataforma de Atención al Cliente (tickets, agentes)
#
#   Las APIs externas tienen características distintas a los sistemas internos:
#     - Requieren autenticación via tokens / OAuth
#     - Imponen rate limits y quotas de consumo
#     - Devuelven datos en JSON paginado
#     - Pueden tener latencia variable o interrupciones temporales
#
#   Patrón utilizado: Fan-Out / Fan-In
#     - Una tarea inicial valida tokens y configuración.
#     - Tres tareas de extracción corren en paralelo (fan-out).
#     - Una tarea final consolida los resultados (fan-in).
# =============================================================================


# -----------------------------------------------------------------------------
# SECCIÓN 1: IMPORTACIONES
# Se importan módulos de Apache Airflow 2.x, Python estándar y utilidades
# necesarias para simular llamadas HTTP a APIs REST externas.
# -----------------------------------------------------------------------------

import json                              # Serialización/deserialización JSON
import logging                           # Logging estándar de Python
import random                            # Simular variaciones en respuestas de API
import uuid                              # Generar IDs únicos de registros simulados
from datetime import datetime, timedelta # Manejo de fechas para schedule y retries

from airflow import DAG                              # Clase principal del DAG
from airflow.operators.python import PythonOperator  # Operador para funciones Python


# -----------------------------------------------------------------------------
# SECCIÓN 2: CONFIGURACIÓN DEL LOGGER
# Logger a nivel de módulo. Airflow captura estos logs y los muestra en la UI
# para cada ejecución individual de tarea.
# -----------------------------------------------------------------------------

log = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# SECCIÓN 3: CONSTANTES DE CONFIGURACIÓN
# Parámetros centralizados del pipeline. En producción vendrían de Variables
# de Airflow (Variables.get()) o de un archivo config.yaml por entorno.
# -----------------------------------------------------------------------------

# Ruta de la capa Raw en el Data Lake (JSON sin procesar)
RAW_LAYER_PATH = "/data/raw"

# Identificadores de sistemas fuente
SOURCE_SHOPIFY      = "SHOPIFY_ECOMMERCE_PROD"
SOURCE_FACEBOOK_ADS = "FACEBOOK_ADS_MARKETING_PROD"
SOURCE_ZENDESK      = "ZENDESK_SUPPORT_PROD"

# Fecha de ejecución lógica del pipeline
EXECUTION_DATE = datetime.now().strftime("%Y-%m-%d")

# Configuración de las APIs (en producción estos valores serían secretos
# gestionados por Airflow Connections o un vault como HashiCorp Vault)
API_CONFIG = {
    "shopify": {
        "base_url"    : "https://empresa.myshopify.com/admin/api/2024-01",
        "api_version" : "2024-01",
        "rate_limit"  : "2 req/seg",      # Límite de Shopify en plan estándar
        "auth_type"   : "API Key / Access Token",
        "endpoints"   : ["orders", "products", "customers"],
    },
    "facebook_ads": {
        "base_url"    : "https://graph.facebook.com/v19.0",
        "api_version" : "v19.0",
        "rate_limit"  : "200 calls/hora",  # Límite de la Graph API
        "auth_type"   : "OAuth 2.0 / Long-Lived Token",
        "endpoints"   : ["campaigns", "adsets", "insights"],
    },
    "zendesk": {
        "base_url"    : "https://empresa.zendesk.com/api/v2",
        "api_version" : "v2",
        "rate_limit"  : "400 req/min",    # Límite de Zendesk en plan profesional
        "auth_type"   : "Basic Auth / API Token",
        "endpoints"   : ["tickets", "users", "satisfaction_ratings"],
    },
}


# -----------------------------------------------------------------------------
# SECCIÓN 4: DEFAULT_ARGS
# Argumentos globales aplicados a todas las tareas. Los retries son
# especialmente importantes para APIs externas, donde interrupciones
# temporales (503, rate limit 429) son comunes.
# -----------------------------------------------------------------------------

default_args = {
    # Equipo propietario del pipeline
    "owner": "data_engineering_team",

    # No depender del resultado de ejecuciones anteriores
    "depends_on_past": False,

    # Fecha de inicio del DAG
    "start_date": datetime(2025, 1, 1),

    # Reintentos ante fallos (cubre errores transitorios de API)
    "retries": 3,

    # Espera entre reintentos — importante para respetar rate limits
    "retry_delay": timedelta(minutes=10),

    # Sin notificaciones automáticas por email
    "email_on_failure": False,
    "email_on_retry"  : False,
}


# =============================================================================
# SECCIÓN 5: FUNCIONES AUXILIARES
# Utilidades compartidas entre las funciones de extracción para simular
# comportamientos comunes de las APIs externas.
# =============================================================================


def _simulate_api_call(endpoint: str, page: int, source: str) -> dict:
    """
    Simula una llamada HTTP GET paginada a un endpoint de API REST.

    En producción esta función usaría:
      - requests.get(url, headers=headers, params=params)
      - Manejo de códigos HTTP: 200, 429 (rate limit), 503 (service unavailable)
      - Lógica de backoff exponencial para reintentos
      - Gestión de cursores o tokens de paginación (next_page_token, Link header)

    Args:
        endpoint : Nombre del endpoint consultado (ej: 'orders', 'campaigns').
        page     : Número de página actual en la paginación.
        source   : Identificador del sistema fuente para logging.

    Returns:
        dict con los datos simulados de la respuesta JSON de la API.
    """
    # Simular cantidad variable de registros por página (comportamiento real)
    records_per_page = random.randint(50, 250)

    # Simular registros genéricos como lo devolvería la API en formato JSON
    records = [
        {
            "id"         : str(uuid.uuid4()),
            "source"     : source,
            "endpoint"   : endpoint,
            "page"       : page,
            "created_at" : EXECUTION_DATE,
            "payload"    : f"simulated_data_{endpoint}_{i}",
        }
        for i in range(records_per_page)
    ]

    return {
        "status"      : 200,
        "endpoint"    : endpoint,
        "page"        : page,
        "count"       : records_per_page,
        "records"     : records,
        "has_next_page": page < 3,  # Simular 3 páginas máximo por endpoint
    }


def _save_to_raw_layer(data: list, source_name: str, endpoint: str) -> str:
    """
    Simula el guardado de datos JSON en la capa Raw del Data Lake.

    En producción esta función escribiría:
      - Archivos JSON en un bucket S3 (boto3), ADLS (azure-storage-blob)
        o GCS (google-cloud-storage).
      - Particionado por fecha: source/YYYY/MM/DD/endpoint/file.json
      - Compresión opcional: Snappy o GZIP para reducir almacenamiento.
      - Metadata del archivo: tamaño, hash MD5, timestamp de escritura.

    Args:
        data      : Lista de registros a guardar.
        source_name: Nombre del sistema fuente (usado para el path).
        endpoint  : Nombre del endpoint (usado para el nombre del archivo).

    Returns:
        str: Ruta simulada donde se almacenó el archivo JSON.
    """
    # Construir la ruta particionada por fecha (estándar en Data Lakes)
    year, month, day = EXECUTION_DATE.split("-")
    file_path = (
        f"{RAW_LAYER_PATH}/{source_name}/{year}/{month}/{day}/"
        f"{endpoint}_{EXECUTION_DATE}.json"
    )

    # En producción: escribir el JSON al storage (S3, ADLS, GCS, HDFS)
    json_content = json.dumps(data, indent=2, default=str)
    file_size_kb = len(json_content.encode("utf-8")) / 1024

    log.info(f"  → Archivo guardado: {file_path}")
    log.info(f"  → Tamaño estimado : {file_size_kb:.1f} KB")
    log.info(f"  → Registros       : {len(data):,}")

    return file_path


# =============================================================================
# SECCIÓN 6: FUNCIONES DE EXTRACCIÓN POR API
# Cada función simula el flujo completo de extracción de una API externa:
# autenticación, paginación, manejo de errores y persistencia en Raw Layer.
# =============================================================================


def extract_shopify_ecommerce(**context):
    """
    Extrae datos de la plataforma E-Commerce Shopify vía REST API.

    Endpoints consultados:
      - /orders.json      : Pedidos del día (nuevos, actualizados, cancelados)
      - /products.json    : Catálogo de productos activos
      - /customers.json   : Clientes nuevos y actualizados

    Consideraciones técnicas en producción:
      - Shopify limita a 2 req/seg en planes estándar y 4 req/seg en Shopify Plus.
      - La paginación usa cursor-based pagination (Link header con rel="next").
      - Los webhooks son una alternativa a polling para eventos en tiempo real.
      - Access Token se almacena en Airflow Connections (conn_id: 'shopify_conn').

    Args:
        context: Contexto de ejecución de Airflow.
    """
    log.info("=" * 60)
    log.info(f"[{SOURCE_SHOPIFY}] Iniciando extracción - Fecha: {EXECUTION_DATE}")
    log.info(f"[{SOURCE_SHOPIFY}] Base URL : {API_CONFIG['shopify']['base_url']}")
    log.info(f"[{SOURCE_SHOPIFY}] Auth     : {API_CONFIG['shopify']['auth_type']}")
    log.info(f"[{SOURCE_SHOPIFY}] Rate Limit: {API_CONFIG['shopify']['rate_limit']}")
    log.info("=" * 60)

    try:
        # ----------------------------------------------------------------
        # PASO 1: Autenticación con Shopify
        # En producción: recuperar el access token desde Airflow Connections
        #   hook = HttpHook(http_conn_id='shopify_conn', method='GET')
        #   headers = {"X-Shopify-Access-Token": Variable.get("shopify_token")}
        # ----------------------------------------------------------------
        log.info(f"[{SOURCE_SHOPIFY}] Autenticando con Access Token...")
        log.info(f"[{SOURCE_SHOPIFY}] Shop: empresa.myshopify.com")
        log.info(f"[{SOURCE_SHOPIFY}] Autenticación exitosa. Iniciando extracción por endpoint.")

        archivos_generados = []
        total_registros    = 0

        # ----------------------------------------------------------------
        # PASO 2: Extracción por endpoint con paginación
        # ----------------------------------------------------------------
        for endpoint in API_CONFIG["shopify"]["endpoints"]:
            log.info(f"[{SOURCE_SHOPIFY}] Extrayendo endpoint: /{endpoint}.json")
            todos_los_registros = []
            page = 1

            # Simular lógica de paginación con cursor (Link header)
            while True:
                log.info(f"[{SOURCE_SHOPIFY}]   Consultando página {page} de /{endpoint}...")

                # Simular llamada HTTP GET paginada
                respuesta = _simulate_api_call(endpoint, page, SOURCE_SHOPIFY)

                todos_los_registros.extend(respuesta["records"])
                log.info(f"[{SOURCE_SHOPIFY}]   Página {page}: {respuesta['count']} registros recibidos.")

                # Verificar si hay más páginas (en producción: leer Link header)
                if not respuesta.get("has_next_page"):
                    log.info(f"[{SOURCE_SHOPIFY}]   No hay más páginas para /{endpoint}.")
                    break

                page += 1

            # Guardar todos los registros del endpoint en Raw Layer
            file_path = _save_to_raw_layer(todos_los_registros, "shopify", endpoint)
            archivos_generados.append(file_path)
            total_registros += len(todos_los_registros)
            log.info(f"[{SOURCE_SHOPIFY}] Endpoint /{endpoint} completado: {len(todos_los_registros):,} registros.")

        # ----------------------------------------------------------------
        # PASO 3: Publicar metadata via XCom
        # ----------------------------------------------------------------
        resultado = {
            "source"            : SOURCE_SHOPIFY,
            "execution_date"    : EXECUTION_DATE,
            "endpoints_extracted": API_CONFIG["shopify"]["endpoints"],
            "files_generated"   : archivos_generados,
            "total_records"     : total_registros,
            "raw_layer_path"    : f"{RAW_LAYER_PATH}/shopify/{EXECUTION_DATE}/",
            "status"            : "SUCCESS",
        }

        context["ti"].xcom_push(key="shopify_result", value=resultado)

        log.info(f"[{SOURCE_SHOPIFY}] Total registros extraídos: {total_registros:,}")
        log.info(f"[{SOURCE_SHOPIFY}] Archivos generados: {len(archivos_generados)}")
        log.info(f"[{SOURCE_SHOPIFY}] Estado: SUCCESS ✓")
        return resultado

    except Exception as e:
        log.error(f"[{SOURCE_SHOPIFY}] ERROR durante la extracción: {str(e)}")
        log.error(f"[{SOURCE_SHOPIFY}] Posibles causas: token expirado, rate limit alcanzado, API no disponible.")
        raise


def extract_facebook_ads(**context):
    """
    Extrae métricas de campañas publicitarias desde la Facebook Graph API.

    Endpoints consultados:
      - /act_{ad_account_id}/campaigns : Campañas activas y su configuración
      - /act_{ad_account_id}/adsets    : Conjuntos de anuncios con targeting
      - /{campaign_id}/insights        : Métricas de rendimiento (impresiones,
                                         clics, conversiones, gasto)

    Consideraciones técnicas en producción:
      - La Graph API usa OAuth 2.0 con Long-Lived Tokens (60 días de vigencia).
      - Las métricas de insights tienen un retraso de ~24-72 horas (data delay).
      - La API Marketing tiene límites por Business Account (200 calls/hora base).
      - Los campos de insights deben declararse explícitamente (no hay SELECT *).
      - Usar Batch Requests para reducir el número de llamadas HTTP.

    Args:
        context: Contexto de ejecución de Airflow.
    """
    log.info("=" * 60)
    log.info(f"[{SOURCE_FACEBOOK_ADS}] Iniciando extracción - Fecha: {EXECUTION_DATE}")
    log.info(f"[{SOURCE_FACEBOOK_ADS}] Base URL : {API_CONFIG['facebook_ads']['base_url']}")
    log.info(f"[{SOURCE_FACEBOOK_ADS}] Auth     : {API_CONFIG['facebook_ads']['auth_type']}")
    log.info(f"[{SOURCE_FACEBOOK_ADS}] Rate Limit: {API_CONFIG['facebook_ads']['rate_limit']}")
    log.info("=" * 60)

    try:
        # ----------------------------------------------------------------
        # PASO 1: Validar y refrescar token OAuth
        # En producción:
        #   token = Variable.get("facebook_long_lived_token")
        #   response = requests.get(
        #       f"https://graph.facebook.com/debug_token?input_token={token}",
        #       params={"access_token": f"{app_id}|{app_secret}"}
        #   )
        # ----------------------------------------------------------------
        log.info(f"[{SOURCE_FACEBOOK_ADS}] Validando Long-Lived Token OAuth 2.0...")
        log.info(f"[{SOURCE_FACEBOOK_ADS}] Ad Account ID: act_123456789")
        log.info(f"[{SOURCE_FACEBOOK_ADS}] Token vigente. Fecha de expiración simulada: {EXECUTION_DATE}")
        log.info(f"[{SOURCE_FACEBOOK_ADS}] Business Manager: Empresa S.A.")

        archivos_generados = []
        total_registros    = 0

        # ----------------------------------------------------------------
        # PASO 2: Extracción por endpoint con paginación cursor-based
        # Facebook Graph API usa cursor pagination con campos 'before'/'after'
        # ----------------------------------------------------------------
        for endpoint in API_CONFIG["facebook_ads"]["endpoints"]:
            log.info(f"[{SOURCE_FACEBOOK_ADS}] Extrayendo endpoint: /{endpoint}")

            # Para insights, declarar los campos explícitamente
            if endpoint == "insights":
                log.info(
                    f"[{SOURCE_FACEBOOK_ADS}]   Fields: impressions, clicks, spend, "
                    "reach, cpc, cpm, ctr, conversions, date_start, date_stop"
                )
                log.info(
                    f"[{SOURCE_FACEBOOK_ADS}]   Date preset: yesterday | "
                    f"Level: campaign | Time increment: 1"
                )

            todos_los_registros = []
            page = 1

            while True:
                log.info(f"[{SOURCE_FACEBOOK_ADS}]   Cursor page {page} para /{endpoint}...")

                respuesta = _simulate_api_call(endpoint, page, SOURCE_FACEBOOK_ADS)
                todos_los_registros.extend(respuesta["records"])
                log.info(f"[{SOURCE_FACEBOOK_ADS}]   Recibidos {respuesta['count']} registros.")

                if not respuesta.get("has_next_page"):
                    break

                page += 1

            file_path = _save_to_raw_layer(todos_los_registros, "facebook_ads", endpoint)
            archivos_generados.append(file_path)
            total_registros += len(todos_los_registros)
            log.info(f"[{SOURCE_FACEBOOK_ADS}] Endpoint /{endpoint} completado: {len(todos_los_registros):,} registros.")

        # ----------------------------------------------------------------
        # PASO 3: Publicar metadata via XCom
        # ----------------------------------------------------------------
        resultado = {
            "source"            : SOURCE_FACEBOOK_ADS,
            "execution_date"    : EXECUTION_DATE,
            "endpoints_extracted": API_CONFIG["facebook_ads"]["endpoints"],
            "files_generated"   : archivos_generados,
            "total_records"     : total_registros,
            "raw_layer_path"    : f"{RAW_LAYER_PATH}/facebook_ads/{EXECUTION_DATE}/",
            "status"            : "SUCCESS",
        }

        context["ti"].xcom_push(key="facebook_result", value=resultado)

        log.info(f"[{SOURCE_FACEBOOK_ADS}] Total registros extraídos: {total_registros:,}")
        log.info(f"[{SOURCE_FACEBOOK_ADS}] Archivos generados: {len(archivos_generados)}")
        log.info(f"[{SOURCE_FACEBOOK_ADS}] Estado: SUCCESS ✓")
        return resultado

    except Exception as e:
        log.error(f"[{SOURCE_FACEBOOK_ADS}] ERROR durante la extracción: {str(e)}")
        log.error(f"[{SOURCE_FACEBOOK_ADS}] Posibles causas: token expirado, Ad Account suspendido, quota excedida.")
        raise


def extract_zendesk_support(**context):
    """
    Extrae datos del sistema de Atención al Cliente Zendesk vía REST API.

    Endpoints consultados:
      - /tickets.json              : Tickets de soporte (abiertos, cerrados, pendientes)
      - /users.json                : Agentes y usuarios finales
      - /satisfaction_ratings.json : Calificaciones CSAT de los clientes

    Consideraciones técnicas en producción:
      - Zendesk usa Basic Auth con email/token o OAuth 2.0.
      - La API v2 soporta cursor-based y offset-based pagination.
      - El endpoint /incremental/tickets.json permite extracción incremental
        por unix timestamp (más eficiente que extraer todos los tickets).
      - CSAT ratings tienen SLA de disponibilidad de 24h después del cierre.
      - Para alto volumen usar Zendesk Incremental API o Zendesk Explore.

    Args:
        context: Contexto de ejecución de Airflow.
    """
    log.info("=" * 60)
    log.info(f"[{SOURCE_ZENDESK}] Iniciando extracción - Fecha: {EXECUTION_DATE}")
    log.info(f"[{SOURCE_ZENDESK}] Base URL : {API_CONFIG['zendesk']['base_url']}")
    log.info(f"[{SOURCE_ZENDESK}] Auth     : {API_CONFIG['zendesk']['auth_type']}")
    log.info(f"[{SOURCE_ZENDESK}] Rate Limit: {API_CONFIG['zendesk']['rate_limit']}")
    log.info("=" * 60)

    try:
        # ----------------------------------------------------------------
        # PASO 1: Autenticación Basic Auth con API Token
        # En producción:
        #   from airflow.hooks.http_hook import HttpHook
        #   hook = HttpHook(http_conn_id='zendesk_conn', method='GET')
        #   headers = {"Authorization": f"Basic {base64(email/token:api_token)}"}
        # ----------------------------------------------------------------
        log.info(f"[{SOURCE_ZENDESK}] Autenticando con API Token (Basic Auth)...")
        log.info(f"[{SOURCE_ZENDESK}] Subdominio: empresa.zendesk.com")
        log.info(f"[{SOURCE_ZENDESK}] Usuario   : data-pipeline@empresa.com/token")
        log.info(f"[{SOURCE_ZENDESK}] Autenticación exitosa.")

        archivos_generados = []
        total_registros    = 0

        # ----------------------------------------------------------------
        # PASO 2: Extracción incremental por endpoint
        # Para tickets se usa el endpoint incremental con start_time unix
        # para obtener solo los registros modificados desde la última ejecución
        # ----------------------------------------------------------------
        for endpoint in API_CONFIG["zendesk"]["endpoints"]:

            # Estrategia diferenciada por tipo de endpoint
            if endpoint == "tickets":
                log.info(f"[{SOURCE_ZENDESK}] Usando Incremental Ticket Export API...")
                log.info(f"[{SOURCE_ZENDESK}] URL: /incremental/tickets.json?start_time=<unix_ts>")
                log.info(f"[{SOURCE_ZENDESK}] Filtrando tickets actualizados desde: {EXECUTION_DATE} 00:00:00 UTC")
            elif endpoint == "satisfaction_ratings":
                log.info(f"[{SOURCE_ZENDESK}] Extrayendo CSAT ratings del día anterior...")
                log.info(f"[{SOURCE_ZENDESK}] URL: /satisfaction_ratings.json?start_time={EXECUTION_DATE}")
            else:
                log.info(f"[{SOURCE_ZENDESK}] Extrayendo endpoint estándar: /{endpoint}.json")

            todos_los_registros = []
            page = 1

            while True:
                log.info(f"[{SOURCE_ZENDESK}]   Consultando página {page} de /{endpoint}...")

                respuesta = _simulate_api_call(endpoint, page, SOURCE_ZENDESK)
                todos_los_registros.extend(respuesta["records"])
                log.info(f"[{SOURCE_ZENDESK}]   Recibidos {respuesta['count']} registros.")

                if not respuesta.get("has_next_page"):
                    log.info(f"[{SOURCE_ZENDESK}]   Paginación completada para /{endpoint}.")
                    break

                page += 1

            file_path = _save_to_raw_layer(todos_los_registros, "zendesk", endpoint)
            archivos_generados.append(file_path)
            total_registros += len(todos_los_registros)
            log.info(f"[{SOURCE_ZENDESK}] Endpoint /{endpoint} completado: {len(todos_los_registros):,} registros.")

        # ----------------------------------------------------------------
        # PASO 3: Publicar metadata via XCom
        # ----------------------------------------------------------------
        resultado = {
            "source"            : SOURCE_ZENDESK,
            "execution_date"    : EXECUTION_DATE,
            "endpoints_extracted": API_CONFIG["zendesk"]["endpoints"],
            "files_generated"   : archivos_generados,
            "total_records"     : total_registros,
            "raw_layer_path"    : f"{RAW_LAYER_PATH}/zendesk/{EXECUTION_DATE}/",
            "status"            : "SUCCESS",
        }

        context["ti"].xcom_push(key="zendesk_result", value=resultado)

        log.info(f"[{SOURCE_ZENDESK}] Total registros extraídos: {total_registros:,}")
        log.info(f"[{SOURCE_ZENDESK}] Archivos generados: {len(archivos_generados)}")
        log.info(f"[{SOURCE_ZENDESK}] Estado: SUCCESS ✓")
        return resultado

    except Exception as e:
        log.error(f"[{SOURCE_ZENDESK}] ERROR durante la extracción: {str(e)}")
        log.error(f"[{SOURCE_ZENDESK}] Posibles causas: API Token revocado, subdominio incorrecto, quota excedida.")
        raise


# =============================================================================
# SECCIÓN 7: FUNCIONES DE CONTROL
# Nodos de inicio y cierre del pipeline que actúan como puntos de control.
# =============================================================================


def validate_api_tokens(**context):
    """
    Tarea de inicio (nodo de control): valida tokens y configuración de APIs.

    Verifica que todos los requisitos previos estén listos antes de
    iniciar las extracciones paralelas. En producción:
      - Verificaría la vigencia de cada token (GET /debug_token para Facebook,
        GET /api/v2/account para Zendesk, etc.)
      - Validaría que las cuotas de API no estén agotadas.
      - Confirmaría conectividad a cada endpoint externo.
      - Comprobaría que la capa Raw del Data Lake está accesible.

    Args:
        context: Contexto de ejecución de Airflow.
    """
    log.info("=" * 60)
    log.info("VALIDACIÓN DE TOKENS Y CONFIGURACIÓN DE APIs")
    log.info("=" * 60)

    log.info(f"DAG ID       : {context['dag'].dag_id}")
    log.info(f"Run ID       : {context['run_id']}")
    log.info(f"Fecha lógica : {EXECUTION_DATE}")
    log.info(f"Raw Layer    : {RAW_LAYER_PATH}")

    log.info("Verificando tokens de autenticación por API:")
    for api_name, config in API_CONFIG.items():
        log.info(f"  ✓ {api_name.upper()}")
        log.info(f"      URL         : {config['base_url']}")
        log.info(f"      Auth Type   : {config['auth_type']}")
        log.info(f"      Rate Limit  : {config['rate_limit']}")
        log.info(f"      Endpoints   : {', '.join(config['endpoints'])}")

    log.info("Verificando estructura de directorios en Raw Layer:")
    for source in ["shopify", "facebook_ads", "zendesk"]:
        path = f"{RAW_LAYER_PATH}/{source}/{EXECUTION_DATE}/"
        log.info(f"  ✓ Directorio verificado: {path}")

    log.info("Validación completada. Las extracciones paralelas pueden iniciarse.")


def consolidate_api_results(**context):
    """
    Tarea de cierre (Fan-In): consolida los resultados de todas las APIs.

    Recupera la metadata publicada por cada extractor via XCom y genera
    un resumen ejecutivo del proceso. En producción también:
      - Escribiría una entrada en la tabla de auditoría del Data Warehouse.
      - Actualizaría el catálogo de datos (DataHub / OpenMetadata / Amundsen).
      - Enviaría notificaciones al canal #data-pipelines de Slack.
      - Dispararía el DAG de transformación Bronze si todo fue exitoso
        (usando TriggerDagRunOperator).

    Args:
        context: Contexto de ejecución de Airflow.
    """
    log.info("=" * 60)
    log.info("CONSOLIDACIÓN DE RESULTADOS — APIs EXTERNAS")
    log.info("=" * 60)

    ti = context["ti"]

    # Recuperar resultados de cada extractor via XCom
    resultado_shopify  = ti.xcom_pull(task_ids="extract_shopify_ecommerce", key="shopify_result")
    resultado_facebook = ti.xcom_pull(task_ids="extract_facebook_ads",      key="facebook_result")
    resultado_zendesk  = ti.xcom_pull(task_ids="extract_zendesk_support",   key="zendesk_result")

    todos_resultados = [resultado_shopify, resultado_facebook, resultado_zendesk]

    # Calcular métricas consolidadas del proceso
    total_registros    = sum(r["total_records"] for r in todos_resultados if r)
    total_archivos     = sum(len(r["files_generated"]) for r in todos_resultados if r)
    fuentes_exitosas   = sum(1 for r in todos_resultados if r and r.get("status") == "SUCCESS")

    # Reporte consolidado
    log.info(f"Fecha de ejecución          : {EXECUTION_DATE}")
    log.info(f"APIs procesadas             : {fuentes_exitosas} / {len(todos_resultados)}")
    log.info(f"Total registros ingestados  : {total_registros:,}")
    log.info(f"Total archivos JSON creados : {total_archivos}")
    log.info("")
    log.info("Detalle por API:")
    log.info("-" * 55)

    for resultado in todos_resultados:
        if resultado:
            log.info(f"  API        : {resultado['source']}")
            log.info(f"  Endpoints  : {', '.join(resultado['endpoints_extracted'])}")
            log.info(f"  Registros  : {resultado['total_records']:,}")
            log.info(f"  Archivos   : {len(resultado['files_generated'])}")
            log.info(f"  Destino    : {resultado['raw_layer_path']}")
            log.info(f"  Estado     : {resultado['status']}")
            log.info("-" * 55)

    if fuentes_exitosas == len(todos_resultados):
        log.info("RESULTADO FINAL: INGESTA DE APIs COMPLETADA EXITOSAMENTE ✓")
        log.info(f"Todos los JSON están disponibles en: {RAW_LAYER_PATH}/")
        log.info("El pipeline Bronze (dag_03) puede iniciarse.")
    else:
        fallidas = len(todos_resultados) - fuentes_exitosas
        log.warning(f"RESULTADO FINAL: INGESTA PARCIAL — {fallidas} API(s) con errores.")
        log.warning("Revisar tareas fallidas antes de iniciar transformaciones Bronze.")


# =============================================================================
# SECCIÓN 8: DEFINICIÓN DEL DAG
# =============================================================================

with DAG(
    # Identificador único del DAG en Airflow
    dag_id="dag_02_ingestion_apis",

    # Argumentos por defecto definidos en Sección 4
    default_args=default_args,

    # Descripción visible en la UI de Airflow
    description=(
        "Extracción diaria desde APIs externas: Shopify (E-Commerce), "
        "Facebook Ads (Marketing) y Zendesk (Soporte). Almacenamiento en Raw Layer (JSON)."
    ),

    # Ejecución diaria a las 03:00 AM
    # Se ejecuta 1 hora después del DAG 01 para no saturar la red corporativa
    schedule_interval="0 3 * * *",

    # Sin catchup — no ejecutar corridas históricas
    catchup=False,

    # Etiquetas para filtrado en la UI de Airflow
    tags=["ingestion", "apis", "shopify", "facebook-ads", "zendesk", "raw-layer"],

    # Una sola instancia activa simultánea (evita duplicados en la Raw Layer)
    max_active_runs=1,

    # Documentación interna del DAG (visible en la UI → DAG Docs)
    doc_md="""
    ## DAG 02 — Ingesta APIs Externas

    **Objetivo:** Extraer datos de APIs externas de E-Commerce, Marketing
    y Soporte, almacenando los resultados en formato JSON en la Raw Layer.

    **Fuentes:**
    - Shopify E-Commerce (orders, products, customers)
    - Facebook Ads Marketing (campaigns, adsets, insights)
    - Zendesk Support (tickets, users, satisfaction_ratings)

    **Patrón:** Fan-Out / Fan-In  
    **Formato de salida:** JSON particionado por fecha  
    **Schedule:** Diario 03:00 AM  
    **Siguiente capa:** dag_03_bronze_transformation  
    **DAG previo:** dag_01_ingestion_erp_crm
    """,

) as dag:

    # =========================================================================
    # SECCIÓN 9: DEFINICIÓN DE TAREAS
    # =========================================================================

    # -------------------------------------------------------------------------
    # TAREA 0: Validación de tokens y configuración (nodo de inicio)
    # -------------------------------------------------------------------------
    task_validate_tokens = PythonOperator(
        task_id="validate_api_tokens",
        python_callable=validate_api_tokens,
        doc_md="Valida tokens de autenticación y configuración de las tres APIs externas.",
    )

    # -------------------------------------------------------------------------
    # TAREA 1: Extracción desde Shopify E-Commerce
    # -------------------------------------------------------------------------
    task_extract_shopify = PythonOperator(
        task_id="extract_shopify_ecommerce",
        python_callable=extract_shopify_ecommerce,
        doc_md="Extrae órdenes, productos y clientes desde Shopify REST API.",
    )

    # -------------------------------------------------------------------------
    # TAREA 2: Extracción desde Facebook Ads
    # -------------------------------------------------------------------------
    task_extract_facebook = PythonOperator(
        task_id="extract_facebook_ads",
        python_callable=extract_facebook_ads,
        doc_md="Extrae campañas, ad sets e insights de rendimiento desde Facebook Graph API.",
    )

    # -------------------------------------------------------------------------
    # TAREA 3: Extracción desde Zendesk Support
    # -------------------------------------------------------------------------
    task_extract_zendesk = PythonOperator(
        task_id="extract_zendesk_support",
        python_callable=extract_zendesk_support,
        doc_md="Extrae tickets, agentes y CSAT ratings desde Zendesk REST API v2.",
    )

    # -------------------------------------------------------------------------
    # TAREA 4: Consolidación de resultados (nodo de cierre — Fan-In)
    # trigger_rule='all_done': se ejecuta aunque alguna API haya fallado,
    # permitiendo registrar el resultado parcial del proceso.
    # -------------------------------------------------------------------------
    task_consolidate = PythonOperator(
        task_id="consolidate_api_results",
        python_callable=consolidate_api_results,
        trigger_rule="all_done",
        doc_md="Consolida metadata de las tres extracciones y genera reporte de auditoría.",
    )

    # =========================================================================
    # SECCIÓN 10: DEPENDENCIAS — PATRÓN FAN-OUT / FAN-IN
    #
    #   task_validate_tokens
    #          │
    #          ├──► task_extract_shopify  ──┐
    #          ├──► task_extract_facebook ──┼──► task_consolidate
    #          └──► task_extract_zendesk  ──┘
    #
    # La validación inicial es el único nodo secuencial.
    # Las tres extracciones corren en PARALELO (workers independientes de Airflow).
    # La consolidación espera a que todas terminen (con o sin error).
    # =========================================================================

    # La validación debe completarse antes de cualquier extracción
    task_validate_tokens >> [
        task_extract_shopify,
        task_extract_facebook,
        task_extract_zendesk,
    ]

    # Todas las extracciones deben terminar antes de la consolidación
    [
        task_extract_shopify,
        task_extract_facebook,
        task_extract_zendesk,
    ] >> task_consolidate
