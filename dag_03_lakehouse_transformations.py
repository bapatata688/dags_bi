"""
================================================================================
DAG: dag_03_lakehouse_transformations
================================================================================
Propósito:
    Este DAG representa la capa de transformación dentro de la arquitectura
    Lakehouse, moviendo los datos a través de las zonas:

        Raw (Bronze) -> Processed (Silver) -> Curated (Gold)

    En Raw se encuentran los datos crudos generados por los DAGs de ingesta
    (dag_01_ingestion_erp_crm y dag_02_ingestion_apis). Este DAG limpia,
    estandariza y aplica reglas de negocio para producir datasets analíticos
    listos para consumo en el Data Warehouse, dashboards de BI y modelos
    de Machine Learning.

Capas del Lakehouse:
    - Bronze (Raw):       Datos crudos, sin transformar, tal como llegaron.
    - Silver (Processed): Datos limpios, estandarizados, deduplicados,
                           con tipos de datos correctos y validaciones básicas.
    - Gold (Curated):     Datasets analíticos agregados, modelados para
                           consumo de negocio (dimensiones, hechos, KPIs).
================================================================================
"""

# ------------------------------------------------------------------------------
# Importaciones
# ------------------------------------------------------------------------------
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# ------------------------------------------------------------------------------
# Configuración de logging
# ------------------------------------------------------------------------------
logger = logging.getLogger(__name__)


# ------------------------------------------------------------------------------
# default_args: configuración estándar para todas las tareas del DAG
# ------------------------------------------------------------------------------
default_args = {
    "owner": "data_engineering_team",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# ==============================================================================
# FUNCIONES DE LA CAPA SILVER (Raw -> Processed)
# Limpieza, estandarización y validaciones básicas de calidad de datos.
# ==============================================================================

def clean_erp_crm_data(**kwargs):
    """
    Limpia y estandariza los datos provenientes de SAP y Salesforce
    almacenados en la capa Raw.

    Operaciones simuladas:
        - Eliminación de duplicados.
        - Normalización de formatos de fecha.
        - Estandarización de nombres de columnas (snake_case).
        - Eliminación de registros con campos críticos nulos.
    """
    logger.info("Iniciando limpieza de datos ERP/CRM (Raw -> Processed)...")

    try:
        source_path = "/data_lake/raw/erp_crm/"
        target_path = "/data_lake/processed/erp_crm/"

        # Simulación del proceso de limpieza
        records_read = 15000
        duplicates_removed = 320
        nulls_removed = 45
        records_written = records_read - duplicates_removed - nulls_removed

        logger.info(f"Registros leídos desde {source_path}: {records_read}")
        logger.info(f"Duplicados eliminados: {duplicates_removed}")
        logger.info(f"Registros con nulos críticos eliminados: {nulls_removed}")
        logger.info(f"Registros escritos en {target_path}: {records_written}")

        result = {
            "dataset": "erp_crm",
            "source_path": source_path,
            "target_path": target_path,
            "records_in": records_read,
            "records_out": records_written,
            "status": "success",
        }

        logger.info("Limpieza de datos ERP/CRM completada exitosamente.")
        return result

    except Exception as e:
        logger.error(f"Error en clean_erp_crm_data: {str(e)}")
        raise


def clean_billing_risk_data(**kwargs):
    """
    Limpia y estandariza los datos de facturación (SQL Server) y riesgo
    (PostgreSQL) almacenados en la capa Raw.

    Operaciones simuladas:
        - Conversión de tipos de datos (montos a tipo decimal).
        - Estandarización de monedas a una moneda base (USD).
        - Validación de rangos de fechas de facturación.
        - Eliminación de registros con montos negativos inválidos.
    """
    logger.info("Iniciando limpieza de datos de facturación y riesgo (Raw -> Processed)...")

    try:
        source_path = "/data_lake/raw/billing_risk/"
        target_path = "/data_lake/processed/billing_risk/"

        # Simulación del proceso de limpieza
        records_read = 8500
        invalid_amounts_removed = 60
        currency_converted = 8440
        records_written = records_read - invalid_amounts_removed

        logger.info(f"Registros leídos desde {source_path}: {records_read}")
        logger.info(f"Registros con montos inválidos eliminados: {invalid_amounts_removed}")
        logger.info(f"Registros convertidos a moneda base (USD): {currency_converted}")
        logger.info(f"Registros escritos en {target_path}: {records_written}")

        result = {
            "dataset": "billing_risk",
            "source_path": source_path,
            "target_path": target_path,
            "records_in": records_read,
            "records_out": records_written,
            "status": "success",
        }

        logger.info("Limpieza de datos de facturación y riesgo completada exitosamente.")
        return result

    except Exception as e:
        logger.error(f"Error en clean_billing_risk_data: {str(e)}")
        raise


def clean_external_apis_data(**kwargs):
    """
    Limpia y estandariza los datos JSON crudos provenientes de Shopify,
    Facebook Ads y Zendesk almacenados en la capa Raw.

    Operaciones simuladas:
        - Aplanado (flattening) de estructuras JSON anidadas.
        - Estandarización de zonas horarias (UTC).
        - Normalización de identificadores entre sistemas externos.
        - Filtrado de registros incompletos o de prueba (test orders, etc.).
    """
    logger.info("Iniciando limpieza de datos de APIs externas (Raw -> Processed)...")

    try:
        source_path = "/data_lake/raw/external_apis/"
        target_path = "/data_lake/processed/external_apis/"

        # Simulación del proceso de limpieza
        records_read = 22000
        flattened_records = 21800
        test_records_filtered = 150
        records_written = records_read - test_records_filtered

        logger.info(f"Registros leídos desde {source_path}: {records_read}")
        logger.info(f"Registros aplanados (JSON -> tabular): {flattened_records}")
        logger.info(f"Registros de prueba filtrados: {test_records_filtered}")
        logger.info(f"Registros escritos en {target_path}: {records_written}")

        result = {
            "dataset": "external_apis",
            "source_path": source_path,
            "target_path": target_path,
            "records_in": records_read,
            "records_out": records_written,
            "status": "success",
        }

        logger.info("Limpieza de datos de APIs externas completada exitosamente.")
        return result

    except Exception as e:
        logger.error(f"Error en clean_external_apis_data: {str(e)}")
        raise


# ==============================================================================
# FUNCIONES DE LA CAPA GOLD (Processed -> Curated)
# Transformaciones de negocio y creación de datasets analíticos.
# ==============================================================================

def build_sales_analytics_dataset(**kwargs):
    """
    Construye el dataset analítico de ventas (Curated), combinando datos
    de ERP/CRM, facturación y la tienda Shopify.

    Reglas de negocio simuladas:
        - Cálculo de ingresos netos por transacción (monto - descuentos).
        - Asociación de cada venta con su cliente y región geográfica.
        - Cálculo de métricas agregadas: ventas totales por día y por canal.
    """
    logger.info("Construyendo dataset analítico de ventas (Processed -> Curated)...")

    try:
        target_path = "/data_lake/curated/sales_analytics/"

        # Simulación de la transformación de negocio
        total_transactions = 18500
        total_revenue_usd = 1_245_300.75
        aggregated_rows = 365  # Ej: una fila resumen por día

        logger.info(f"Transacciones procesadas: {total_transactions}")
        logger.info(f"Ingresos totales calculados: ${total_revenue_usd:,.2f} USD")
        logger.info(f"Filas agregadas generadas (resumen diario): {aggregated_rows}")
        logger.info(f"Dataset 'sales_analytics' escrito en {target_path}")

        result = {
            "dataset": "sales_analytics",
            "target_path": target_path,
            "total_transactions": total_transactions,
            "total_revenue_usd": total_revenue_usd,
            "status": "success",
        }

        logger.info("Dataset de ventas (Gold) creado exitosamente.")
        return result

    except Exception as e:
        logger.error(f"Error en build_sales_analytics_dataset: {str(e)}")
        raise


def build_marketing_performance_dataset(**kwargs):
    """
    Construye el dataset analítico de desempeño de marketing (Curated),
    combinando datos de Facebook Ads con datos de ventas (Shopify/CRM).

    Reglas de negocio simuladas:
        - Cálculo de ROAS (Return On Ad Spend) por campaña.
        - Cálculo de costo por adquisición (CPA).
        - Atribución de ventas a campañas mediante UTM/cliente.
    """
    logger.info("Construyendo dataset de desempeño de marketing (Processed -> Curated)...")

    try:
        target_path = "/data_lake/curated/marketing_performance/"

        # Simulación de la transformación de negocio
        campaigns_processed = 42
        total_ad_spend_usd = 85_400.00
        average_roas = 3.6

        logger.info(f"Campañas procesadas: {campaigns_processed}")
        logger.info(f"Gasto total en publicidad: ${total_ad_spend_usd:,.2f} USD")
        logger.info(f"ROAS promedio calculado: {average_roas}")
        logger.info(f"Dataset 'marketing_performance' escrito en {target_path}")

        result = {
            "dataset": "marketing_performance",
            "target_path": target_path,
            "campaigns_processed": campaigns_processed,
            "average_roas": average_roas,
            "status": "success",
        }

        logger.info("Dataset de marketing (Gold) creado exitosamente.")
        return result

    except Exception as e:
        logger.error(f"Error en build_marketing_performance_dataset: {str(e)}")
        raise


def build_customer_360_dataset(**kwargs):
    """
    Construye el dataset Customer 360 (Curated), unificando información
    de CRM (Salesforce), soporte (Zendesk), facturación y riesgo.

    Reglas de negocio simuladas:
        - Unificación de identidades de cliente entre sistemas (entity resolution).
        - Cálculo de score de riesgo crediticio por cliente.
        - Cálculo de tickets de soporte abiertos/cerrados por cliente.
        - Segmentación de clientes (Alto valor, Medio, Bajo).
    """
    logger.info("Construyendo dataset Customer 360 (Processed -> Curated)...")

    try:
        target_path = "/data_lake/curated/customer_360/"

        # Simulación de la transformación de negocio
        unified_customers = 9800
        high_value_segment = 1200
        medium_value_segment = 5300
        low_value_segment = 3300

        logger.info(f"Clientes unificados (entity resolution): {unified_customers}")
        logger.info(f"Segmento Alto valor: {high_value_segment}")
        logger.info(f"Segmento Medio valor: {medium_value_segment}")
        logger.info(f"Segmento Bajo valor: {low_value_segment}")
        logger.info(f"Dataset 'customer_360' escrito en {target_path}")

        result = {
            "dataset": "customer_360",
            "target_path": target_path,
            "unified_customers": unified_customers,
            "status": "success",
        }

        logger.info("Dataset Customer 360 (Gold) creado exitosamente.")
        return result

    except Exception as e:
        logger.error(f"Error en build_customer_360_dataset: {str(e)}")
        raise


# ==============================================================================
# FUNCIÓN DE CONSOLIDACIÓN FINAL
# ==============================================================================

def consolidate_lakehouse_transformations(**kwargs):
    """
    Tarea final de consolidación. Recolecta los resultados de todas las
    transformaciones (Silver y Gold) vía XCom, genera un resumen ejecutivo
    y deja registro de auditoría del proceso ELT completo.

    En un entorno productivo, esta tarea adicionalmente:
        - Actualizaría el catálogo de datos (Data Catalog).
        - Notificaría a los equipos de BI que los datasets Gold están listos.
        - Registraría métricas de calidad en el sistema de observabilidad.
    """
    logger.info("Iniciando consolidación de transformaciones Lakehouse...")

    try:
        ti = kwargs["ti"]

        # Recolección de resultados de la capa Silver
        erp_crm_result = ti.xcom_pull(task_ids="clean_erp_crm_data")
        billing_risk_result = ti.xcom_pull(task_ids="clean_billing_risk_data")
        external_apis_result = ti.xcom_pull(task_ids="clean_external_apis_data")

        # Recolección de resultados de la capa Gold
        sales_result = ti.xcom_pull(task_ids="build_sales_analytics_dataset")
        marketing_result = ti.xcom_pull(task_ids="build_marketing_performance_dataset")
        customer_result = ti.xcom_pull(task_ids="build_customer_360_dataset")

        silver_results = [erp_crm_result, billing_risk_result, external_apis_result]
        gold_results = [sales_result, marketing_result, customer_result]

        logger.info("===== RESUMEN CAPA SILVER (Processed) =====")
        for res in silver_results:
            if res:
                logger.info(
                    f"  - {res['dataset']}: {res['records_in']} -> {res['records_out']} "
                    f"registros | status={res['status']}"
                )
            else:
                logger.warning("  - Una tarea de la capa Silver no retornó resultados.")

        logger.info("===== RESUMEN CAPA GOLD (Curated) =====")
        for res in gold_results:
            if res:
                logger.info(f"  - {res['dataset']}: status={res['status']}")
            else:
                logger.warning("  - Una tarea de la capa Gold no retornó resultados.")

        logger.info("Consolidación de transformaciones Lakehouse completada exitosamente.")
        logger.info("Datasets Gold disponibles para consumo en Data Warehouse y BI.")

        return {
            "silver_datasets": [r["dataset"] for r in silver_results if r],
            "gold_datasets": [r["dataset"] for r in gold_results if r],
            "status": "success",
        }

    except Exception as e:
        logger.error(f"Error en consolidate_lakehouse_transformations: {str(e)}")
        raise


# ==============================================================================
# DEFINICIÓN DEL DAG
# ==============================================================================

with DAG(
    dag_id="dag_03_lakehouse_transformations",
    description="Transformaciones Lakehouse: Raw -> Processed -> Curated",
    default_args=default_args,
    schedule_interval="0 4 * * *",  # Ejecución diaria a las 4 AM, después de la ingesta
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "transformation", "elt", "silver", "gold"],
) as dag:

    # --------------------------------------------------------------------
    # FASE 1: Limpieza y estandarización (Raw -> Processed / Bronze -> Silver)
    # Estas tareas se ejecutan en paralelo (Fan-Out), ya que operan sobre
    # conjuntos de datos independientes provenientes de los DAGs de ingesta.
    # --------------------------------------------------------------------
    clean_erp_crm = PythonOperator(
        task_id="clean_erp_crm_data",
        python_callable=clean_erp_crm_data,
    )

    clean_billing_risk = PythonOperator(
        task_id="clean_billing_risk_data",
        python_callable=clean_billing_risk_data,
    )

    clean_external_apis = PythonOperator(
        task_id="clean_external_apis_data",
        python_callable=clean_external_apis_data,
    )

    # --------------------------------------------------------------------
    # FASE 2: Transformaciones de negocio (Processed -> Curated / Silver -> Gold)
    # Cada dataset Gold depende de uno o más datasets Silver, ya que combina
    # información de distintas fuentes para generar valor analítico.
    # --------------------------------------------------------------------
    build_sales_analytics = PythonOperator(
        task_id="build_sales_analytics_dataset",
        python_callable=build_sales_analytics_dataset,
    )

    build_marketing_performance = PythonOperator(
        task_id="build_marketing_performance_dataset",
        python_callable=build_marketing_performance_dataset,
    )

    build_customer_360 = PythonOperator(
        task_id="build_customer_360_dataset",
        python_callable=build_customer_360_dataset,
    )

    # --------------------------------------------------------------------
    # FASE 3: Consolidación final del proceso ELT
    # trigger_rule="all_done" garantiza que el resumen se genere incluso
    # si alguna transformación falla, permitiendo identificar qué datasets
    # Gold quedaron disponibles y cuáles no.
    # --------------------------------------------------------------------
    consolidate = PythonOperator(
        task_id="consolidate_lakehouse_transformations",
        python_callable=consolidate_lakehouse_transformations,
        trigger_rule="all_done",
    )

    # --------------------------------------------------------------------
    # DEPENDENCIAS DEL DAG
    # --------------------------------------------------------------------

    # Silver -> Gold: cada dataset Curated depende de los datasets Processed
    # relevantes para su construcción.

    # sales_analytics depende de ERP/CRM (clientes, pedidos) y de facturación
    [clean_erp_crm, clean_billing_risk] >> build_sales_analytics

    # marketing_performance depende de APIs externas (Facebook Ads, Shopify)
    # y de ERP/CRM para atribución de ventas
    [clean_external_apis, clean_erp_crm] >> build_marketing_performance

    # customer_360 depende de TODAS las fuentes Silver (CRM, facturación,
    # riesgo y APIs externas como Zendesk)
    [clean_erp_crm, clean_billing_risk, clean_external_apis] >> build_customer_360

    # Gold -> Consolidación final
    [build_sales_analytics, build_marketing_performance, build_customer_360] >> consolidate
