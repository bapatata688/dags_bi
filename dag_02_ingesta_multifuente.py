import warnings
warnings.filterwarnings("ignore", category=RuntimeWarning)


from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta

# Fan out extrancion paralela 

def extraer_sap():
    print("\n========== FAN-OUT: SAP (ERP) ==========")
    print("Tipo de dato: ESTRUCTURADO")
    print("Extrayendo clientes, ventas e inventario")
    print("Estado: COMPLETADO → /raw/sap/")


def extraer_crm():
    print("\n========== FAN-OUT: CRM ==========")
    print("Salesforce CRM → datos de clientes")
    print("Tipo de dato: SEMI-ESTRUCTURADO")
    print("Estado: COMPLETADO → /raw/crm/")


def extraer_ecommerce():
    print("\n========== FAN-OUT: E-COMMERCE ==========")
    print("Shopify → ventas online")
    print("Tipo de dato: SEMI-ESTRUCTURADO")
    print("Estado: COMPLETADO → /raw/ecommerce/")


def extraer_marketing():
    print("\n========== FAN-OUT: MARKETING ==========")
    print("Facebook Ads API → campañas publicitarias")
    print("Tipo de dato: JSON (semi-estructurado)")
    print("Estado: COMPLETADO → /raw/marketing/")


# Fan in consolidacion de datos 

def consolidar_datos():
    print("\n====================================")
    print("            FAN-IN STAGE            ")
    print("====================================")

    print("Recibiendo datasets desde:")
    print(" SAP")
    print(" CRM")
    print(" E-Commerce")
    print(" Marketing")

    print("\nProcesamiento de integración:")
    print("- Normalizando esquemas")
    print("- Resolviendo llaves de negocio")
    print("- Eliminando inconsistencias")
    print("- Unificando modelo de datos")

    print("\nRESULTADO FINAL:")
    print("Dataset consolidado → /processed/")
    print("Listo para DAG 03 (Lakehouse)")
    print("Estado: SUCCESS")


# configuracion general del dag

default_args = {
    "owner": "data-engineering",  # responsable del dag
    "retries": 3,  # numero de reintentos en caso de fallo
    "retry_delay": timedelta(minutes=5),  # tiempo entre reintentos
}

# definicion del dag principal de orquestacion
with DAG(
    dag_id="dag_02_fanout_fanin_multifuente",  # identificador unico
    description="ingesta multi-fuente con patron fan-out / fan-in",  # descripcion general
    schedule="@daily",  # ejecucion automatica diaria
    start_date=datetime(2026, 1, 1),  # fecha de inicio del dag
    catchup=False,  # evita ejecucion de fechas anteriores
    default_args=default_args,  # configuracion base
    tags=["fanout", "fanin", "ingesta", "multi-fuente"],  # etiquetas de organizacion
) as dag:

    # =========================
    # fan-out (ejecucion paralela)
    # =========================

    sap = PythonOperator(
        task_id="sap", # identificador de la tarea sap en airflow
        python_callable=extraer_sap  # funcion que simula extraccion desde sap erp
    )

    crm = PythonOperator(
        task_id="crm", # tarea para extraccion desde salesforce crm
        python_callable=extraer_crm # funcion que obtiene datos de clientes
    )

    ecommerce = PythonOperator(
        task_id="ecommerce", # tarea de extraccion desde plataforma de ventas online
        python_callable=extraer_ecommerce # funcion que obtiene datos de shopify
    )

    marketing = PythonOperator( 
        task_id="marketing", # tarea de extraccion de campañas publicitarias
        python_callable=extraer_marketing # funcion que obtiene datos de facebook ads api
    )

    # =========================
    # fan-in (consolidacion final)
    # =========================

    consolidacion = PythonOperator(
        task_id="consolidar",  # tarea final de integracion
        python_callable=consolidar_datos # funcion que unifica todos los datasets
    )

    # definicion del flujo: primero fan-out, luego fan-in
    [sap, crm, ecommerce, marketing] >> consolidacion


# =========================
# ejecucion 
# =========================

if __name__ == "__main__":
    extraer_sap()
    extraer_crm()
    extraer_ecommerce()
    extraer_marketing()
    consolidar_datos()