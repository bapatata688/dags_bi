import warnings
warnings.filterwarnings("ignore", category=RuntimeWarning)


from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta

def extraer_sap():
    print("=== INICIO EXTRACCIÓN SAP ===")
    print("Tipo de dato: ESTRUCTURADO")
    print("Conectando a SAP ERP...")
    print("Extrayendo: Clientes, Ventas, Inventario")
    print("Registros obtenidos: 10,000")
    print("Zona destino: Data Lake /raw/sap/")
    print("Datos enviados a capa RAW")
    print("Trigger conceptual hacia DAG 03 (Lakehouse)")
    print("=== FIN EXTRACCIÓN SAP ===")

# configuracion general del dag
default_args = {
    "owner": "data-engineering", # responsable del dag
    "retries": 3, # numero de reintentos si falla la tarea
    "retry_delay": timedelta(minutes=5), # tiempo entre reintentos
}

# definicion del dag principal
with DAG(
    dag_id="dag_01_ingesta_sap",  # identificador unico del dag
    description="ingesta sap hacia data lake",  # descripcion del proceso
    schedule="@daily",  # ejecucion diaria automatica
    start_date=datetime(2026, 1, 1),  # fecha de inicio del dag
    catchup=False,  # evita ejecutar dias pasados pendientes
    default_args=default_args,  # parametros generales
    tags=["ingesta", "sap", "erp"],  # etiquetas para organizacion
) as dag:

    # tarea que ejecuta la funcion de extraccion
    task = PythonOperator(
        task_id="extraer_sap",  # identificador de la tarea
        python_callable=extraer_sap,  # funcion que ejecuta la tarea
    )

# ejecucion local para pruebas sin airflow
if __name__ == "__main__":
    extraer_sap()