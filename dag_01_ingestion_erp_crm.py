import warnings

warnings.filterwarnings("ignore", category=RuntimeWarning)


from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta


def conectar_sap():
    print("Conectando con sistema SAP...")


def extraer_datos_erp():
    print("Extrayendo 10,000 registros del ERP...")


def guardar_data_lake():
    print("Datos ERP almacenados en Data Lake.")


default_args = {
    "owner": "data_engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5)
}


with DAG(
    dag_id="dag_01_ingestion_erp",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["ERP", "ingestion"]
) as dag:

    conectar = PythonOperator(
        task_id="conectar_sap",
        python_callable=conectar_sap
    )

    extraer = PythonOperator(
        task_id="extraer_datos_erp",
        python_callable=extraer_datos_erp
    )

    guardar = PythonOperator(
        task_id="guardar_data_lake",
        python_callable=guardar_data_lake
    )

    conectar >> extraer >> guardar

if __name__ == "__main__":
    print("====== EJECUCION DE PRUEBA DAG ERP ======")

    conectar_sap()
    extraer_datos_erp()
    guardar_data_lake()

    print("====== PROCESO FINALIZADO ======")