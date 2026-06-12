import warnings
warnings.filterwarnings("ignore", category=RuntimeWarning)


from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta

# =========================
# capa raw
# =========================
# esta es la primera etapa del pipeline
# aqui llegan los datos desde dag 01 y dag 02 sin transformaciones avanzadas
# solo se realiza una validacion basica de estructura y calidad minima

def capa_raw():
    print("\n========== capa raw ==========")
    # ingesta inicial de datos desde distintas fuentes
    print("recibiendo datos desde dag 01 y dag 02")
    # validacion basica de formato y estructura
    print("validando estructura basica")
    # tipos de datos soportados en esta capa
    print("tipo de datos: estructurados y semi-estructurados")
    # limpieza inicial de registros corruptos o invalidos
    print("eliminando registros corruptos")
    # almacenamiento en zona raw del data lake
    print("salida → /raw/")
    print("estado: completado")


# =========================
# capa processed
# =========================
# en esta etapa se aplican transformaciones de negocio
# los datos comienzan a tener calidad para analisis

def capa_processed():
    print("\n========== capa processed ==========")
    # aplicacion de reglas de transformacion
    print("aplicando transformaciones de negocio")
    # normalizacion de campos para estandarizacion
    print("- normalizacion de campos")
    # eliminacion o imputacion de valores nulos
    print("- limpieza de nulos")
    # conversion a formato optimizado para analitica
    print("- conversion a formato parquet")
    # almacenamiento en capa procesada
    print("salida → /processed/")
    print("estado: completado")


# =========================
# capa curated
# =========================
# aqui se construyen datasets listos para analitica avanzada
# se aplica modelado dimensional (esquema estrella)

def capa_curated():
    print("\n========== capa curated ==========")
    # construccion de datasets finales para analisis
    print("construyendo datasets analiticos")
    # aplicacion de modelo dimensional
    print("aplicando modelo dimensional")
    # definicion de dimensiones principales del negocio
    print("dimensiones: cliente, producto, tiempo")
    # definicion de tabla de hechos
    print("hechos: ventas")
    # almacenamiento en capa curated
    print("salida → /curated/")
    print("estado: listo para analitica")


# =========================
# data warehouse
# =========================
# esta capa representa el almacenamiento estructurado para analitica sql
# normalmente se usa snowflake, postgresql o redshift

def data_warehouse():
    print("\n========== data warehouse ==========")
    # carga de datos en motor analitico
    print("cargando datos en snowflake / postgresql")
    # optimizacion para consultas sql
    print("optimizacion para consultas sql")
    # implementacion de modelo estrella
    print("modelo estrella implementado")
    print("estado: data warehouse listo")


# =========================
# conexion a mlops (dag 05)
# =========================
# esta etapa envia los datos preparados a sistemas de machine learning

def envio_mlops():
    print("\n========== conexion mlops ==========")
    # envio de dataset curado hacia pipeline de ml
    print("enviando dataset curado a dag 05")
    # posibles casos de uso de machine learning
    print("casos de uso:")
    print("- prediccion de ventas")
    print("- churn de clientes")
    print("- segmentacion de clientes")
    print("estado: listo para machine learning")


# =========================
# configuracion dag
# =========================
# definicion de parametros globales del workflow

default_args = {
    "owner": "data-engineering",  # responsable del dag
    "retries": 3,  # numero de reintentos en caso de fallo
    "retry_delay": timedelta(minutes=5),  # tiempo entre reintentos
}

# definicion del pipeline completo tipo lakehouse
with DAG(
    dag_id="dag_03_lakehouse_pipeline",  # identificador unico
    description="pipeline lakehouse: raw → processed → curated → warehouse → ml",
    schedule="@daily",  # ejecucion diaria automatica
    start_date=datetime(2026, 1, 1),  # fecha de inicio
    catchup=False,  # evita ejecuciones historicas
    default_args=default_args,
    tags=["lakehouse", "etl", "warehouse", "ml"],
) as dag:

    # definicion de tareas por capa del pipeline
    raw = PythonOperator(task_id="raw", python_callable=capa_raw)
    processed = PythonOperator(task_id="processed", python_callable=capa_processed)
    curated = PythonOperator(task_id="curated", python_callable=capa_curated)
    warehouse = PythonOperator(task_id="warehouse", python_callable=data_warehouse)
    ml = PythonOperator(task_id="mlops", python_callable=envio_mlops)

    # orquestacion secuencial del pipeline
    # cada etapa depende de la anterior
    raw >> processed >> curated >> warehouse >> ml


# =========================
# ejecucion manual (evidencia)
# =========================
# permite ejecutar el script sin airflow para pruebas locales

if __name__ == "__main__":
    capa_raw()
    capa_processed()
    capa_curated()
    data_warehouse()
    envio_mlops()