import warnings
warnings.filterwarnings("ignore", category=RuntimeWarning)


from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta

# =========================
# entrenamiento de modelos
# =========================
# esta etapa representa el entrenamiento de modelos de machine learning
# los datos provienen del data warehouse ya estructurados y listos para analisis

def entrenar_modelos():
    print("\n========== mlops: entrenamiento ==========")
    # consumo de datos desde la capa analitica
    print("consumiendo datos desde data warehouse")
    # modelos que se entrenan en el pipeline
    print("modelos:")
    print("- prediccion de ventas")
    print("- churn de clientes")
    print("- segmentacion de clientes")
    print("estado: entrenamiento completado")


# =========================
# evaluacion de modelos
# =========================
# aqui se validan las metricas de desempeño de los modelos entrenados

def evaluar_modelos():
    print("\n========== mlops: evaluacion ==========")
    # metricas de evaluacion de modelos supervisados y no supervisados
    print("metricas de evaluacion:")
    print("- accuracy: 0.87")
    print("- rmse: 12.4")
    print("- silhouette score: 0.71")
    # decision de aprobacion para produccion
    print("estado: modelos aprobados para produccion")


# =========================
# despliegue (produccion)
# =========================
# esta etapa publica los modelos en un entorno productivo
# tambien activa monitoreo y reentrenamiento automatico

def desplegar_modelos():
    print("\n========== mlops: despliegue ==========")
    # publicacion de modelos en produccion
    print("publicando modelos en produccion")
    # monitoreo de cambio en distribucion de datos
    print("activando monitoreo de drift")
    # automatizacion de reentrenamiento
    print("configurando reentrenamiento automatico")
    print("estado: produccion activa")


# =========================
# dashboards ejecutivos
# =========================
# esta etapa representa la capa de business intelligence
# se alimenta directamente desde el data warehouse

def actualizar_dashboards():
    print("\n========== bi / dashboards ==========")
    # herramientas de visualizacion utilizadas
    print("actualizando herramientas bi:")
    print("- power bi")
    print("- tableau")
    print("- looker")

    # indicadores clave del negocio
    print("\nindicadores actualizados:")
    print("- ventas totales")
    print("- ticket promedio")
    print("- retencion de clientes")
    print("- inventario")

    # fuente de datos principal
    print("fuente: data warehouse")
    print("estado: dashboards actualizados")


# =========================
# configuracion dag
# =========================
# parametros generales del pipeline de machine learning

default_args = {
    "owner": "ml-engineering",  # equipo responsable del pipeline ml
    "retries": 3,  # reintentos automaticos en caso de fallo
    "retry_delay": timedelta(minutes=10),  # tiempo entre reintentos
}

# definicion del dag de mlops y business intelligence
with DAG(
    dag_id="dag_05_mlops_dashboards",  # identificador unico del dag
    description="pipeline mlops completo + dashboards ejecutivos",
    schedule="@daily",  # ejecucion diaria automatica
    start_date=datetime(2026, 1, 1),  # fecha de inicio
    catchup=False,  # evita ejecuciones historicas
    default_args=default_args,
    tags=["mlops", "machine-learning", "bi", "dashboards"],
) as dag:

    # =========================
    # tareas del pipeline mlops
    # =========================

    t1 = PythonOperator(
        task_id="train",
        python_callable=entrenar_modelos
    )

    t2 = PythonOperator(
        task_id="evaluate",
        python_callable=evaluar_modelos
    )

    t3 = PythonOperator(
        task_id="deploy",
        python_callable=desplegar_modelos
    )

    t4 = PythonOperator(
        task_id="dashboards",
        python_callable=actualizar_dashboards
    )

    # orquestacion secuencial del pipeline mlops
    t1 >> t2 >> t3 >> t4


# =========================
# ejecucion 
# =========================


if __name__ == "__main__":
    entrenar_modelos()
    evaluar_modelos()
    desplegar_modelos()
    actualizar_dashboards()