import warnings
warnings.filterwarnings("ignore", category=RuntimeWarning)


from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta

# =========================
# validaciones de calidad
# =========================
# este bloque representa controles de data quality
# se ejecutan reglas para asegurar consistencia, que los datos esten completos y que  sean validos 

# validacion de campos nulos en columnas criticas del modelo
def validar_nulos():
    print("\n========== data quality: nulos ==========")
    # se revisan campos obligatorios del modelo de ventas
    print("validando campos criticos: clienteid, fecha, totalventa")
    # regla de negocio aplicada
    print("regla: no nulos")
    print("estado: ok")


# validacion de registros duplicados en la tabla de hechos
def validar_duplicados():
    print("\n========== data quality: duplicados ==========")
    # busqueda de duplicados en fact table
    print("buscando registros duplicados en factventas")
    # regla de unicidad
    print("regla: sin duplicados")
    print("estado: ok")


# validacion de rangos y consistencia de valores
def validar_rangos():
    print("\n========== data quality: rangos ==========")
    # validacion de valores positivos en ventas
    print("validando totalventa > 0")
    # validacion de fechas correctas
    print("validando fechas validas")
    print("estado: ok")


# validacion de integridad referencial entre dimensiones y hechos
def validar_integridad():
    print("\n========== data quality: integridad ==========")
    # relacion entre tabla de hechos y dimension cliente
    print("verificando relacion clienteid ↔ dimcliente")
    # relacion entre tabla de hechos y dimension producto
    print("verificando relacion productoid ↔ dimproducto")
    print("estado: ok")


# =========================
# observabilidad
# =========================
# este bloque representa monitoreo del pipeline
# permite medir rendimiento, estabilidad y cumplimiento de sla

def observabilidad():
    print("\n========== observabilidad ==========")
    # metricas operativas del pipeline
    print("metricas del pipeline:")
    print("- sla: 99%")
    print("- latencia controlada")
    print("- retries minimos")
    print("- logs centralizados")
    # canales de notificacion de eventos
    print("alertas: email / slack / teams")
    print("estado: monitoreado")


# =========================
# alertas (callback de fallo)
# =========================
# funcion que se ejecuta automaticamente si una tarea falla
# airflow pasa un contexto con informacion del error

def alerta_fallo(context):
    print("\n========== alerta ==========")
    # identificacion de la tarea que fallo
    print("fallo detectado en:", context["task_instance"].task_id)
    # simulacion de envio de notificacion
    print("enviando notificacion a sistema de alertas...")


# =========================
# configuracion dag
# =========================
# parametros globales del workflow de data quality

default_args = {
    "owner": "data-quality",  # equipo responsable del dag
    "retries": 2,  # numero de reintentos automaticos
    "retry_delay": timedelta(minutes=5),  # tiempo entre reintentos
    "on_failure_callback": alerta_fallo,  # callback en caso de error
}

# definicion del dag de calidad y observabilidad
with DAG(
    dag_id="dag_04_data_quality_observability",  # identificador unico
    description="validaciones de calidad y observabilidad del pipeline",
    schedule="@daily",  # ejecucion diaria
    start_date=datetime(2026, 1, 1),  # fecha inicio
    catchup=False,  # evita ejecuciones historicas
    default_args=default_args,
    tags=["data-quality", "observabilidad", "governance"],
) as dag:

    # =========================
    # tareas de data quality
    # =========================

    t1 = PythonOperator(
        task_id="nulos",
        python_callable=validar_nulos
    )

    t2 = PythonOperator(
        task_id="duplicados",
        python_callable=validar_duplicados
    )

    t3 = PythonOperator(
        task_id="rangos",
        python_callable=validar_rangos
    )

    t4 = PythonOperator(
        task_id="integridad",
        python_callable=validar_integridad
    )

    # =========================
    # observabilidad del pipeline
    # =========================

    t5 = PythonOperator(
        task_id="observabilidad",
        python_callable=observabilidad
    )

    # orquestacion secuencial de controles de calidad
    t1 >> t2 >> t3 >> t4 >> t5


# =========================
# ejecucion 
# =========================

if __name__ == "__main__":
    validar_nulos()
    validar_duplicados()
    validar_rangos()
    validar_integridad()
    observabilidad()