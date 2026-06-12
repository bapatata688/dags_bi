# Plataforma Moderna de Ingenieria de Datos

Repositorio: https://github.com/bapatata688/dags_bi

Proyecto desarrollado para Level Up.  
Docente: Diego Menendez  
Integrantes: Diego Alejandro Escobar Barahona, Daniella Marissa Navarro Araniva

---

## Descripcion

Implementacion de una plataforma de ingenieria de datos construida sobre Apache Airflow. El proyecto orquesta el ciclo de vida completo del dato mediante cinco DAGs especializados: desde la extraccion en sistemas ERP y APIs externas, pasando por transformaciones en arquitectura Lakehouse, validaciones de calidad y observabilidad, hasta el despliegue de modelos de Machine Learning y actualizacion de dashboards ejecutivos.

---

## Estructura del Repositorio

```
dags_bi/
├── dag_01_ingestion_erp_crm.py
├── dag_02_ingestion_apis.py
├── dag_03_lakehouse_transformations.py
├── dag_04_calidad_observabilidad.py
└── dag_05_mlops_dashboards.py
```

---

## DAGs

### dag_01_ingestion_erp_crm.py

Extrae datos desde el sistema SAP ERP hacia la capa Raw del Data Lake. Implementa un pipeline secuencial de tres tareas: conexion al sistema fuente, extraccion de registros y almacenamiento en el Data Lake.

**Patron:** Pipeline secuencial  
**Schedule:** `@daily`  
**Owner:** `data_engineering`  
**Retries:** 2, cada 5 minutos  
**Flujo de tareas:**

```
conectar_sap >> extraer_datos_erp >> guardar_data_lake
```

---

### dag_02_ingestion_apis.py

Orquesta la extraccion de datos desde tres APIs externas hacia la capa Raw del Data Lake en formato JSON particionado por fecha.

**Fuentes:**

- Shopify E-Commerce — ordenes, productos, clientes
- Facebook Ads — campanas, adsets, insights
- Zendesk Support — tickets, usuarios, CSAT ratings

**Patron:** Fan-Out / Fan-In  
**Schedule:** `0 3 * * *` (03:00 AM diario)  
**Owner:** `data_engineering_team`  
**Retries:** 3, cada 10 minutos  
**Flujo de tareas:**

```
validate_api_tokens
    ├── extract_shopify_ecommerce
    ├── extract_facebook_ads
    └── extract_zendesk_support
            └── consolidate_api_results (trigger_rule: all_done)
```

Cada extractor publica su metadata via XCom. La tarea de consolidacion recupera los resultados de las tres extracciones y genera un reporte de auditoria del proceso.

---

### dag_03_lakehouse_transformations.py

Implementa la arquitectura Medallion (Bronze / Silver / Gold) para transformar los datos crudos en datasets analiticos listos para consumo empresarial.

**Capas:**

| Capa   | Path                    | Descripcion                                           |
| ------ | ----------------------- | ----------------------------------------------------- |
| Bronze | `/data_lake/raw/`       | Datos crudos tal como llegaron de los sistemas fuente |
| Silver | `/data_lake/processed/` | Datos limpios, estandarizados y deduplicados          |
| Gold   | `/data_lake/curated/`   | Datasets analiticos agregados por dominio de negocio  |

**Transformaciones Silver:**

- `clean_erp_crm_data` — elimina duplicados, normaliza fechas, estandariza columnas
- `clean_billing_risk_data` — convierte monedas a USD, valida rangos de montos
- `clean_external_apis_data` — aplana JSON anidados, estandariza zonas horarias

**Transformaciones Gold:**

- `build_sales_analytics_dataset` — ingresos netos, margen por producto
- `build_customer_360_dataset` — LTV, segmento de riesgo por cliente
- `build_marketing_roi_dataset` — ROAS por campana, costo por adquisicion

**Schedule:** `@daily`  
**Owner:** `data_engineering_team`  
**Retries:** 2, cada 5 minutos

---

### dag_04_calidad_observabilidad.py

Pipeline de validaciones de calidad de datos seguido de una tarea de observabilidad del pipeline. Incluye un callback de fallo que notifica automaticamente ante cualquier error de tarea.

**Validaciones implementadas:**

| Task ID          | Que valida                                    |
| ---------------- | --------------------------------------------- |
| `nulos`          | Campos criticos sin valores nulos             |
| `duplicados`     | Registros unicos por clave primaria           |
| `rangos`         | Valores dentro de rangos de negocio definidos |
| `integridad`     | Integridad referencial entre tablas           |
| `observabilidad` | Metricas SLA, latencia y estado de logs       |

**Flujo de tareas:**

```
nulos >> duplicados >> rangos >> integridad >> observabilidad
```

**Callback de error:** `alerta_fallo(context)` registrado en `on_failure_callback`. Reporta el `task_id` fallido y simula notificacion a sistemas de alerta (Slack / Teams / PagerDuty).

**Schedule:** `@daily`  
**Owner:** `data-quality`  
**Retries:** 2, cada 5 minutos  
**Tags:** `data-quality`, `observabilidad`, `governance`

---

### dag_05_mlops_dashboards.py

Cierra el ciclo de vida del dato transformando los datasets de la capa Gold en modelos predictivos en produccion y dashboards ejecutivos actualizados.

**Pipeline MLOps:**

| Task ID      | Accion                                           | Metricas de referencia                               |
| ------------ | ------------------------------------------------ | ---------------------------------------------------- |
| `train`      | Entrenamiento de modelos desde el Data Warehouse | Ventas, churn de clientes, segmentacion              |
| `evaluate`   | Validacion de metricas de desempeno              | Accuracy: 0.87, RMSE: 12.4, Silhouette: 0.71         |
| `deploy`     | Publicacion en produccion y monitoreo de drift   | Reentrenamiento automatico activado                  |
| `dashboards` | Actualizacion Power BI, Tableau y Looker         | KPIs: ventas, ticket promedio, retencion, inventario |

**Flujo de tareas:**

```
train >> evaluate >> deploy >> dashboards
```

**Schedule:** `@daily`  
**Owner:** `ml-engineering`  
**Retries:** 3, cada 10 minutos  
**Tags:** `mlops`, `machine-learning`, `bi`, `dashboards`

---

## Orden de Ejecucion del Pipeline

| DAG    | Horario  | Depende de      | Habilita      |
| ------ | -------- | --------------- | ------------- |
| DAG 01 | 02:00 AM | SAP ERP         | DAG 03        |
| DAG 02 | 03:00 AM | APIs externas   | DAG 03        |
| DAG 03 | 05:00 AM | DAG 01 y DAG 02 | DAG 04        |
| DAG 04 | 06:00 AM | DAG 03          | DAG 05        |
| DAG 05 | 07:00 AM | DAG 04          | Dashboards BI |

---

## Requisitos

- Apache Airflow 2.x
- Python 3.10+
- Providers: `apache-airflow-providers-standard`

Instalacion de dependencias:

```bash
pip install apache-airflow
pip install apache-airflow-providers-standard
```

---

## Ejecucion Local (sin Airflow)

Todos los DAGs incluyen un bloque `__main__` que permite ejecutar las funciones de negocio directamente para pruebas:

```bash
python dag_01_ingestion_erp_crm.py
python dag_02_ingestion_apis.py
python dag_03_lakehouse_transformations.py
python dag_04_calidad_observabilidad.py
python dag_05_mlops_dashboards.py
```

---

## Convenciones de Codigo

- Nomenclatura de archivos: `dag_NN_nombre_descriptivo.py`, numerado por orden de ejecucion
- Funciones en `snake_case`
- `logging.getLogger(__name__)` en lugar de `print()` en DAGs de produccion (DAG 02 y 03)
- Constantes centralizadas a nivel de modulo
- `default_args` como diccionario reutilizado por todas las tareas del DAG
- Manejo de errores con `try/except`, logging del error y re-raise para que Airflow detecte el fallo

---

## Arquitectura de Datos

```
Sistemas fuente
    SAP ERP / Salesforce CRM
    Shopify / Facebook Ads / Zendesk
            |
            v
    Data Lake — Capa Raw (Bronze)
            |
            v
    Data Lake — Capa Processed (Silver)
            |
            v
    Data Lake — Capa Curated (Gold)
            |
        +---+---+
        |       |
        v       v
    MLOps   Dashboards BI
              Power BI / Tableau / Looker
```
