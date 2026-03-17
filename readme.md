# Sistema ETL y Base de Datos Analítica

## Descripción

Este proyecto implementa un sistema **ETL (Extract, Transform, Load)** desarrollado en **Python**, cuyo objetivo es integrar datos provenientes de diferentes fuentes, almacenarlos en una capa de **staging** y cargarlos posteriormente en una **base de datos analítica** para su análisis y visualización.

El sistema permite procesar datos provenientes de:

- Archivos CSV  
- Bases de datos relacionales (SQL Server)  
- APIs REST  

Los datos procesados pueden visualizarse mediante un **dashboard web**.

---

# Arquitectura del Sistema

El flujo general del sistema es el siguiente:

1. Extracción de datos desde diferentes fuentes.
2. Procesamiento mediante el servicio ETL.
3. Almacenamiento temporal en tablas de staging.
4. Carga de datos en la base de datos analítica.
5. Visualización de información mediante un dashboard.

Flujo simplificado:

Fuentes de Datos → ETL Worker → Staging → Base Analítica → Dashboard

---

# Estructura del Proyecto

El proyecto está organizado en módulos para facilitar su mantenimiento y escalabilidad.

---

# Componentes Principales

### CsvExtractor
Extrae datos desde archivos CSV utilizando **pandas**.

### DatabaseExtractor
Obtiene datos desde bases de datos relacionales usando **SQLAlchemy y pyodbc**.

### ApiExtractor
Consume datos desde **APIs REST** utilizando solicitudes HTTP.

### StagingDataLoader
Carga los datos extraídos en tablas staging y archivos temporales.

### LoggerService
Registra logs, errores y métricas del proceso ETL.

### EtlWorkerService
Orquesta la ejecución del proceso ETL y ejecuta los extractores en paralelo.

---
# Instalacion y Ejecucion
instalacion:

pip install -r requirements.txt


Arranque del projecto:


python Proceso.py

# Base de Datos Analítica

La base de datos analítica almacena los datos procesados por el ETL y permite realizar consultas para análisis.

Ejemplo de tabla analítica:

```sql
CREATE TABLE ventas_analitica (
    id INTEGER PRIMARY KEY,
    fecha DATE,
    cliente TEXT,
    producto TEXT,
    cantidad INTEGER,
    total REAL
);

SELECT cliente, SUM(total) AS total_ventas
FROM ventas_analitica
GROUP BY cliente;

import streamlit as st
import sqlite3
import pandas as pd

conn = sqlite3.connect('analitica/analitica.db')
df = pd.read_sql_query('SELECT * FROM ventas_analitica', conn)

st.dataframe(df)


