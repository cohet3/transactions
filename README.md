# Optimización y Análisis de Transacciones Bancarias Usando Azure y Databricks

## Descripción del Proyecto
Este proyecto automatiza la ingesta, transformación y análisis de grandes volúmenes de datos bancarios. Utilizando Azure Data Factory, Databricks, y Power BI, procesamos transacciones simuladas para generar métricas clave y visualizaciones interactivas.

## Tecnologías Utilizadas
- **Azure**:
  - Azure Blob Storage
  - Azure Data Factory
  - Azure Databricks
- **Lenguajes**: Scala
- **Visualización**: Power BI
- **Big Data Framework**: Apache Spark

## Flujo de Trabajo
![Pipeline Diagram](images/Diagrama%20de%20flujo.png)

1. **Ingesta de datos**: Archivos de transacciones cargados a Azure Blob Storage.
2. **Transformación**: ADF ejecuta un notebook en Databricks para procesar datos.
3. **Carga**: Resultados guardados en Blob Storage.
4. **Visualización**: Dashboard en Power BI.

## Ejecución del Proyecto
1. Configura los servicios de Azure.
2. Clona este repositorio.
3. Carga los notebooks en Databricks.
4. Ejecuta el pipeline en Azure Data Factory.

## Resultados
### Gráfico de Barras: Transacciones por Ubicación
### Gráfico de Líneas: Tendencia Temporal
### Mapa Interactivo: Transacciones por Ubicación
![Gráfico Líneas](images/power.gif)
<!-- <video width="640" height="360" controls>
  <source src="images/power.gif" type="video/mp4">
  Tu navegador no soporta reproducción de video.
</video> -->
## Dataset
[Descarga Dataset](data/transactions.csv)
- **transacciones.csv**:
  - Transaction_ID, Account_ID, Amount, Date, Location, Type
  - Los datos han sido generados artificialmente para poner en práctica el despligue en la nube con grandes volumenes de datos y configurar un sencillo pipeline para comprender su funcionamiento todo con fines educativos.