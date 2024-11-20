# Databricks notebook source
# MAGIC %scala
# MAGIC // Configuración de la cuenta de almacenamiento
# MAGIC val storageAccountName = "storageproyectobanco123"
# MAGIC val containerName = "transacciones" // Asegúrate de que el contenedor esté creado en la cuenta de almacenamiento
# MAGIC val storageAccountKey = "" // Reemplaza esto con la clave de acceso copiada
# MAGIC
# MAGIC // Configurar acceso en Databricks
# MAGIC spark.conf.set(s"fs.azure.account.key.${storageAccountName}.blob.core.windows.net", storageAccountKey)
# MAGIC
# MAGIC // Ruta del contenedor
# MAGIC val filePath = s"wasbs://${containerName}@${storageAccountName}.blob.core.windows.net/"
# MAGIC println(s"Conexión configurada a $filePath")
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC // Listar archivos en el contenedor
# MAGIC dbutils.fs.ls(filePath)
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.conf.set(s"fs.azure.account.key.${storageAccountName}.blob.core.windows.net", storageAccountKey)
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.{DataFrame, SparkSession}
# MAGIC import org.apache.spark.sql.functions._
# MAGIC import scala.util.Random
# MAGIC
# MAGIC // Crear un DataFrame de transacciones sintéticas
# MAGIC val numTransactions = 100000 // Número de transacciones que deseas generar
# MAGIC
# MAGIC // Función para generar un DataFrame con datos sintéticos
# MAGIC def generateSyntheticData(spark: SparkSession, numRows: Int): DataFrame = {
# MAGIC   import spark.implicits._
# MAGIC
# MAGIC   val transactionTypes = Seq("Depósito", "Retiro", "Transferencia", "Pago")
# MAGIC   val locations = Seq("Madrid", "Barcelona", "Valencia", "Sevilla", "Zaragoza")
# MAGIC
# MAGIC   val data = (1 to numRows).map { id =>
# MAGIC     val transactionID = s"T${id}"
# MAGIC     val accountID = s"A${Random.nextInt(10000) + 1}"
# MAGIC     val amount = BigDecimal((Random.nextDouble() * 6000) - 1000).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
# MAGIC     val date = java.sql.Timestamp.valueOf(s"2023-${Random.nextInt(12) + 1}-${Random.nextInt(28) + 1} ${Random.nextInt(24)}:${Random.nextInt(60)}:${Random.nextInt(60)}")
# MAGIC     val location = locations(Random.nextInt(locations.length))
# MAGIC     val transactionType = transactionTypes(Random.nextInt(transactionTypes.length))
# MAGIC
# MAGIC     (transactionID, accountID, amount, date, location, transactionType)
# MAGIC   }
# MAGIC
# MAGIC   // Convertir la secuencia a DataFrame
# MAGIC   spark.createDataFrame(data).toDF("Transaction_ID", "Account_ID", "Amount", "Date", "Location", "Type")
# MAGIC }
# MAGIC
# MAGIC // Generar el DataFrame de transacciones
# MAGIC val transactionsDF = generateSyntheticData(spark, numTransactions)
# MAGIC
# MAGIC // Mostrar los primeros registros para verificar
# MAGIC transactionsDF.show(10)
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC // Ruta de almacenamiento en Azure Blob
# MAGIC val outputPath = s"${filePath}transacciones.csv"
# MAGIC
# MAGIC // Guardar el DataFrame como archivo CSV en Azure Blob Storage
# MAGIC transactionsDF.write
# MAGIC   .mode("overwrite")
# MAGIC   .option("header", "true")
# MAGIC   .csv(outputPath)
# MAGIC
# MAGIC println(s"Archivo de transacciones guardado en: $outputPath")
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC val filePath = "wasbs://transacciones@storageproyectobanco123.blob.core.windows.net/transacciones.csv/"
# MAGIC val transactionsDF = spark.read
# MAGIC   .option("header", "true")
# MAGIC   .option("inferSchema", "true")
# MAGIC   .csv(filePath)
# MAGIC
# MAGIC // Mostrar algunos registros para verificar que se cargaron correctamente
# MAGIC transactionsDF.show(10)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Conteo Total de Transacciones:

# COMMAND ----------

# MAGIC %scala
# MAGIC val totalTransacciones = transactionsDF.count()
# MAGIC println(s"Total de transacciones: $totalTransacciones")
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Conteo de Transacciones por Tipo:

# COMMAND ----------

# MAGIC %scala
# MAGIC val transaccionesPorTipo = transactionsDF.groupBy("Type").count()
# MAGIC transaccionesPorTipo.show()
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Monto Promedio de las Transacciones:

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.functions._
# MAGIC
# MAGIC val promedioMonto = transactionsDF.agg(avg("Amount")).first().getDouble(0)
# MAGIC println(s"Monto promedio de las transacciones: $$ $promedioMonto")
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Monto Total de Transacciones por Ubicación:

# COMMAND ----------

# MAGIC %scala
# MAGIC val totalMontoPorUbicacion = transactionsDF.groupBy("Location")
# MAGIC   .agg(sum("Amount").alias("Total_Monto"))
# MAGIC   .orderBy(desc("Total_Monto"))
# MAGIC
# MAGIC totalMontoPorUbicacion.show()
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Distribución de Transacciones por Fecha:

# COMMAND ----------

# MAGIC %scala
# MAGIC val transaccionesPorFecha = transactionsDF.groupBy("Date")
# MAGIC   .count()
# MAGIC   .orderBy("Date")
# MAGIC
# MAGIC transaccionesPorFecha.show()
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Exportar Datos

# COMMAND ----------

# MAGIC %scala
# MAGIC // Exportar total de montos por ubicación como CSV
# MAGIC totalMontoPorUbicacion.write
# MAGIC   .mode("overwrite")
# MAGIC   .option("header", "true")
# MAGIC   .csv(s"${filePath}resultado_monto_por_ubicacion")
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC val transaccionesPorTipo = transactionsDF.groupBy("Type").count()
# MAGIC transaccionesPorTipo.show()
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC val totalMontoPorUbicacion = transactionsDF.groupBy("Location")
# MAGIC   .agg(sum("Amount").alias("Total_Monto"))
# MAGIC   .orderBy(desc("Total_Monto"))
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC totalMontoPorUbicacion.write
# MAGIC   .mode("overwrite")
# MAGIC   .option("header", "true")
# MAGIC   .csv(s"${filePath}resultado_monto_por_ubicacion")
# MAGIC