# Hadoop_Spark_And_Spark_Streaming_UNAD_2025
## Introducción
En este repositorio podrán encontrar la solución a la actividad de la fase 3 de la materia de BigData de la Universidad Nacional Abierta y a Distancia, la cual consiste en 2 partes, la primera consiste en obtener una fuente de datos de datos abiertos de Colombia, con el propósito de almacenarlo en el HDFS (Hadoop File System) para en un momento posterior, análizar estos datos, convirtiendolos en un DataFrame con ayuda de Spark para facilitar su tratamiento, su limpieza y su tratamiento para poder obtener componentes estadísticos de estos datos y realizar un debido tratamiento.

La fuente de datos elegida es acerca de los MEDICAMENTOS VITALES NO DISPONIBLES, nos presenta datos acerca de Medicamentos de gran importancia que no se encuentran disponibles debido a la gran demanda de otras compañías.

Esta fuente de datos, podrán encontrarla en la carpeta `\data` de este repositorio.

La segunda parte de esta actividad se enfoca en la librería Kafka, librería que permite generar datos masivos en tiempo real con el fín de realizar un ejercicio de análisis de estos datos en tiempo real. y nos permite darle uso a la librería para generar un archivo productor de datos aleatorios en tiempo real durante la ejecución del programa. Posteriormente con ayuda de SparkStreaming tomaremos los datos en tiempo real y los analizaremos uno por uno y mostraremos los resultados por consola.

# Ejercicio 1: Hadoop y Spark con uso de DataFrames

Como se mencionó anteriormente, en este ejercicio manejaremos la fuente de datos de MEDICAMENTOS VITALES NO DISPONIBLES encontrada en Datos Abiertos de Colombia, que podemos encontrar en este enlace [Medicamentos Vitales No Disponibles](https://www.datos.gov.co/Salud-y-Protecci-n-Social/MEDICAMENTOS-VITALES-NO-DISPONIBLES/sdmr-tfmf/about_data) obtendremos estos datos con el comando:
```bash
wget https://www.datos.gov.co/resource/sdmr-tfmf.csv
```
con esto, nos habrá descargado el archivo CSV correspondiente y podremos manipularlo con Hadoop.

Posteriormente, luego de haber ejecutado el Cluster de Hadoop en nuestro servidor Ubuntu podremos crear la carpeta donde se almacenará nuestra fuente de datos:
```bash
hdfs dfs -mkdir /Tarea4
```
Para luego copiar nuestra fuente de datos en este directorio.
```bash
hdfs dfs -put /home/hadoop/sdmr-tfmf.csv /Tarea4
```

Posteriormente, iniciando una nueva instancia de nuestro Putty, e iniciando con el usuario vboxuser, podremos crear el archivo `tarea4.py` en el cual, daremos uso a la librería pyspark para obtener la fuente de datos de hadoop y analizarla fácilmente.
```python
#Importamos librerias necesarias
import findspark
findspark.init('/opt/spark')   # <- ruta a tu SPARK_HOME; ajusta si es otra
from pyspark.sql import SparkSession, functions as F

# Inicializa la sesión de Spark
spark = SparkSession.builder.appName('Tarea4').master('local[*]').config('spark.hadoop.fs.defaultFS', 'hdfs://localhost:9000').getOrCreate()

# Define la ruta del archivo .csv en HDFS
file_path = 'hdfs://localhost:9000/Tarea4/rows2.csv'

# Lee el archivo .csv
df = spark.read.format('csv').option('header','true').option('inferSchema', 'true').load(file_path)

# imprimimos el esquema
df.printSchema()

# Muestra las primeras filas del DataFrame
df.show()

df_clean = df.dropna()
df_clean = df_clean.withColumnRenamed('fecha_de_autorizaci_n', 'fecha_autorizacion')
df_clean = df_clean.withColumnRenamed('principio_activo1', 'nombre_medicamento')
df_clean = df_clean.withColumnRenamed('cantidad_solicitada', 'cantidad')

# Número de registros
print("Total de registros:", df_clean.count())

# Estadisticas básicas
df_clean.summary().show()

# Resumen estadístico
df_clean.describe().show()

# Consulta: Filtrar por Cantidad de Medicamento y seleccionar columnas
print("Medicamentos con más de 1000 cantidades existentes solicitados \n")
medicamentos = df_clean.filter(F.col('cantidad_solicitada') > 1000).select('nombre_medicamento', 'fecha_autorizacion', 'cantidad')
medicamentos.show()

# Ordenar filas por los valores de la columna "fecha_de_autorizaci_n" en orden ascendente
print("Medicamentos ordenados por fecha de autorización en orden ascendente")
sorted_df = medicamentos.sort(F.col('fecha_autorizacion').asc())
sorted_df.show()

# Almacenar los resultados procesados
output_path = 'hdfs://localhost:9000/user/vboxuser/resultados/medicamentos_procesados'
#sorted_df.write.mode("overwrite").parquet(output_path)

```
Posteriormente, si ejecutamos nuestro programa, podremos ver un análisis exploratorio de la fuente de datos más claro gracias a Spark, y podremos encontrar que existen un total de 873 datos en total en donde destacamos la cantidad máxima de medicamentos solicitados de 20.000 y la cantidad minima de 1.

# Ejercicio 2: Kafka con Spark Streaming

Para el desarrollo del ejercicio 2 será necesario realizar la correcta instalación de la librería de Kafka para luego iniciar el servidor ZooKeeper.
```bash
sudo /opt/Kafka/bin/zookeeper-server-start.sh /opt/Kafka/config/zookeeper.properties &
```
Para posteriormente iniciar el servidor de Kafka:
```bash
sudo /opt/Kafka/bin/kafka-server-start.sh /opt/Kafka/config/server.properties &
```
Luego tendremos la posibilidad de crear un tema (topic) de Kafka:
```bash
/opt/Kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic sensor_data
```
Y podremos crear nuestro Productor(producer) de Kafka
```python
import time 
import json 
import random 
from kafka import KafkaProducer
def generate_sensor_data(): 
    return { "sensor_id": random.randint(1, 10), "temperature": round(random.uniform(20, 30), 2), "humidity": round(random.uniform(30, 70), 2), "timestamp": int(time.time()) }
producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: json.dumps(x).encode('utf-8'))
while True: 
    sensor_data = generate_sensor_data() 
    producer.send('sensor_data', value=sensor_data) 
    print(f"Sent: {sensor_data}") 
    time.sleep(1)
```
Esto genera datos simulados de sensores y los envía al tema (topic) de Kafka que creamos anteriormente (sensor_data).
Con esto, estaremos listos para la implementación de nuestro consumidor con Spark Streaming.
```python

#Importamos librerias necesarias
import findspark
findspark.init('/opt/spark')   # <- ruta a tu SPARK_HOME; ajusta si es otra
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, TimestampType
import logging

# Configura el nivel de log a WARN para reducir los mensajes INFO
spark = SparkSession.builder.appName("KafkaSparkStreaming").master('local[*]').config('spark.hadoop.fs.defaultFS', 'hdfs://localhost:9000').getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Definir el esquema de los datos de entrada
schema = StructType([
        StructField("sensor_id", IntegerType()),
        StructField("temperature", FloatType()),
        StructField("humidity", FloatType()),
        StructField("timestamp", TimestampType())
])

# Crear una sesión de Spark
spark = SparkSession.builder.appName("SensorDataAnalysis").master('local[*]').config('spark.hadoop.fs.defaultFS', 'hdfs://localhost:9000').getOrCreate()

# Configurar el lector de streaming para leer desde Kafka
df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "sensor_data").load()

# Parsear los datos JSON
parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Calcular estadisticas por ventana de tiempo
windowed_stats = parsed_df.groupBy(window(col("timestamp"), "1 minute"), "sensor_id").agg({"temperature":"avg", "humidity":"avg"})

# Escribir los resultados en la consola
query = windowed_stats.writeStream.outputMode("complete").format("console").start()

query.awaitTermination()


```
Este script utiliza Spark Streaming para leer datos del tema(topic) de Kafka, procesa los datos en ventanas de tiempo de 1 minuto y calcula la temperatura y humedad promedio para cada sensor.

### Ejecución
Estamos listos para poner a prueba nuestro productor(producer) de Kafka.
```bash
python3 kafka_producer.py
```
y en otra terminal, podremos ejecutar nuestro consumidor(consumer) con Spark Streaming
```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 spark_streaming_consumer.py
```
