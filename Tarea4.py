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
sorted_df.write.mode("overwrite").parquet(output_path)
