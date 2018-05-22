

# Added libraries.
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import numpy as np

pd.set_option('display.max_columns', 60)

defun = spark.read.csv('s3://daniel-sharp/decesos/defun_2016.csv', header =True) 
causas = spark.read.csv('s3://daniel-sharp/decesos/decatcausa.csv', header =True) 
estados = spark.read.csv('s3://daniel-sharp/decesos/decateml.CSV', header =True) 
genero = spark.read.csv('s3://daniel-sharp/decesos/desexo.csv', header =True) 
edad = spark.read.csv('s3://daniel-sharp/decesos/deedad.csv', header =True) 
escolaridad = spark.read.csv('s3://daniel-sharp/decesos/deesco.csv', header =True) 

for colum in defun.columns:
    defun = defun.withColumn(colum, regexp_replace(colum, '\t', ''))
    
numerics = ['anio_ocur','horas','anio_nacim']

for colum in numerics:
    defun = defun.withColumn(colum, col(colum).cast("double"))

estados = estados.withColumn('cve_ent', regexp_replace('cve_ent', '\t', '')).withColumn('cve_mun', regexp_replace('cve_mun', '\t', '')).filter('cve_mun == 000')

defun_base = defun.alias('a').join(escolaridad.alias('b'), col('a.escolarida') == col('b.CVE')).withColumn('edad', defun.anio_ocur - defun.anio_nacim).filter('edad < 300').filter('edad > 0').filter('horas < 24').filter('escolarida < 24').withColumn('id', monotonically_increasing_id()).withColumn("anios_edu", col("anios_edu").cast("double")).drop('escolarida')

defun_base.limit(10).show()


# Elegimos utilizar las variables de edad y anios de educación para ejecutar el modelo de k-means pues son la que identificamos que mejor separan las observaciones en causas de defunción. Intentamos ejecutar el modelo con otras combinaciones de variables, por ejemplo: incluyendo año de defunción y hora de defunción pero vimos que no eran informativas y metían ruido en los clústers. Además, elegimos utilizar 4 clústers porque, bajo nuestro objetivo de segmentar las causas de defunción, lograbamos la mayor heterogeneidad de causas entre los clusters.

features = ['edad','anios_edu']

from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml import Pipeline

scaler = StandardScaler(inputCol='grouped_features', outputCol= 'features', withMean=True)
assembler = VectorAssembler(inputCols = features, outputCol = 'grouped_features')
kmeans = KMeans(k=4, seed=1)
pipe = Pipeline(stages = [assembler, scaler, kmeans])

model = pipe.fit(defun_base).transform(defun_base)


# Como métrica adicional para evaluar nuestro modelo utilizamos la función de Clustering Evaluator, que calcula la medida de Silueta utilizando la distancia Euclidiana cuadrada. Su valor va de -1 a 1 y un valor más cercano a uno indica una mejor separación entre los grupos y menor separación entre los grupos.  
# 
# Obtuvimos un valor de 0.51, que es relativamente bueno.

evaluator = ClusteringEvaluator()
evaluator.evaluate(model)

# El modelo separó los datos en 4 grupos. El número de registros en cada uno se muestra a continuación:

model.groupBy('prediction').count().show()

# El modelo segmentó por edades, generando un grupo de 'jóvenes' (grupo 2) y 3 de adultos mayores:

print("Analisis Descriptivo para Edad")
model.groupBy('prediction').agg((mean(model.edad)).alias('Promedio'),                                      (stddev(model.edad)).alias('Desviación'),                                     (max(model.edad)).alias('Máximo'),                                     (min(model.edad)).alias('Mínimo')).sort('prediction').show()

# En cuanto a años de estudio, hay un grupo de 'alta educación' (grupo 3), uno de baja educación que corresponde con el de los más ancianos (grupo 0). Como intermedios están el grupo 1 y 2.

print("Analisis Descriptivo para años de estudio")
model.groupBy('prediction').agg((mean(model.anios_edu)).alias('Promedio'),                                      (stddev(model.anios_edu)).alias('Desviación'),                                     (max(model.anios_edu)).alias('Máximo'),                                     (min(model.anios_edu)).alias('Mínimo')).sort('prediction').show()

# En cuanto a diferenciación en las enfermedades de cada grupo, estas varían y se pueden explicar por las diferencias en edad promedio en los grupos. Por ejemplo, la principal causa de muerte en el grupo de los jóvenes es la violencia. Mientras que la de mayor incidencia en los otros es infarto. En el grupo de mayor edad aparecen enfermedades respiratorias como otras causas comunes. En los grupos intermedios aparece la cirrosis como una causa común.

causas_por_grupo = model.alias('a').groupBy('prediction','a.causa_def').count().select("*",dense_rank().over(Window.partitionBy('prediction').orderBy(desc('count'))).alias('rn')).filter('rn < 6').join(causas.alias('b'), col('a.causa_def') == col('b.CVE')).drop('causa_def','CVE')

causas_por_grupo.show()

# Como se puede observar en la tabla anterior, las causas de muerte varían en los diferentes grupos pues la edad esta claramente correlacionada con la causa de muerte. Por ejemplo, el grupo 2, correspondiente a los jóvenes, muestra causas relacionadas con violencia y accidentes. Mientras que en los grupos de mayor edad promedio están relacionadas con enfermedades. Por otro lado, dentro de estos grupos, las enfermedades también varían, por ejemplo, con el grupo 0, que es el de edad promedio de 85, las causas de muerte se diferencian de los otros por incluir problemas respiratorios.

causas_por_estado = model.alias('a').groupBy('prediction','a.ent_ocurr').count().select("*",dense_rank().over(Window.partitionBy('prediction').orderBy(desc('count'))).alias('rn')).filter('rn < 6').join(estados.alias('b'), col('a.ent_ocurr') == col('b.cve_ent')).drop('ent_ocurr','cve_ent','cve_loc','cve_mun')

causas_por_estado.show()

# No hay mucha diferenciación en los estados donde ocurren los decesos, como es de esperar en todos los grupos se encuentra la Ciudad de México y el Estado de México en los primeros lugares.

model.groupBy('prediction').agg((mean(model.sexo)-1).alias('% mujeres')).sort('prediction').show()

# Es interesante notar que en el grupo de los más jóvenes (grupo 2), donde las causas de muerte están relacionadas con violencia, únicamente 3 de cada 10 muertes son mujeres. Esta proporción va aumentando junto con el rango de edades, que se puede explicar a que, dado que las mujeres tienen mayor esperanza de vida, la proporción de mujeres del total de la población va aumentando con la edad.

print("Analisis Descriptivo para Horas de Defunción")
model.groupBy('prediction').agg((mean(model.horas)).alias('Promedio'),                                      (stddev(model.horas)).alias('Desviación'),                                     (max(model.horas)).alias('Máximo'),                                     (min(model.horas)).alias('Mínimo')).sort('prediction').show()

# No hay diferencia en esta variable.

# ### Análisis de resultados:
# 
# Elegimos utilizar 4 grupos porque observamos que con esta configuración se logra hacer una separación de las observaciones en grupos característicos, diferenciados principalmente por la variable 'edad' y por ende, de causas de muerte. La variable de año no aporta información para segmentar, pues más del 90% de las observaciones ocurrieron en 2015 y 2016. 
