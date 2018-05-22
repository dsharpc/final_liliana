


# Added libraries.
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window


defun = spark.read.csv('s3://daniel-sharp/decesos/defun_2016.csv', header =True) 
causas = spark.read.csv('s3://daniel-sharp/decesos/decatcausa.csv', header =True) 
estados = spark.read.csv('s3://daniel-sharp/decesos/decateml.CSV', header =True) 
genero = spark.read.csv('s3://daniel-sharp/decesos/desexo.csv', header =True) 
edad = spark.read.csv('s3://daniel-sharp/decesos/deedad.csv', header =True) 


estados_clean = estados.withColumn('cve_ent', regexp_replace('cve_ent', '\t', '')).withColumn('cve_mun', regexp_replace('cve_mun', '\t', '')).filter('cve_mun == 000')
defun_c = defun.withColumn('ent_ocurr', regexp_replace('ent_ocurr', '\t', ''))
genero = genero.selectExpr("DESCRIP as descrip_sexo", "CVE as cve_sexo")
edad = edad.selectExpr("DESCRIP as descrip_edad", "CVE as cve_edad")


# a. ¿Cuántos casos de defunción por Hepatitis A hay por entidad? (B150, B159) ordena la salida de forma descendente por número de decesos

defun_c.alias('a').join(causas.alias('b'),col('a.causa_def') == col('b.cve')).join(estados_clean.alias('c'),col('a.ent_ocurr') == col('c.cve_ent')).filter((defun_c['causa_def'] == 'B150') | (defun_c['causa_def'] == 'B159')).groupBy('nom_loc').count().sort(desc('count')).show()


# b. ¿Cuál es la 6ta causa de defunción por entidad y sexo para las personas de 53 años?


defun_c.alias('a').join(causas.alias('b'),col('a.causa_def') == col('b.cve')).join(estados_clean.alias('c'),col('a.ent_ocurr') == col('c.cve_ent')).join(genero.alias('d'),col('a.sexo') == col('d.cve_sexo')).join(edad.alias('e'),col('a.edad') == col('e.cve_edad')).filter("descrip_edad like '%Cincuenta y tres años%'").groupBy('nom_loc','DESCRIP','descrip_sexo').count().select("*",dense_rank().over(Window.partitionBy('nom_loc', 'descrip_sexo').orderBy(desc('count'))).alias('rn')).filter('rn == 6').show()

