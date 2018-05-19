-- Iniciamos la base de datos
drop database if exists defunciones cascade;

create database if not exists defunciones location "/usr/local/deaths/hive/defuncionesdb/";
-- Creamos la tabla de las defunciones
create external table if not exists defunciones.defun (
ent_regis string,
mun_regis string,
ent_resid string,
mun_resid string,
tloc_resid string,
loc_resid string,
ent_ocurr string,
mun_ocurr string,
tloc_ocurr string,
loc_ocurr string,
causa_def string,
lista_mex string,
sexo string,
edad string,
dia_ocurr string,
mes_ocurr string,
anio_ocur string,
dia_regis string,
mes_regis string,
anio_regis string,
dia_nacim string,
mes_nacim string,
anio_nacim string,
ocupacion string,
escolarida string,
edo_civil string,
presunto string,
ocurr_trab string,
lugar_ocur string,
necropsia string,
asist_medi string,
sitio_ocur string,
cond_cert string,
nacionalid string,
derechohab string,
embarazo string,
rel_emba string,
horas string,
minutos string,
capitulo string,
grupo string,
lista1 string,
gr_lismex string,
vio_fami string,
area_ur string,
edad_agru string,
complicaro string,
dia_cert string,
mes_cert string,
anio_cert string,
maternas string,
lengua string,
cond_act string,
par_agre string,
ent_ocules string,
mun_ocules string,
loc_ocules string,
razon_m string,
dis_re_oax string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/usr/local/deaths/hive/defuncionesdb/defun';

LOAD DATA INPATH '/usr/local/deaths/defun_2016.csv' INTO table defunciones.defun;

-- Creamos la tabla que el diccionario de causas
create external table if not exists defunciones.causas (cve string,
descrip string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/usr/local/deaths/hive/defuncionesdb/causas';

LOAD DATA INPATH '/usr/local/deaths/decatcausa.csv' INTO table defunciones.causas;

-- Creamos la tabla que el diccionario de estados
create external table if not exists defunciones.estados (cve_ent string, 
cve_mun string,
cve_loc string,
nom_loc string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/usr/local/deaths/hive/defuncionesdb/estados';

LOAD DATA INPATH '/usr/local/deaths/decateml.CSV' INTO table defunciones.estados;

-- Creamos la tabla que el diccionario de genero
create external table if not exists defunciones.genero (cve_gen string, 
descrip_gen string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/usr/local/deaths/hive/defuncionesdb/genero';

LOAD DATA INPATH '/usr/local/deaths/desexo.csv' INTO table defunciones.genero;

use defunciones;


-- a. ¿Cuál es la 34va causa de muerte en México? no olvides poner el nombre de la causa

-- Agrupando por código (aunque esto repite descripciones)
with t as(
SELECT regexp_replace(causa_def,'"', '') as cdef 
from defun
),

u as (
SELECT cdef, COUNT(cdef) as count 
FROM t
GROUP BY cdef
),

b as (
SELECT regexp_replace(descrip,'"', '') as descrip, count 
FROM u
JOIN
causas c
on u.cdef == c.cve
),

ranked as (
SELECT *, DENSE_RANK() OVER(ORDER BY count DESC) as rank 
FROM b
)

SELECT * 
FROM ranked WHERE rank ==34;




-- Agrupando por descripcion (aunque esto ignora códigos diferentes)
SELECT * FROM(
SELECT *, DENSE_RANK() OVER(ORDER BY cnt DESC) as rank FROM(
SELECT regexp_replace(descrip,'"', ''), COUNT(descrip) as cnt FROM(
SELECT regexp_replace(causa_def,'"', '') as causa_def
FROM defun) t
JOIN causas c
on t.causa_def == c.cve
GROUP BY descrip) a) b
WHERE rank == 34;


-- b. ¿Cuál es la 7ma causa de muerte en mujeres en Chihuahua? no olvides poner el nombre de la causa

SELECT * FROM(
SELECT *, DENSE_RANK() OVER(ORDER BY cnt DESC) as rank FROM(
SELECT descrip, COUNT(*) as cnt
FROM(
SELECT regexp_replace(c.descrip,'"', '') as descrip, g.descrip_gen as sexo, regexp_replace(e.nom_loc,'("\s*) |(\s*")', '') as entidad
FROM causas c, defun d, genero g, estados e
WHERE regexp_replace(d.causa_def,'"', '') = c.cve AND d.sexo = g.cve_gen AND regexp_replace(e.cve_ent,'("\s*) |(\s*")', '') = regexp_replace(d.ent_ocurr,'("\s*) |(\s*")', '') AND regexp_replace(e.cve_mun,'("\s*) |(\s*")', '') == 000) x
WHERE sexo == 'Mujeres' AND entidad == 'Chihuahua'
GROUP BY descrip) y) z
where rank == 7;


