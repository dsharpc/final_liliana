-- Iniciamos la base de datos
drop database if exists defunciones cascade;

create database if not exists defunciones location "s3://finalmge/hive/defuncionesdb/";
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
LOCATION 's3://finalmge/hive/defuncionesdb/defun';

LOAD DATA INPATH 's3://finalmge/datos/defun_2016.csv' INTO table defunciones.defun;

-- Creamos la tabla que el diccionario de causas
create external table if not exists defunciones.causas (cve string,
descrip string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION 's3://finalmge/hive/defuncionesdb/causas';

LOAD DATA INPATH 's3://finalmge/datos/decatcausa.csv' INTO table defunciones.causas;

-- Creamos la tabla que el diccionario de estados
create external table if not exists defunciones.estados (cve_ent string, 
cve_mun string,
cve_loc string,
nom_loc string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION 's3://finalmge/hive/defuncionesdb/estados';

LOAD DATA INPATH 's3://finalmge/datos/decateml.CSV' INTO table defunciones.estados;

-- Creamos la tabla que el diccionario de genero
create external table if not exists defunciones.genero (cve_gen string, 
descrip_gen string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION 's3://finalmge/hive/defuncionesdb/genero';

LOAD DATA INPATH 's3://finalmge/datos/desexo.csv' INTO table defunciones.genero;

use defunciones;

