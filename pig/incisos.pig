#### Carga de datos

defunc = LOAD '/code/defun_2016.csv' USING PigStorage(',') AS (
ent_regis:chararray,
mun_regis:chararray,
ent_resid:chararray,
mun_resid:chararray,
tloc_resid:chararray,
loc_resid:chararray,
ent_ocurr:chararray,
mun_ocurr:chararray,
tloc_ocurr:chararray,
loc_ocurr:chararray,
causa_def:chararray,
lista_mex:chararray,
sexo,edad:chararray,
dia_ocurr:chararray,
mes_ocurr:chararray,
anio_ocur:chararray,
dia_regis:chararray,
mes_regis:chararray,
anio_regis:chararray,
dia_nacim:chararray,
mes_nacim:chararray,
anio_nacim:chararray,
ocupacion:chararray,
escolarida:chararray,
edo_civil:chararray,
presunto:chararray,
ocurr_trab:chararray,
lugar_ocur:chararray,
necropsia:chararray,
asist_medi:chararray,
sitio_ocur:chararray,
cond_cert:chararray,
nacionalid:chararray,
derechohab:chararray,
embarazo:chararray,
rel_emba:chararray,
horas:chararray,
minutos:chararray,
capitulo:chararray,
grupo:chararray,
lista1:chararray,
gr_lismex:chararray,
vio_fami:chararray,
area_ur:chararray,
edad_agru:chararray,
complicaro:chararray,
dia_cert:chararray,
mes_cert:chararray,
anio_cert:chararray,
maternas:chararray,
lengua:chararray,
cond_act:chararray,
par_agre:chararray,
ent_ocules:chararray,
mun_ocules:chararray,
loc_ocules:chararray,
razon_m:chararray,
dis_re_oax:chararray);

edos = LOAD '/code/decateml.CSV' USING PigStorage(',') AS (
cve_ent:chararray,
cve_mun:chararray,
cve_loc:chararray,
nom_loc:chararray);

### Inciso A: ¿Cuál es el estado de México donde hay más muertes? campo ent_ocurr
### Indica el total de decesos en ese estado $\rightarrow$ No olivides poner la clave de la entidad y el nombre

preclean_edos = FOREACH edos GENERATE REPLACE(cve_ent,'"','') as (cve_ent:chararray),REPLACE(cve_mun,'"','')as (cve_mun:chararray), REPLACE(nom_loc,'"','') as (nom_loc:chararray);  

clean_edos = foreach preclean_edos generate TRIM(cve_ent) as (cve_ent:chararray),TRIM(cve_mun) as (cve_mun:chararray), TRIM(nom_loc) as (nom_loc:chararray);
solo_edos = filter clean_edos BY (cve_mun == '000');

preclean_defunc = foreach defunc generate REPLACE(ent_ocurr,'"','') as (ent_ocurr:chararray);
clean_defunc = foreach preclean_defunc generate TRIM(ent_ocurr) as (ent_ocurr:chararray);

joined_edos = JOIN clean_defunc BY ent_ocurr, solo_edos BY cve_ent;

grouped = GROUP joined_edos BY (cve_ent,nom_loc);

res = foreach grouped GENERATE group, COUNT(joined_edos) as ocurrencias;

res_sorted = ORDER res BY ocurrencias DESC;

res_final = LIMIT res_sorted 1;

flat = foreach res_final GENERATE FLATTEN($0), $1;

store flat into '/code/a/' using PigStorage(',');

### Inciso B: Indica por entidad donde residía ordenado descendentemente por número de decesos, campo ent_resid. No olvides poner la clave de cada entidad y el nombre

preclean_edos = FOREACH edos GENERATE REPLACE(cve_ent,'"','') as (cve_ent:chararray),REPLACE(cve_mun,'"','')as (cve_mun:chararray), REPLACE(nom_loc,'"','') as (nom_loc:chararray);  

clean_edos = foreach preclean_edos generate TRIM(cve_ent) as (cve_ent:chararray),TRIM(cve_mun) as (cve_mun:chararray), TRIM(nom_loc) as (nom_loc:chararray);
solo_edos = filter clean_edos BY (cve_mun == '000');

preclean_defunc = foreach defunc generate REPLACE(ent_resid,'"','') as (ent_resid:chararray);
clean_defunc = foreach preclean_defunc generate TRIM(ent_resid) as (ent_resid:chararray);

joined_edos = JOIN clean_defunc BY ent_resid, solo_edos BY cve_ent;

grouped = GROUP joined_edos BY (cve_ent,nom_loc);

res = foreach grouped GENERATE group, COUNT(joined_edos) as ocurrencias;

res_sorted = ORDER res BY ocurrencias DESC;

flat = foreach res_sorted GENERATE FLATTEN($0), $1;

store flat into '/code/b/' using PigStorage(',');

### Inciso C. Indica por entidad donde se registró el deceso ordenado descendentemente, campo ent_regis No olvides poner la clave de cada entidad y el nombre

preclean_edos = FOREACH edos GENERATE REPLACE(cve_ent,'"','') as (cve_ent:chararray),REPLACE(cve_mun,'"','')as (cve_mun:chararray), REPLACE(nom_loc,'"','') as (nom_loc:chararray);  

clean_edos = foreach preclean_edos generate TRIM(cve_ent) as (cve_ent:chararray),TRIM(cve_mun) as (cve_mun:chararray), TRIM(nom_loc) as (nom_loc:chararray);
solo_edos = filter clean_edos BY (cve_mun == '000');

preclean_defunc = foreach defunc generate REPLACE(ent_regis,'"','') as (ent_regis:chararray);
clean_defunc = foreach preclean_defunc generate TRIM(ent_regis) as (ent_regis:chararray);

joined_edos = JOIN clean_defunc BY ent_regis, solo_edos BY cve_ent;

grouped = GROUP joined_edos BY (cve_ent,nom_loc);

res = foreach grouped GENERATE group, COUNT(joined_edos) as ocurrencias;

res_sorted = ORDER res BY ocurrencias DESC;

flat = foreach res_sorted GENERATE FLATTEN($0), $1;

store flat into '/code/c/' using PigStorage(',');
