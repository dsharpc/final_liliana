-- b. ¿Cuál es la 7ma causa de muerte en mujeres en Chihuahua? no olvides poner el nombre de la causa
																																																																																																																																	

with t as(
SELECT regexp_replace(causa_def,'"', '') as cdef, ent_ocurr, sexo
from defun
),

u as (
SELECT cdef, regexp_replace(ent_ocurr,'("\s*) |(\s*")', '') as ent_ocurr, sexo, COUNT(cdef) as count 
FROM t
GROUP BY cdef, ent_ocurr, sexo
),

e as (
SELECT regexp_replace(cve_ent,'("\s*) |(\s*")', '') as cve_ent, regexp_replace(nom_loc,'("\s*) |(\s*")', '') as entidad
FROM estados																			
WHERE regexp_replace(cve_mun,'("\s*) |(\s*")', '') == 000
),

b as (
SELECT regexp_replace(descrip,'"', '') as descrip, descrip_gen, entidad, count 
FROM u
JOIN
causas c
on (u.cdef == c.cve)
JOIN
e
on (u.ent_ocurr == e.cve_ent)
JOIN
genero g
on (u.sexo == g.cve_gen)
),

ranked as (
SELECT *, DENSE_RANK() OVER(ORDER BY count DESC) as rank 
FROM b
WHERE descrip_gen == 'Mujeres' AND entidad == 'Chihuahua'
)

SELECT * 
FROM ranked WHERE rank ==7;


