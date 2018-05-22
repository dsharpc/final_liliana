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

