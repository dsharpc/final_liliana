#!/usr/bin/env python
import sys
import re

# Generamos diccionario con los nombres de los meses
meses_dic = {}
# Leemos la tabla de los meses a memoria
meses = open('../datos/demeses.csv','r')
# Llenamos el diccionario con los valores de la tabla
for line in meses:
    line = line.strip()
    splits = line.split(',')
    meses_dic[splits[0]] = splits[1]
meses.close()

for line in sys.stdin:
    line = line.strip()
    line = line.strip('"')
    linea = re.split(',',line)
    # Hacemos el join de la tabla
    if linea[15] in meses_dic:
        linea.append(meses_dic[linea[15]])
        print(linea[59] + "\t1")

