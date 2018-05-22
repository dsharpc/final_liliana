#!/usr/bin/env python
import sys
import re

# Generamos diccionario 
vio_dic = {}
# Leemos la tabla 
vio = open('deviofam.csv','r')
# Llenamos el diccionario con los valores de la tabla
for line in vio:
    line = line.strip()
    splits = line.split(',')
    vio_dic[splits[0]] = splits[1]
vio.close()

for line in sys.stdin:
    line = line.strip()
    line = line.strip('"')
    linea = re.split(',',line)
    # Hacemos el join de la tabla
    if linea[43] in vio_dic:
        linea.append(vio_dic[linea[43]])
        print(linea[59] + "\t1")
