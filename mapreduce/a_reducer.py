#!/usr/bin/env python
#cat ../datos/defun_2016.csv | python3 a_mapper.py | sort | python3 a_reducer.py
import sys
previous = None
sum = 0

for line in sys.stdin:
  key, value = line.split('\t')
  if key != previous:
      if previous is not None:
        print(previous + '\t' + str(sum))
      previous = key
      sum = 0
  sum+=1
print(previous + '\t' + str(sum))
