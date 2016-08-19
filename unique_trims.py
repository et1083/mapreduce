import MapReduce
import sys

"""
Word Count Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: document identifier
    # value: document contents
    key = record[0]
    value = record[1]
    value= value[:-10]
    mr.emit_intermediate(1, value)

def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    total = 0
    duplicate = []
    holder = []
    for v in list_of_values:
      
      if v !=duplicate:
        mr.emit(v)
      total = 0
      holder = v
      for v in list_of_values:
        if holder == v:
	  total +=1
        if total == 2:
	  duplicate = holder 

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
