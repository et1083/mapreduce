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
    #value= value[:-10]
      
    mr.emit_intermediate(1, value)

def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    total = 0
    duplicates = 0
    holder = []
    for v in list_of_values:
      v=v[:-10]
      total = 0
      
      holder = v
      for v in list_of_values:
        v=v[:-10]
	
        if holder == v:
	  total +=1
        if total ==2:
	  duplicate = v
	    mr.emit(holder)
	    duplicates +=1 
	    
      #print total
      if total < 2:
        mr.emit(holder)

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
