import MapReduce
import sys

"""
Word Count Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: order id
    # value: document contents
    key = record[1]
    
    mr.emit_intermediate(key, record)

def reducer(key, list_of_values):
    # key: order id
    # value: everything else in record
    
    new_list = []
    final_list = []
    
    for v in list_of_values:
      if v[0].encode('ascii', 'ignore') == "order":
        new_list.extend(v) 
	
        for v in list_of_values: 
	  final_list[:] = [] 
          if v[0].encode('ascii', 'ignore') == "line_item":
            final_list.extend(new_list)  
	    final_list.extend(v)
            mr.emit(final_list)
	    

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
