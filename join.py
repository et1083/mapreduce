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
    
    order_list = []
    item_list = []
    final_list = []
    
    for v in list_of_values:
      if v[0].encode('ascii', 'ignore') == "order":
        order_list.extend(v) 
    i=0	
    for y in list_of_values: 
      item_list[:] = [] 
      
      if y[0].encode('ascii', 'ignore') == "line_item":
           
        item_list.extend(order_list)  
        #print final_list
        item_list.extend(y)
	
      #final_list.insert(i,item_list) 
      #i +=1
      print item_list
	
      #mr.emit(new_list)
	    

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
