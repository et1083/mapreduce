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
    value = {}
    value[0] = record[0].encode('ascii', 'ignore')
    value[1]=record[1].encode('ascii', 'ignore')
    mr.emit_intermediate(key, value)

def reducer(key, list_of_values):
    # key: person
    # list_of_values: list of friend pairs 
    total = 0
    person_friend = []
    friend_person = []
    match = []
    stick = list_of_values[0]
    print list_of_values
    
    #print list_of_values
    #for v in list_of_values:
     # print key, v
   
    person = key   
	#mr.emit(friend_person)

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
