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
    mr.emit_intermediate(1, record)

def reducer(key, list_of_values):
    # key: person
    # list_of_values: list of friend pairs 
    total = 0
    person_friend = []
    friend_person = []
    match = []
    answer_key =[]
    #print list_of_values
    for v in list_of_values:
      #friend_person = [v[1].encode('ascii', 'ignore'), v[0].encode('ascii', 'ignore')]
      #print v  
      person_friend = [v[0].encode('ascii', 'ignore'), v[1].encode('ascii', 'ignore')]
      friend_person = [v[1].encode('ascii', 'ignore'), v[0].encode('ascii', 'ignore')]
      
      total = 0 
      for v in list_of_values:
        match = [v[0].encode('ascii', 'ignore'), v[1].encode('ascii', 'ignore')]
        #print match, friend_person
	if total != 1:
          if friend_person == match:
            total = 1
      	    
      if total == 0:
        answer_key = [person_friend[0], person_friend[1]]
	tuple(person_friend)
	tuple(friend_person)
        mr.emit(person_friend)
	mr.emit(friend_person)

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
