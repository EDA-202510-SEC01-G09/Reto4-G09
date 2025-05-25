from DataStructures.Map import map_functions as mf
from DataStructures.Map import map_entry as me
from DataStructures.List import array_list as lt
import random as rd

def new_map(num_elements, load_factor, prime=109345121):
    
     capacity = int(mf.next_prime(num_elements/ load_factor))

     x = lt.new_list() 
     for i in range(capacity):
         dict = {"key": None, "value": None}
         lt.add_last(x, dict) 

     map = {"prime": prime, "capacity": capacity, 'scale': rd.randint(1,prime-1),
           'shift': rd.randint(0, prime-1),'table': x, "current_factor": 0, "limit_factor": load_factor,
             "size": 0}
    
     return map

def put(my_map, key, value):
    hash = mf.hash_value(my_map, key)
    slot = int(find_slot(my_map, key, hash)[1])

    entry = lt.get_element(my_map["table"], slot)
    if me.get_key(entry) is None or me.get_key(entry) == "__EMPTY__":
        # Only increment size if the key is new
        my_map["size"] += 1

    me.set_key(entry, key)
    me.set_value(entry, value)

    my_map["current_factor"] = size(my_map) / my_map["capacity"]

    if my_map["current_factor"] > my_map["limit_factor"]:
        return rehash(my_map)

    return my_map

def contains(my_map, key):
    hash_value = mf.hash_value(my_map, key)
    capacity = my_map["capacity"]
    for _ in range(capacity):
        entry = lt.get_element(my_map["table"], hash_value)
        entry_key = me.get_key(entry)
        if entry_key is None:
            return False
        if entry_key != "__EMPTY__" and entry_key == key:
            return True
        hash_value = (hash_value + 1) % capacity
    return False

def get(my_map, key):
    hash_value = mf.hash_value(my_map, key)
    capacity = my_map["capacity"]
    for _ in range(capacity):
        entry = lt.get_element(my_map["table"], hash_value)
        entry_key = me.get_key(entry)
        if entry_key is None:
            return None  # No está la clave
        if entry_key != "__EMPTY__" and entry_key == key:
            return me.get_value(entry)
        hash_value = (hash_value + 1) % capacity
    return None



def remove(my_map, key):
    hash_value = mf.hash_value(my_map, key)
    capacity = my_map["capacity"]
    for _ in range(capacity):
        entry = lt.get_element(my_map["table"], hash_value)
        entry_key = me.get_key(entry)
        if entry_key is None:
            return my_map  # No está la clave
        if entry_key != "__EMPTY__" and entry_key == key:
            me.set_key(entry, "__EMPTY__")
            me.set_value(entry, "__EMPTY__")
            my_map["size"] -= 1
            return my_map
        hash_value = (hash_value + 1) % capacity
    return my_map

def size(my_map):
    return my_map["size"]

def is_empty(my_map):
    return my_map["size"] == 0

def key_set (my_map):
     result = lt.new_list() 
     for i in my_map["table"]["elements"]:
        if me.get_key(i) is not None and me.get_key(i) != "__EMPTY__":
            lt.add_last(result, me.get_key(i))
     return result
    
def value_set(my_map):
     result = lt.new_list()
     for i in my_map["table"]["elements"]:
        if me.get_value(i) is not None and me.get_value(i) != "__EMPTY__":
            lt.add_last(result, me.get_value(i))
     
     return result

def find_slot(my_map, key, hash_value):
   
   first_avail = None
   found = False
   ocupied = False

   while not found:
      if is_available(my_map["table"], hash_value):
            if first_avail is None:
               first_avail = hash_value
            entry = lt.get_element(my_map["table"], hash_value) 
            if me.get_key(entry) is None:
               found = True

      elif default_compare(key, lt.get_element(my_map["table"], hash_value)) == 0: 
            first_avail = hash_value
            found = True 
            ocupied = True
      hash_value = (hash_value + 1) % my_map["capacity"]
   return ocupied, first_avail

def is_available(table, pos):

     entry = lt.get_element(table, pos)
     if me.get_key(entry) is None or me.get_key(entry) == "__EMPTY__":
          result = True
     else:
         result = False
     return result 

def rehash(my_map):
     my_map["capacity"] = mf.next_prime(my_map["capacity"] * 2)
     my_map["current_factor"] = 0
     my_map["size"] = 0
     old_table = my_map["table"]
     my_map["table"] = lt.new_list()
     for i in range(my_map["capacity"]):
         dict = {"key": None, "value": None}
         lt.add_last(my_map["table"], dict)
     for i in old_table["elements"]:
         if me.get_key(i) is not None and me.get_key(i) != "__EMPTY__":
             put(my_map, me.get_key(i), me.get_value(i))

     return my_map

def default_compare(key, entry):

     if key == me.get_key(entry):
          return 0
     elif key > me.get_key(entry):
          return 1
     return -1


