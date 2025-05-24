from DataStructures.Map import map_functions as mf
from DataStructures.Map import map_entry as me
from DataStructures.List import array_list as ar
from DataStructures.List import single_linked_list as sl
import random as rd

def new_map(num_elements, load_factor, prime=109345121):
    capacity_prime = mf.next_prime(num_elements/load_factor)
    table = ar.new_list()
    for i in range(capacity_prime):
        x = sl.new_list()
        ar.add_last(table, x)
    my_table = {"prime": 109345121,
                "capacity": capacity_prime,
                "scale": rd.randint(1, prime-1),
                "shift": rd.randint(0, prime-1),
                "table": table,
                "current_factor": 0,
                "limit_factor": load_factor,
                "size": 0}
    return my_table

def put(my_table, key, value):
    hash = mf.hash_value(my_table,key)

    if sl.is_present(my_table["table"]["elements"][hash], key, default_compare) != -1:
        pos = sl.is_present(my_table["table"]["elements"][hash], key, default_compare)
        sl.change_info(my_table["table"]["elements"][hash], pos, {"key": key, "value": value})
        
    elif sl.is_present(my_table["table"]["elements"][hash], key, default_compare) == -1:
        sl.add_last(my_table["table"]["elements"][hash], me.new_map_entry(key, value))
        my_table["size"] += 1
        my_table["current_factor"] = my_table["size"]/my_table["capacity"]
    
    if my_table["current_factor"] > my_table["limit_factor"]:
        rehash(my_table)
        
    return my_table

def default_compare(key, entry):
    if key == me.get_key(entry):
        return 0
    elif key > me.get_key(entry):
        return 1
    elif key < me.get_key(entry):
        return -1

def contains(my_table, key):
    in_table = False
    hash = mf.hash_value(my_table, key)
    if sl.is_present(my_table["table"]["elements"][hash], key, default_compare) != -1:
        in_table = True
    return in_table

def remove(my_table, key):
    if contains(my_table, key) == False:
        return my_table
    else: 
        hash = mf.hash_value(my_table, key)
        pos = sl.is_present(my_table["table"]["elements"][hash],key, default_compare)
        sl.delete_element(my_table["table"]["elements"][hash], pos)
        my_table["size"] -= 1
        my_table["current_factor"] = my_table["size"]/my_table["capacity"]
        return my_table

def get(my_table, key):
    hash = mf.hash_value(my_table, key)
    pos = sl.is_present(my_table["table"]["elements"][hash],key, default_compare)
    if pos == -1:
        return None
    else:   
        info = sl.get_element(my_table["table"]["elements"][hash], pos)
        value = me.get_value(info)
        return value

def size(my_table):
    size = my_table["size"]
    return size

def is_empty(my_table):
    empty = False
    if size(my_table) == 0:
        empty = True
    return empty

def key_set(my_table):
    pos = 0
    lista_llaves = ar.new_list()
    while pos < my_table["capacity"]:
        slot_size = sl.size(my_table["table"]["elements"][pos])
        for i in range(slot_size):
            llave = me.get_key(sl.get_element(my_table["table"]["elements"][pos],i))
            if llave != "__EMPTY__" and llave != None:
                ar.add_last(lista_llaves, llave)
        pos += 1
    return lista_llaves

def value_set(my_table):
    pos = 0
    lista_valores = ar.new_list()
    while pos < my_table["capacity"]:
        slot_size = sl.size(my_table["table"]["elements"][pos])
        for i in range(slot_size):
            valor = me.get_value(sl.get_element(my_table["table"]["elements"][pos],i))
            if valor != "__EMPTY__" and valor != None:
                ar.add_last(lista_valores, valor)
        pos += 1
    return lista_valores

def rehash(my_table):
    cap_vieja = my_table["capacity"]
    cap_nueva = mf.next_prime(2*cap_vieja)
    tabla_orig = my_table["table"]

    new_table = ar.new_list()
    for i in range(cap_nueva):
        ar.add_last(new_table, sl.new_list())

    my_table["size"] = 0
    my_table["capacity"] = cap_nueva
    my_table["current_factor"] = 0
    my_table["table"] = new_table

    pos = 0
    while pos < cap_vieja:
        for i in range(tabla_orig["elements"][pos]["size"]):
            llave = me.get_key(sl.get_element(tabla_orig["elements"][pos], i))
            valor = me.get_value(sl.get_element(tabla_orig["elements"][pos], i))
            if llave != "__EMPTY__" and llave != None and valor != "__EMPTY__" and valor != None:
                put(my_table, llave, valor)
        pos += 1

    return my_table

