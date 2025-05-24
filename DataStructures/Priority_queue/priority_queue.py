from DataStructures.List import array_list as al
from DataStructures.Priority_queue import index_pq_entry as ie

def new_heap(cmp_function):

# Recibe una funcion tal que, si es de mayor prioridad, devuelve 1
# si es de igual prioridad, devuelve 0
# si es de menor prioridad, devuelve -1

    lista = al.new_list()
    al.add_last(lista, None)
    my_heap = {
        "elements": lista,
        "size": 0,
        "cmp_function": cmp_function
    }
    return my_heap

    
def priority(my_heap, parent, child):

# True si 'parent' tiene mayor o igual prioridad que 'child'

    cmp_function = my_heap["cmp_function"]
    result = cmp_function(parent, child)
    return result >= 0  # Mayor o igual prioridad que el hijo

def insert(my_heap, element): 

    al.add_last(my_heap["elements"], element)
    my_heap["size"] += 1
    swim(my_heap, my_heap["elements"]["size"]-1)

def swim(my_heap, pos):

    if pos <= 1:
        return my_heap
    else:
        parent_pos = pos // 2
        parent = al.get_element(my_heap["elements"], parent_pos)
        child = al.get_element(my_heap["elements"], pos)
        # Si el hijo tiene mayor prioridad que el padre, los intercambia
        if priority(my_heap, child, parent):
            al.exchange(my_heap["elements"], parent_pos, pos)
            return swim(my_heap, parent_pos)
        else:
            return my_heap

def size(my_heap):
    return my_heap["size"]

def is_empty(my_heap):
    if size(my_heap) == 0:
        return True
    else: 
        return False
    
def get_first_priority(my_heap):
    if is_empty(my_heap):
        return None
    else:
        elemento = al.get_element(my_heap["elements"], 1)
        return elemento

def remove (my_heap):
    if is_empty(my_heap):
        return None
    else:
        first = al.get_element(my_heap["elements"], 1)
        al.exchange(my_heap["elements"], 1, size(my_heap))
        al.remove_last(my_heap["elements"])
        my_heap["size"] -= 1
        sink(my_heap, 1)
        return first


def sink(my_heap, pos):
    size_heap = size(my_heap)
    left_child_pos = pos * 2
    right_child_pos = pos * 2 + 1

    # Si no hay hijos, termina
    if left_child_pos > size_heap:
        return my_heap

    father = al.get_element(my_heap["elements"], pos)
    left_child = al.get_element(my_heap["elements"], left_child_pos)

    # Si solo hay hijo izquierdo
    if right_child_pos > size_heap:
        if not priority(my_heap, father, left_child):
            al.exchange(my_heap["elements"], pos, left_child_pos)
            sink(my_heap, left_child_pos)
        return my_heap

    # Si hay ambos hijos
    right_child = al.get_element(my_heap["elements"], right_child_pos)
    # Elige el hijo con mayor prioridad para comparar con el padre
    if priority(my_heap, left_child, right_child):
        best_child_pos = left_child_pos
        best_child = left_child
    else:
        best_child_pos = right_child_pos
        best_child = right_child

    if not priority(my_heap, father, best_child):
        al.exchange(my_heap["elements"], pos, best_child_pos)
        sink(my_heap, best_child_pos)

    return my_heap