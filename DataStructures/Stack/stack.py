from DataStructures.List import single_linked_list as sl
from DataStructures.List import array_list as ar

def new_stack():
    return ar.new_list()

def push(my_stack, element):
    return ar.add_last(my_stack, element)

def pop(my_stack):
    return ar.remove_last(my_stack)

def is_empty(my_stack):
    return ar.is_empty(my_stack)

def top(my_stack):
    return ar.get_element(my_stack, ar.size(my_stack)-1)

def size(my_stack):
    return ar.size(my_stack)