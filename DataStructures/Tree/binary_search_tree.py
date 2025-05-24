from DataStructures.Tree import bst_node as bst
from DataStructures.List import single_linked_list as sl
from DataStructures.List import array_list as ar


def new_map():
    return {"root": None,
            "size": 0}

def put(my_bst, key, value):
    if my_bst["root"] is None:
        my_bst["root"] = bst.new_node(key, value)
    else:
        insert_node(my_bst["root"], key, value)
        my_bst["root"]["size"] += 1
    return my_bst

def insert_node(root, key, value):
    if key < root["key"]:
        if root["left"] is None:
            root["left"] = bst.new_node(key, value)
        else:
            insert_node(root["left"], key, value)

    elif key > root["key"]:
        if root["right"] is None:
            root["right"] = bst.new_node(key, value)

        else:
            insert_node(root["right"], key, value)
    else:
        root["value"] = value

def get(my_bst, key):

    if my_bst["root"] is None:
        return None
    else:
        return get_node(my_bst["root"], key)

def get_node(root, key):

    if root is None:
        return None
    if key < bst.get_key(root):
        return get_node(root["left"], key)
    elif key > bst.get_key(root):
        return get_node(root["right"], key)
    else:
        return bst.get_value(root)
    
def remove(my_bst, key):
    if my_bst["root"] is None:
        return my_bst
    else:
        my_bst["root"] = remove_node(my_bst["root"], key)
        if my_bst["root"] is None:
            my_bst["root"] = None
    return my_bst

def remove_node(root, key):
    if root is None:
        return None
    if key < bst.get_key(root):
        root["left"] = remove_node(root["left"], key)
    elif key > bst.get_key(root):
        root["right"] = remove_node(root["right"], key)
    else:
        if root["left"] is None:
            return root["right"]
        elif root["right"] is None:
            return root["left"]
        else:
            min_node = get_min_node(root["right"])
            root["key"] = min_node["key"]
            root["value"] = min_node["value"]
            root["right"] = remove_node(root["right"], min_node["key"])
    return root

def contains(my_bst, key):

    search = get(my_bst, key)
    if search is None:
        return False
    else:
        return True
    
def size(my_bst):
    return size_tree(my_bst["root"])

def size_tree(root):
    if root is None:
        return 0
    return root["size"] 

def is_empty(my_bst):
    if my_bst["root"] is None:
        return True
    else:
        return False
    
def key_set(my_bst):
    if my_bst["root"] is None:
        return sl.new_list()
    else:
        result = sl.new_list()
        result = key_set_tree(my_bst["root"], result)
        return result
    
def key_set_tree(root, list):
    if root is None:
        return sl.new_list()
    key_set_tree(root["left"], list)
    sl.add_last(list, bst.get_key(root))
    key_set_tree(root["right"], list)
    return list

def value_set(my_bst):
    if my_bst["root"] is None:
        return sl.new_list()
    else:
        result = sl.new_list()
        return value_set_tree(my_bst["root"], result)
    
def value_set_tree(root, list):
    if root is None:
        return sl.new_list()
    value_set_tree(root["left"], list)
    sl.add_last(list, bst.get_value(root))
    value_set_tree(root["right"], list)
    return list

def get_min(my_bst):
    if my_bst["root"] is None:
        return None
    else:
        return bst.get_key(get_min_node(my_bst["root"]))

def get_min_node(root):
    if root is None:
        return None
    while root["left"] is not None:
        root = root["left"]
    return root

def get_max(my_bst):
    if my_bst["root"] is None:
        return None
    else:
        return bst.get_key(get_max_node(my_bst["root"]))

def get_max_node(root):
    if root is None:
        return None
    while root["right"] is not None:
        root = root["right"]
    return root

def delete_min(my_bst):
    if my_bst["root"] == None:
        return None
    else:
        return delete_min_tree(my_bst["root"])

def delete_min_tree(root):
    if root["left"] != None:
        root = root["left"]
    elif root["left"] == None:
        root["left"] = root["left"]["right"]
    return root

def delete_max(my_bst):
    if my_bst["root"] == None:
        return None
    else:
        return delete_max_tree(my_bst["root"])

def delete_max_tree(root):
    if root["right"] != None:
        root = root["right"]
    elif root["right"] == None:
        root["right"] = root["right"]["left"]
    return root

def floor(my_bst, key):
    if my_bst["root"] is None:
        return None
    return floor_key(my_bst["root"], key)

def floor_key(root, key):
    if root is None:
        return None
    if key == bst.get_key(root):
        return  bst.get_key(root)
    elif key < bst.get_key(root):
        return floor_key(root["left"], key)
    else:
        temp = floor_key(root["right"], key)
        if temp is not None:
            return temp
        else:
            return bst.get_key(root)

def ceiling(my_bst, key):
    if my_bst["root"] is None:
        return None
    return ceiling_key(my_bst["root"], key)

def ceiling_key(root, key):
    if root is None:
        return None
    if key == bst.get_key(root):
        return bst.get_key(root)
    elif key < bst.get_key(root):
        temp = ceiling_key(root["left"], key)
        if temp is not None:
            return temp
        else:
            return bst.get_key(root)
    else:
        return ceiling_key(root["right"], key)

def select(my_bst,pos):
    if my_bst["root"] is None:
        return None
    return select_key(my_bst["root"], pos)

def select_key(root, key):
    if root is None:
        return None
    left_size = size_tree(root["left"])
    if key < left_size:
        return select_key(root["left"], key)
    elif key > left_size:
        return select_key(root["right"], key - left_size - 1)
    else:
        return bst.get_key(root)
    
def rank(my_bst, key):
    if my_bst["root"] is None:
        return 0  
    return rank_key(my_bst["root"], key)

def rank_key(root, key):
    if root is None:
        return 0  
    if key < bst.get_key(root):
        return rank_key(root["left"], key)
    elif key > bst.get_key(root):
        return 1 + size_tree(root["left"]) + rank_key(root["right"], key)
    else:
        return size_tree(root["left"])  
    
def height(my_bst):
    if my_bst["root"] is None:
        return 0
    return height_tree(my_bst["root"])

def height_tree(root):
    if root is None:
        return -1
    left_height = height_tree(root["left"])
    right_height = height_tree(root["right"])
    return max(left_height, right_height) + 1

def keys(my_bst, key_initial , key_final):
    if my_bst["root"] is None:
        return sl.new_list()
    else:
        list_key = sl.new_list()
        return keys_range(my_bst["root"], key_initial, key_final, list_key)

def keys_range(root, key_initial, key_final, list_key):
    if root is None:
        return list_key  
    if key_initial < bst.get_key(root):
        keys_range(root["left"], key_initial, key_final, list_key)
    if key_initial <= bst.get_key(root) <= key_final:
        sl.add_last(list_key, bst.get_key(root))
    if key_final > bst.get_key(root):
        keys_range(root["right"], key_initial, key_final, list_key)
    return list_key

def values(my_bst, key_initial , key_final):
    if my_bst["root"] is None:
        return sl.new_list()
    else:
        list_value = sl.new_list()
        return values_range(my_bst["root"], key_initial, key_final, list_value)
    
def values_range(root, key_initial, key_final, list_value):
    if root is None:
        return list_value  
    if key_initial < bst.get_key(root):
        values_range(root["left"], key_initial, key_final, list_value)
    if key_initial <= bst.get_key(root) <= key_final:
        sl.add_last(list_value, bst.get_value(root))
    if key_final > bst.get_key(root):
        values_range(root["right"], key_initial, key_final, list_value)
    return list_value
    
def default_compare(key,element):
    element_key = bst.get_key(element)  
    if key == element_key:
        return 0
    elif key > element_key:
        return 1
    else:
        return -1

