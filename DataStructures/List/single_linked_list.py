def new_list():
    new_list = {"first": None,
                "last": None, 
                "size": 0,
                }
    return new_list

def get_element(my_list, pos):
    searchpos = 0
    node = my_list["first"]
    while searchpos < pos:
        node = node["next"]
        searchpos += 1
    return node["info"]

def is_present(my_list, element, cmp_function):
    is_in_array = False
    temp = my_list["first"]
    count = 0
    while not is_in_array and temp is not None:
        if cmp_function(element, temp["info"]) == 0:
            is_in_array = True
        else:
            temp = temp["next"]
            count += 1

    if not is_in_array:
        count = -1
    return count

def add_first(my_list, element):
    new_node = {"info": element,
                "next": None,
                }
    if my_list["size"] == 0:
        my_list["first"] = new_node
        my_list["last"] = new_node
    else:
        new_node["next"] = my_list["first"]
        my_list["first"] = new_node
    my_list["size"] += 1
    return my_list

def add_last(my_list, element):
    new_node = {"info": element,
                "next": None,
                }
    
    if my_list["size"] == 0:
        my_list["first"] = new_node
        my_list["last"] = new_node
    else:
        my_list["last"]["next"] = new_node
        my_list["last"] = new_node
    my_list["size"] += 1
    return my_list

def size(my_list):
    return my_list["size"]

def first_element(my_list):
    return my_list["first"]

def remove_first(my_list):
    if my_list["size"] == 0:
        raise Exception('IndexError: list index out of range')
    else:
        first = my_list["first"]
        now_first = first["next"]
        my_list["first"] = now_first
        my_list["size"] -= 1
        return first["info"]

def is_empty(my_list):
    if my_list["size"] == 0:
        return True
    else: 
        return False
    
def last_element(my_list):
    last_element = my_list["last"]
    return last_element["info"]

def change_info(my_list, pos, new_info):
    if pos < 0 or pos > my_list["size"] -1:
        raise Exception('IndexError: list index out of range')
    else: 
        count = 0
        actual = my_list["first"]
        while actual != None and count != pos:
            count += 1
            actual = actual["next"]
        actual["info"] = new_info
        return actual

def exchange (my_list, pos1, pos2):

    if pos1 < 0 or pos1 > my_list["size"] -1 or pos2 < 0 or pos2 > my_list["size"] -1 or my_list["size"] == 0:
        return None
    
    else:
        cont = 0
        act1 = my_list["first"]
        while cont < pos1:
            cont += 1
            act1 = act1["next"]

        cont2 = 0
        act2 = my_list["first"]
        while cont2 < pos2:
            cont2 += 1
            act2 = act2["next"]

        info1 = act1["info"]
        act1["info"] = act2["info"]
        act2["info"] = info1
        return my_list

def delete_element(my_list, pos):
    if pos < 0 or pos > my_list["size"] -1:
        raise None
    else:
        count = 0
        actual = my_list["first"]
        while actual != None and count != pos:
            count += 1
            actual = actual["next"]
        if actual == my_list["first"]:
            my_list["first"] = actual["next"]
        else:
            prev = my_list["first"]
            while prev["next"] != actual:
                prev = prev["next"]
            prev["next"] = actual["next"]
        my_list["size"] -= 1
        return my_list
    
def remove_last(my_list):
    if my_list["size"] == 0:
        return None
    else:
        
        if my_list["size"] == 1:
            last = my_list["last"]
            my_list["first"] = None
            my_list["last"] = None
            my_list["size"] -= 1
            return last["info"]
            
        else:
            actual = my_list["first"]
            last = my_list["last"]
            while actual["next"]["next"] is not None:
                actual = actual["next"]
            actual["next"] = None
            my_list["last"] = actual
            my_list["size"] -= 1
def sub_list(my_list, start, num_elements):

    if start < 0 or start >= my_list["size"]:
        raise IndexError("Start index out of range")

    if num_elements <= 0:
        return new_list() 

    lista = new_list()

    current = my_list["first"]
    index = 0

    while current is not None and index < start:
        current = current["next"]
        index += 1

    count = 0
    while current is not None and count < num_elements:
        new_node = {"info": current["info"], "next": None}  # Create a new independent node

        if lista["first"] is None:
            lista["first"] = new_node
            lista["last"] = new_node
        else:
            lista["last"]["next"] = new_node
            lista["last"] = new_node

        lista["size"] += 1
        current = current["next"]
        count += 1

    return lista



def insert_element(my_list, element, pos):
    if pos < 0 or pos > size(my_list):
        return None

    else:
        if my_list["size"] == 0:    
            new_node = {"info": element,
                    "next": None,
                    }
            my_list["first"] = new_node
            my_list["last"] = new_node
            my_list["size"] += 1
            return my_list
        else:
            if pos == 0:
                new_node = {"info": element,
                        "next": None,
                        }
                if my_list["size"] == 0:
                    my_list["first"] = new_node
                    my_list["last"] = new_node
                else:
                    new_node["next"] = my_list["first"]
                    my_list["first"] = new_node
                    my_list["size"] += 1
                return my_list
            
            else:
                count = 0
                actual = my_list["first"]
                next = actual["next"]
                while count != pos and actual != None:
                    previo = actual
                    actual = next
                    next = actual["next"]
                    count += 1
                sig = actual
                actual = element
                previo["next"] = actual
                actual["next"] = sig
                my_list["size"] += 1
                return my_list
            

#Laboraorio 5: Funciones de ordenamiento con Single linked list

def default_sort_criteria(element_1, element_2):

   is_sorted = False
   if element_1 < element_2:
      is_sorted = True
   return is_sorted

def selection_sort(my_list, sort_criteria):

    tamano = size(my_list)
    if tamano <= 1:
        result = my_list

    else:
        for i in range(tamano):
            menor = get_element(my_list, i)
            f = 1 + i
            pos_menor = i
            while f < tamano:
                sort = sort_criteria(get_element(my_list, f),menor)
                if sort == True:
                    menor = get_element(my_list, f)
                    pos_menor = f
                f += 1
            exchange(my_list, i, pos_menor)

        result = my_list

    return result

def insertion_sort(my_list, sort_crit):
    tamano = size(my_list)

    if tamano <= 1:
        result = my_list
    
    else:
        for i in range(tamano):
            f = i - 1
            x = i
            while f > -1:
                sort = sort_crit(get_element(my_list, x), get_element(my_list,f))
                if sort == True:
                    exchange(my_list, x, f)
                    x -= 1
                else:
                    f = -1
                f -= 1
        result = my_list

    return result

def shell_sort(my_list, sort_crit):
    n = size(my_list)

    if n <= 1:
        result = my_list

    else:

        gap = n // 2 #????

        while gap > 0:

            for i in range(gap, n):
                elem = get_element(my_list, i)
                j = i

                while j >= gap and sort_crit(elem, get_element(my_list, j - gap)):

                    exchange(my_list, j, j-gap)
                    j -= gap

            gap = gap // 2

        result = my_list

    return result


def merge_sort(my_list, sort_crit):
    n = size(my_list)

    if n <= 1:
        return my_list  

    mid = n // 2

    if n % 2 == 0:
        l = merge_sort(sub_list(my_list, 0, mid), sort_crit)
        r = merge_sort(sub_list(my_list, mid, mid ), sort_crit)
    else:
        l = merge_sort(sub_list(my_list, 0, mid), sort_crit)
        r = merge_sort(sub_list(my_list, mid, mid + 1), sort_crit)

    return merge(l, r, sort_crit)

def merge(lista1, lista2, sort_crit):
    lista = new_list()  
    i = 0
    j = 0

    while i < size(lista1) and j < size(lista2):

        if sort_crit(get_element(lista1, i), get_element(lista2, j)):
            add_last(lista, get_element(lista1, i))
            i += 1
        else:
            add_last(lista, get_element(lista2, j))
            j += 1

    while i < size(lista1):
        add_last(lista, get_element(lista1, i))
        i += 1

    while j < size(lista2):
        add_last(lista, get_element(lista2, j))
        j += 1

    return lista 

def quick_sort(arr, sort_crit, low=0, high=None):
    if high is None:
        high = arr["size"] - 1

    if low < high:  
        pi = partition(arr, low, high)
        
        quick_sort(arr, sort_crit, low, pi - 1)
        quick_sort(arr, sort_crit, pi + 1, high)

def partition(arr, low, high):
    pivot = get_element(arr, high)
    i = low - 1

    for j in range(low, high):
        if get_element(arr, j) < pivot:
            i += 1
            exchange(arr, i, j)
    
    exchange(arr, i + 1, high)
    return i + 1



 