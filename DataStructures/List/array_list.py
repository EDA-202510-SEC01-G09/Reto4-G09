def new_list():
    new_list = {"elements": [],
                "size": 0,
                }
    return new_list

def get_element(my_list, index):
    return my_list["elements"][index]

def is_present(my_list, element, cmp_function):
    size = my_list["size"]
    if size > 0:
        keyexist = False
        for keypos in range(0, size):
            info = my_list["elements"][keypos]
            if cmp_function(element, info) == 0:
                keyexist = True
                break
        if keyexist:
            return keypos
    return -1

def add_first(my_list, element):
    my_list["elements"].insert(0, element)
    my_list["size"] += 1
    return my_list

def add_last(my_list, element):
    my_list["elements"].append(element)
    my_list["size"] += 1
    return my_list

def size(my_list):
    return my_list["size"]

def first_element(my_list):
    return my_list["elements"][0]

def is_empty(my_list):
    return my_list["size"] == 0

def get_fist_element(my_list):
    return my_list["elements"][0]

def insert_element(my_list, element, index):
    my_list["elements"].insert(index, element)
    my_list["size"] += 1
    return my_list

def delete_element(my_list, index):
    if my_list["size"] > 0:
        my_list["elements"].pop(index)
        my_list["size"] -= 1
    return my_list

def change_info(my_list, index, element):
    my_list["elements"][index] = element
    return my_list

def exchange(my_list, pos1, pos2):
    temp = my_list["elements"][pos1]
    my_list["elements"][pos1] = my_list["elements"][pos2]
    my_list["elements"][pos2] = temp
    return my_list

def sub_list(my_list, start, num_elements):

    if start < 0 or start >= my_list["size"]:

        raise IndexError("Start index is out of range")

    if num_elements < 0:
        raise ValueError("Number of elements cannot be negative")

    end = min(start + num_elements, my_list["size"])

    sublist = {"elements": my_list["elements"][start:end], "size": end - start}

    return sublist
    


def remove_last(my_list):
    if my_list["size"] == 0:
        raise Exception('IndexError: list index out of range')
    else: 
        elementos = my_list["elements"]
        size = my_list["size"]
        elemento = elementos[size-1]
        del elementos[size-1]
        my_list["size"] -= 1
        return elemento
    
def remove_first(my_list):
    if my_list["size"] == 0:
        raise Exception('IndexError: list index out of range')
    else: 
        elementos = my_list["elements"]
        elemento = elementos[0]
        my_list["size"] -= 1
        del elementos[0]
        return elemento
    

#Laboraorio 5: Funciones de ordenamiento con array list

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
                sort = sort_criteria(get_element(my_list, f),menor)# pedro edita esto con base al nuevo sort criteria
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
                sort = sort_crit(get_element(my_list, x), get_element(my_list,f))# lo mismo pq la f me confunde xddd
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

        gap = n // 2

        while gap > 0:

            for i in range(gap, n):
                elem = get_element(my_list, i)
                j = i

                while j >= gap and sort_crit(elem, get_element(my_list, j - gap)) == 2:

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

lista = {"elements": [1, 23, 5, 32, 4, 35, 45, 43], "size": 8}

def quick_sort(arr, sort_crit, low=0, high=None):
    if high is None:  # First call, set high to the last index
        high = arr["size"] - 1

    if low < high:  # Ensure valid indices
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


