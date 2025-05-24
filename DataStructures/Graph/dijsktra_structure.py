from DataStructures.Map import map_linear_probing as mp
from DataStructures.Priority_queue import priority_queue as pq
from DataStructures.Graph import digraph as gr


def new_dijsktra_structure(source, g_order):
    """

    Crea una estructura de busqueda usada en el algoritmo **dijsktra**.

    Se crea una estructura de busqueda con los siguientes atributos:

    - **source**: Vertice de origen. Se inicializa en ``source``
    - **visited**: Mapa con los vertices visitados. Se inicializa en ``None``
    - **pq**: Cola indexada con los vertices visitados. Se inicializa en ``None``

    :returns: Estructura de busqueda
    :rtype: dijsktra_search
    """
    structure = {
        "source": source,
        "visited": mp.new_map(
            g_order, 0.5),
        "pq": pq.new_heap(comparison),
        "predecessors": {}
        }
    return structure

def comparison(a, b):
    # a and b are tuples: (distance, vertex)
    if a[0] == b[0]:
        return 0
    elif a[0] < b[0]:
        return 1  # menor distancia = mayor prioridad
    else:
        return -1

def dijkstra(my_graph, source):
    
    if gr.order(my_graph) == 0:
        raise Exception("El grafo está vacío, no se puede ejecutar Dijkstra.")
    else:
        aux_structure = new_dijsktra_structure(source, gr.order(my_graph))
        pq.insert(aux_structure["pq"], (0, source, None))  # None porque el source no tiene predecesor

        while not pq.is_empty(aux_structure["pq"]):

            current_distance, vertex, predecessor = pq.remove(aux_structure["pq"])

            if mp.get(aux_structure["visited"], vertex) is not None:
                continue
            mp.put(aux_structure["visited"], vertex, current_distance)
            if predecessor is not None:
                aux_structure["predecessors"][vertex] = predecessor
            adjacents = mp.value_set(gr.adjacents(my_graph, vertex))
            for adj in adjacents["elements"]:
                neighbor = adj["to"]
                weight = adj["weight"]
                if mp.get(aux_structure["visited"], neighbor) is None:
                    new_distance = current_distance + weight
                    pq.insert(aux_structure["pq"], (new_distance, neighbor, vertex))
        return aux_structure
    
def dist_to(key_v, aux_structure):
    # Use the map's get function to retrieve the distance for key_v
    distance = mp.get(aux_structure["visited"], key_v)
    if distance is not None:
        return distance
    else:
        raise Exception("No existe una distancia registrada para el vértice dado.")
    
def has_path_to(key_v, aux_structure):
    """
    Retorna True si existe un camino desde el vértice fuente hasta key_v,
    es decir, si key_v fue visitado por Dijkstra.
    """
    if mp.get(aux_structure["visited"], key_v) is not None:
        return True
    else:
        return False
    
def path_to(key_v, aux_structure):
    """
    Retorna la lista de vértices que conforman el camino más corto desde el vértice fuente
    hasta key_v, en orden desde el origen hasta el destino.
    """
    if not has_path_to(key_v, aux_structure):
        raise Exception("No existe un camino desde el vértice fuente hasta el vértice dado.")
    
    path = []
    current = key_v
    while current != aux_structure["source"]:
        path.append(current)
        current = aux_structure["predecessors"][current]  # Usa el nombre correcto del diccionario
    path.append(aux_structure["source"])
    path.reverse()
    return path