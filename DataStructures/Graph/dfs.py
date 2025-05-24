import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from DataStructures.Stack import stack as st
from DataStructures.Map import map_linear_probing as lp
from DataStructures.List import array_list as ar
from DataStructures.Graph import digraph as dg
from DataStructures.Graph import vertex as ve

def dfs(my_graph, source):
    order = dg.order(my_graph)
    visited_map = lp.new_map(order, 0.5)
    value = {"marked": True, "edge_to": None}  # <-- Aquí el cambio
    lp.put(visited_map, source, value)
    visited_map = dfs_vertex(my_graph, source, visited_map)
    return visited_map

def dfs_vertex(my_graph, vertex, visited_map):
    adjacents = lp.value_set(dg.adjacents(my_graph, vertex))

    for adj in adjacents["elements"]:
        neighbor = adj["to"]
        visited = lp.get(visited_map, neighbor)
        if visited is None:
            lp.put(visited_map, neighbor, {"marked": True, "edge_to": vertex})
            dfs_vertex(my_graph, neighbor, visited_map)
    return visited_map
    
def has_path_to(key_v, visited_map):
    """
    Retorna True si existe un camino desde el vértice fuente hasta key_v,
    es decir, si key_v fue visitado por BFS.
    """
    return lp.get(visited_map, key_v) is not None
    
def path_to(key_v, visited_map):
    if not has_path_to(key_v, visited_map):
        return None
    stack_retorno = st.new_stack()
    current = key_v
    while True:
        st.push(stack_retorno, current)
        element = lp.get(visited_map, current)
        if element["edge_to"] is None:
            break
        current = element["edge_to"]
    return stack_retorno