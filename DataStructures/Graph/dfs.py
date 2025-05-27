import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from DataStructures.Stack import stack as st
from DataStructures.Map import map_linear_probing as lp
from DataStructures.List import array_list as ar
from DataStructures.Graph import digraph as dg
from DataStructures.Graph import vertex as ve

def dfs(my_graph, source):
    visited_map = lp.new_map(dg.order(my_graph), 0.5)
    _dfs_visit(my_graph, source, visited_map, None)
    return visited_map

def _dfs_visit(my_graph, vertex, visited_map, parent):
    # Marca el nodo como visitado y guarda el padre
    lp.put(visited_map, vertex, {"marked": True, "edge_to": parent})
    adjacents_map = dg.adjacents(my_graph, vertex)
    adj_keys = lp.key_set(adjacents_map)
    for neighbor in adj_keys["elements"]:
        if lp.get(visited_map, neighbor) is None:
            _dfs_visit(my_graph, neighbor, visited_map, vertex)
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