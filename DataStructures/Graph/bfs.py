from DataStructures.Queue import queue as q
from DataStructures.Stack import stack as st
from DataStructures.List import array_list as ar
from DataStructures.Map import map_linear_probing as lp
from DataStructures.Graph import digraph as dg
from DataStructures.Graph import vertex as ve

def bfs(my_graph, source):
    order = dg.order(my_graph)
    graph_search = lp.new_map(order, 0.5)
    value = {"marked": True, "edge_to": None, "dist_to": 0}
    lp.put(graph_search, source, value)
    visited_map = bfs_vertex(my_graph, source, graph_search)
    return visited_map

def bfs_vertex(my_graph, source, visited_map):
    cola_de_recorrido = q.new_queue()
    q.enqueue(cola_de_recorrido, source)
    while not q.is_empty(cola_de_recorrido):
        llave_actual = q.dequeue(cola_de_recorrido)
        adjacentes = lp.value_set(dg.adjacents(my_graph, llave_actual))

        for adj in adjacentes["elements"]:
            llave_adjacente_actual = adj["to"]  # Solo el destino
            revision = lp.get(visited_map, llave_adjacente_actual)
            if not revision:
                q.enqueue(cola_de_recorrido, llave_adjacente_actual)
                llave_Actual_valor = lp.get(visited_map, llave_actual)
                lp.put(
                    visited_map,
                    llave_adjacente_actual,
                    {
                        "marked": True,
                        "edge_to": llave_actual,
                        "dist_to": llave_Actual_valor["dist_to"] + 1
                    }
                )
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
    vertex_inicial = lp.get(visited_map, key_v)
    road_map = st.new_stack()
    st.push(road_map, key_v)
    while vertex_inicial and vertex_inicial["edge_to"] is not None:
        st.push(road_map, vertex_inicial["edge_to"])
        vertex_inicial = lp.get(visited_map, vertex_inicial["edge_to"])
    return road_map