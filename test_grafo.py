from DataStructures.Graph import digraph as gr
from DataStructures.Graph import dijsktra_structure as ds
from DataStructures.Map import map_linear_probing as lp
from DataStructures.Priority_queue import priority_queue as pq
from DataStructures.Graph import bfs
from DataStructures.Graph import dfs
def test():

    grafo = gr.new_graph(5)
    gr.insert_vertex(grafo, 0, "A")
    gr.insert_vertex(grafo, 1, "B")
    gr.insert_vertex(grafo, 2, "C")
    gr.insert_vertex(grafo, 3, "D")
    gr.insert_vertex(grafo, 4, "E")
    gr.insert_vertex(grafo, 5, "F")
    gr.insert_vertex(grafo, 6, "G")
    gr.insert_vertex(grafo, 7, "H")

    gr.add_edge(grafo, 0, 1, 5)
    gr.add_edge(grafo, 0, 7, 8)
    gr.add_edge(grafo, 0, 4, 9)
    gr.add_edge(grafo, 1, 3, 15)
    gr.add_edge(grafo, 1, 2, 12)
    gr.add_edge(grafo, 1, 7, 4)
    gr.add_edge(grafo, 3, 6, 9)
    gr.add_edge(grafo, 2, 6, 11)
    gr.add_edge(grafo, 2, 3, 3)
    gr.add_edge(grafo, 7, 2, 7)
    gr.add_edge(grafo, 7, 5, 6)
    gr.add_edge(grafo, 4, 7, 5)
    gr.add_edge(grafo, 4, 5, 4)
    gr.add_edge(grafo, 4, 6, 20)
    gr.add_edge(grafo, 5, 2, 1)
    gr.add_edge(grafo, 5, 6, 13)

    visited_dfs = dfs.dfs(grafo, 0)
    print(dfs.path_to(7, visited_dfs))

    pass

def compare(a, b):
    if a == b:
        return 0
    elif a < b:
        return -1
    else:
        return 1