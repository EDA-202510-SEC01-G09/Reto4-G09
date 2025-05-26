import time
import csv
from DataStructures.Graph import digraph as gr
from DataStructures.Map import map_linear_probing as mp
from DataStructures.List import array_list as lt
from DataStructures.Stack import stack as st
from DataStructures.Graph import edge as edg
from DataStructures.Graph import dijsktra_structure as djs
from DataStructures.Priority_queue import priority_queue as pq
from DataStructures.Graph import bfs
from DataStructures.Graph import dfs

def new_logic():
    """
    Crea el catálogo para almacenar las estructuras de datos principales.
    """
    catalog = {
        # Grafo principal, donde cada nodo tendrá un mapa propio de domiciliarios
        "graph": gr.new_graph(100000, 0.5),
        # Mapa para la historia de cada domiciliario (stack de entregas)
        "delivery_person_history": mp.new_map(100000, 0.5),
        # Mapa para acumular tiempos y conteos de cada arista
        "edges_info": mp.new_map(100000, 0.5)
    }
    return catalog

def format_node_id(lat, lon):
    # Formatea la latitud y longitud a 4 decimales y los une con "_"
    return f"{float(lat):.4f}_{float(lon):.4f}"

def load_data(catalog, filename):
    """
    Carga los datos del reto y construye el grafo y las estructuras auxiliares.
    Retorna una matriz (lista de listas nativa) con los datos solicitados para el reporte.
    """
    # Inicializo contadores y estructuras auxiliares propias
    total_domicilios = 0
    total_tiempo = 0.0

    # Mapa para restaurantes únicos (orígenes)
    restaurantes = mp.new_map(100000, 0.5)
    # Mapa para ubicaciones de destino únicas
    destinos = mp.new_map(100000, 0.5)
    # Mapa para domiciliarios únicos
    domiciliarios = mp.new_map(100000, 0.5)

    start_time = get_time()
    with open(filename, encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:
            total_domicilios += 1
            order_id = row["ID"]
            delivery_person_id = row["Delivery_person_ID"]
            time_taken = float(row["Time_taken"])
            origin = format_node_id(row["Restaurant_latitude"], row["Restaurant_longitude"])
            destination = format_node_id(row["Delivery_location_latitude"], row["Delivery_location_longitude"])

            # Registro de domiciliarios únicos
            if not mp.contains(domiciliarios, delivery_person_id):
                mp.put(domiciliarios, delivery_person_id, True)

            # Registro de restaurantes únicos (orígenes)
            if not mp.contains(restaurantes, origin):
                mp.put(restaurantes, origin, True)

            # Registro de destinos únicos
            if not mp.contains(destinos, destination):
                mp.put(destinos, destination, True)

            # Sumo el tiempo total para el promedio
            total_tiempo += time_taken

            # Si el nodo no existe en el grafo, lo creo con un mapa de domiciliarios
            for node in [origin, destination]:
                if not gr.contains_vertex(catalog["graph"], node):
                    info = {"delivery_persons": mp.new_map(100, 0.5)}
                    gr.insert_vertex(catalog["graph"], node, info)

            # Agrego el domiciliario al mapa del nodo si no está presente
            for node in [origin, destination]:
                node_info = gr.get_vertex_information(catalog["graph"], node)
                delivery_map = node_info["delivery_persons"]
                if not mp.contains(delivery_map, delivery_person_id):
                    mp.put(delivery_map, delivery_person_id, True)

            # Genero la clave única para la arista (no dirigida)
            if origin < destination:
                edge_key = origin + "|" + destination
            else:
                edge_key = destination + "|" + origin

            # Acumulo el tiempo y el conteo para calcular el promedio después
            if not mp.contains(catalog["edges_info"], edge_key):
                mp.put(catalog["edges_info"], edge_key, {"total": 0, "count": 0})
            edge_info = mp.get(catalog["edges_info"], edge_key)
            edge_info["total"] += time_taken
            edge_info["count"] += 1

            # Manejo la historia del domiciliario para crear arista entre destinos consecutivos
            if not mp.contains(catalog["delivery_person_history"], delivery_person_id):
                mp.put(catalog["delivery_person_history"], delivery_person_id, st.new_stack())
            history = mp.get(catalog["delivery_person_history"], delivery_person_id)

            if not st.is_empty(history):
                prev = st.top(history)
                prev_dest = prev["destination"]
                prev_time = prev["time_taken"]
                # Genero la clave única para la arista entre destinos consecutivos
                if prev_dest < destination:
                    prev_edge_key = prev_dest + "|" + destination
                else:
                    prev_edge_key = destination + "|" + prev_dest
                if not mp.contains(catalog["edges_info"], prev_edge_key):
                    mp.put(catalog["edges_info"], prev_edge_key, {"total": 0, "count": 0})
                prev_edge_info = mp.get(catalog["edges_info"], prev_edge_key)
                avg_time = (prev_time + time_taken) / 2
                prev_edge_info["total"] += avg_time
                prev_edge_info["count"] += 1

                #Quito el edge entre el origen y destino
                if origin < destination:
                    edge_key_to_remove = origin + "|" + destination
                else:
                    edge_key_to_remove = destination + "|" + origin
                if mp.contains(catalog["edges_info"], edge_key_to_remove):
                    mp.remove(catalog["edges_info"], edge_key_to_remove)
            # Guardo el domicilio actual en la historia del domiciliario (push al stack)
            st.push(history, {
                "order_id": order_id,
                "origin": origin,
                "destination": destination,
                "time_taken": time_taken
            })

    # Al final, agrego las aristas al grafo con el tiempo promedio calculado
    edge_keys = mp.key_set(catalog["edges_info"])
    for i in range(lt.size(edge_keys)):
        edge_key = lt.get_element(edge_keys, i)
        edge_info = mp.get(catalog["edges_info"], edge_key)
        avg_time = edge_info["total"] / edge_info["count"] if edge_info["count"] > 0 else 0
        node1, node2 = edge_key.split("|")
        gr.add_edge(catalog["graph"], node1, node2, avg_time)

    end_time = get_time()
    elapsed_time = round(delta_time(start_time, end_time),2)

    # Construyo la matriz de resultados como lista de listas nativa de Python
    matriz = []
    fila = []
    fila.append(total_domicilios)
    fila.append(mp.size(domiciliarios))
    fila.append(gr.order(catalog["graph"]))
    fila.append(lt.size(edge_keys))
    fila.append(mp.size(restaurantes))
    fila.append(mp.size(destinos))
    promedio_tiempo = round(total_tiempo / total_domicilios,2) if total_domicilios > 0 else 0
    fila.append(promedio_tiempo)
    fila.append(elapsed_time)
    matriz.append(fila)

    return matriz
    #return mp.value_set(gr.adjacents(catalog["graph"],"11.1000_77.1000")) PRUEBA FUNCIONAMIENTO MIN

# Funciones de consulta sobre el catálogo

def get_data(catalog, id):
    """
    Retorna un dato por su ID.
    """
    #TODO: Consulta en las Llamar la función del modelo para obtener un dato
    pass


def req_1(catalog, origen, destino):
    """
    Retorna el camino de menor tiempo entre dos ubicaciones geográficas usando Dijkstra propio.
    """
    graph = catalog["graph"]
    # Inicializo la estructura de Dijkstra con el nodo origen y el número de nodos
    dijkstra = djs.new_dijsktra_structure(origen, gr.order(graph))
    # Distancia al origen es 0
    mp.put(dijkstra["visited"], origen, 0)
    # Insertar el origen en la cola de prioridad
    pq.insert(dijkstra["pq"], (0, origen))
    dijkstra["predecessors"][origen] = None

    while dijkstra["pq"]["size"] > 0:
        # Extraigo el nodo con menor distancia acumulada
        actual_tuple = pq.del_min(dijkstra["pq"])
        actual_dist, actual = actual_tuple
        if actual == destino:
            break
        adyacentes = gr.adjacents(graph, actual)
        adj_keys = mp.key_set(adyacentes)
        for j in range(lt.size(adj_keys)):
            vecino = lt.get_element(adj_keys, j)
            edge = mp.get(adyacentes, vecino)
            peso = edg.get_weight(edge)
            nueva_dist = actual_dist + peso
            # Si no ha sido visitado o encuentro un camino más corto
            if not mp.contains(dijkstra["visited"], vecino) or nueva_dist < mp.get(dijkstra["visited"], vecino):
                mp.put(dijkstra["visited"], vecino, nueva_dist)
                dijkstra["predecessors"][vecino] = actual
                pq.insert(dijkstra["pq"], (nueva_dist, vecino))

    # Reconstruyo el camino usando los predecesores
    camino = lt.new_list()
    actual = destino
    while actual is not None:
        lt.add_first(camino, actual)
        actual = dijkstra["predecessors"].get(actual)
    if not mp.contains(dijkstra["visited"], destino):
        return None, float('inf')
    return camino, mp.get(dijkstra["visited"], destino)


def req_2(catalog, origen, destino, delivery_person_id):
    """
    Retorna el camino con menos puntos intermedios entre dos ubicaciones
    para un domiciliario específico usando BFS propio.
    """
    graph = catalog["graph"]
    # BFS propio usando solo nodos visitados por el domiciliario
    visitados = mp.new_map(gr.order(graph), 0.5)
    prev = mp.new_map(gr.order(graph), 0.5)
    queue = lt.new_list()

    lt.add_last(queue, origen)
    mp.put(visitados, origen, True)
    mp.put(prev, origen, None)

    while lt.size(queue) > 0:
        actual = lt.get_element(queue, 0)
        lt.remove_first(queue)
        if actual == destino:
            break
        adyacentes = gr.adjacents(graph, actual)
        adj_keys = mp.key_set(adyacentes)
        for j in range(lt.size(adj_keys)):
            vecino = lt.get_element(adj_keys, j)
            node_info = gr.get_vertex_information(graph, vecino)
            delivery_map = node_info["delivery_persons"]
            if mp.contains(delivery_map, delivery_person_id) and not mp.contains(visitados, vecino):
                mp.put(visitados, vecino, True)
                mp.put(prev, vecino, actual)
                lt.add_last(queue, vecino)

    # Reconstruyo el camino usando solo tus estructuras
    camino = lt.new_list()
    actual = destino
    while actual is not None and mp.contains(prev, actual):
        lt.add_first(camino, actual)
        actual = mp.get(prev, actual)
    # Verifico si el camino es válido
    if lt.size(camino) == 0 or lt.get_element(camino, 0) != origen:
        return None
    return camino


def req_3(catalog):
    """
    Retorna el resultado del requerimiento 3
    """
    # TODO: Modificar el requerimiento 3
    pass


def req_4(catalog):
    """
    Retorna el resultado del requerimiento 4
    """
    # TODO: Modificar el requerimiento 4
    pass


def req_5(catalog):
    """
    Retorna el resultado del requerimiento 5
    """
    # TODO: Modificar el requerimiento 5
    pass

def req_6(catalog):
    """
    Retorna el resultado del requerimiento 6
    """
    # TODO: Modificar el requerimiento 6
    pass


def req_7(catalog):
    """
    Retorna el resultado del requerimiento 7
    """
    # TODO: Modificar el requerimiento 7
    pass


def req_8(catalog):
    """
    Retorna el resultado del requerimiento 8
    """
    # TODO: Modificar el requerimiento 8
    pass


# Funciones para medir tiempos de ejecucion

def get_time():
    """
    devuelve el instante tiempo de procesamiento en milisegundos
    """
    return float(time.perf_counter()*1000)


def delta_time(start, end):
    """
    devuelve la diferencia entre tiempos de procesamiento muestreados
    """
    elapsed = float(end - start)
    return elapsed
