import time
import csv
from DataStructures.Graph import digraph as gr
from DataStructures.Map import map_linear_probing as mp
from DataStructures.List import array_list as lt
from DataStructures.Stack import stack as st
from DataStructures.Graph import edge as edg

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
    Retorna el resultado del requerimiento 1
    """
    # TODO: Modificar el requerimiento 1
    pass

    """
    Retorna el camino de menor tiempo entre dos ubicaciones geográficas.
    Usa Dijkstra sobre el grafo principal.
    """
    graph = catalog["graph"]
    # Diccionario para almacenar el costo mínimo a cada nodo
    dist = {}
    # Diccionario para reconstruir el camino
    prev = {}
    # Lista de nodos por visitar (tupla: (costo acumulado, nodo))
    import heapq
    heap = []

    # Inicializo todas las distancias en infinito, excepto el origen
    vertices = gr.vertices(graph)
    for i in range(lt.size(vertices)):
        node = lt.get_element(vertices, i)
        dist[node] = float('inf')
        prev[node] = None
    dist[origen] = 0
    heapq.heappush(heap, (0, origen))

    while heap:
        costo_actual, actual = heapq.heappop(heap)
        if actual == destino:
            break
        adyacentes = gr.adjacents(graph, actual)
        adj_keys = mp.key_set(adyacentes)
        for j in range(lt.size(adj_keys)):
            vecino = lt.get_element(adj_keys, j)
            edge = mp.get(adyacentes, vecino)
            peso = edg.get_weight(edge)
            nuevo_costo = costo_actual + peso
            if nuevo_costo < dist[vecino]:
                dist[vecino] = nuevo_costo
                prev[vecino] = actual
                heapq.heappush(heap, (nuevo_costo, vecino))

    # Reconstruyo el camino
    camino = []
    actual = destino
    while actual is not None:
        camino.insert(0, actual)
        actual = prev[actual]
    if dist[destino] == float('inf'):
        return None, float('inf')  # No hay camino
    return camino, dist[destino]

def req_2(catalog, origen, destino, delivery_person_id):
    """
    Retorna el resultado del requerimiento 2
    """
    # TODO: Modificar el requerimiento 2
    pass


    """
    Retorna el camino con menos puntos intermedios entre dos ubicaciones
    para un domiciliario específico (solo por nodos donde ese domiciliario estuvo).
    """
    graph = catalog["graph"]
    from collections import deque

    # BFS clásico pero solo por nodos donde el domiciliario estuvo
    visitados = {}
    prev = {}
    queue = deque()
    queue.append(origen)
    visitados[origen] = True
    prev[origen] = None

    while queue:
        actual = queue.popleft()
        if actual == destino:
            break
        adyacentes = gr.adjacents(graph, actual)
        adj_keys = mp.key_set(adyacentes)
        for j in range(lt.size(adj_keys)):
            vecino = lt.get_element(adj_keys, j)
            # Solo avanzar si el domiciliario estuvo en el nodo vecino
            node_info = gr.get_vertex_information(graph, vecino)
            delivery_map = node_info["delivery_persons"]
            if mp.contains(delivery_map, delivery_person_id) and vecino not in visitados:
                visitados[vecino] = True
                prev[vecino] = actual
                queue.append(vecino)

    # Reconstruyo el camino
    camino = []
    actual = destino
    while actual is not None and actual in prev:
        camino.insert(0, actual)
        actual = prev[actual]
    if not camino or camino[0] != origen:
        return None  # No hay camino válido
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
