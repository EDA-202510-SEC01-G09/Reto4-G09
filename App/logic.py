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
from DataStructures.Queue import queue as q
from decimal import Decimal, ROUND_DOWN


def new_logic():
    """
    Crea el catálogo para almacenar las estructuras de datos principales.
    """
    catalog = {
        # Grafo principal, donde cada nodo tendrá un mapa propio de domiciliarios
        "graph": gr.new_graph(100000),
        # Mapa para la historia de cada domiciliario (stack de entregas)
        "delivery_person_history": mp.new_map(100000, 0.5),
        # Mapa para acumular tiempos y conteos de cada arista
        "edges_info": mp.new_map(100000, 0.5),
        # Mapa para restaurantes únicos (orígenes)
        "restaurantes": mp.new_map(100000, 0.5),
        "rest_connections": mp.new_map(100000, 0.5)
    }
    return catalog

def format_node_id(lat, lon):
    # Corta a 4 decimales sin redondear y rellena con ceros si es necesario
    lat_str = str(Decimal(str(lat)).quantize(Decimal('0.0001'), rounding=ROUND_DOWN))
    lon_str = str(Decimal(str(lon)).quantize(Decimal('0.0001'), rounding=ROUND_DOWN))
    return f"{lat_str}_{lon_str}"

def load_data(catalog, filename):
    """
    Carga los datos del reto y construye el grafo y las estructuras auxiliares.
    Retorna una matriz (lista de listas nativa) con los datos solicitados para el reporte.
    """

    total_domicilios = 0
    total_tiempo = 0.0

    restaurantes = catalog["restaurantes"]
    destinos = mp.new_map(100000, 0.5)
    domiciliarios = mp.new_map(100000, 0.5)
    # Mapa para listas de destinos actuales de cada restaurante
    rest_connections = catalog["rest_connections"]

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
                    mp.put(delivery_map, delivery_person_id, [{"vehicle type": mp.new_map(50,0.5)}, 0])
                vehicles_map_d = mp.get(delivery_map, delivery_person_id)[0]["vehicle type"]
                if not mp.contains(vehicles_map_d, row["Type_of_vehicle"]):
                    mp.put(vehicles_map_d, row["Type_of_vehicle"], 0)
                mp.get(delivery_map, delivery_person_id)[1] += 1

                contador_vehiculo = mp.get(vehicles_map_d, row["Type_of_vehicle"])
                mp.put(vehicles_map_d, row["Type_of_vehicle"], contador_vehiculo + 1)

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

            # --- INICIO OPTIMIZACIÓN SIN SETS ---
            # Lleva la lista de destinos actuales del restaurante
            if origin != destination:
                if not mp.contains(rest_connections, origin):
                    mp.put(rest_connections, origin, lt.new_list())
                dest_list = mp.get(rest_connections, origin)
                # Solo agrega si no está ya en la lista
                found = False
                for i in range(lt.size(dest_list)):
                    if lt.get_element(dest_list, i) == destination:
                        found = True
                        break
                if not found:
                    lt.add_last(dest_list, destination)
                mp.put(rest_connections, origin, dest_list)
            # --- FIN OPTIMIZACIÓN ---

            if not st.is_empty(history):
                prev = st.top(history)
                prev_dest = prev["destination"]
                prev_time = prev["time_taken"]
                # Primero agrega la arista entre destinos consecutivos
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

                # Luego elimina la arista restaurante-destino SOLO si el restaurante tiene más de un destino conectado
                prev_origin = prev["origin"]
                if prev_origin != prev_dest:
                    # Solo si no es un loop
                    if mp.contains(rest_connections, prev_origin):
                        dest_list = mp.get(rest_connections, prev_origin)
                        # Elimina prev_dest de la lista
                        idx_to_remove = -1
                        for i in range(lt.size(dest_list)):
                            if lt.get_element(dest_list, i) == prev_dest:
                                idx_to_remove = i
                                break
                        if idx_to_remove != -1:
                            lt.delete_element(dest_list, idx_to_remove)
                            mp.put(rest_connections, prev_origin, dest_list)
                        # Solo elimina la arista si quedan otros destinos conectados
                        if lt.size(dest_list) >= 1:
                            if prev_origin < prev_dest:
                                edge_key_to_remove = prev_origin + "|" + prev_dest
                            else:
                                edge_key_to_remove = prev_dest + "|" + prev_origin
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
        gr.add_edge(catalog["graph"], node1, node2, round(avg_time,2))

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
    Requerimiento 1: Identificar un camino simple entre dos ubicaciones geográficas usando DFS.
    Retorna:
      - Tiempo de ejecución (ms)
      - Cantidad de puntos geográficos en el camino
      - Secuencia de ubicaciones (camino simple)
      - Ids de domiciliarios que componen el camino (sin repetir)
      - Listado de restaurantes encontrados (ubicaciones origen en el camino)
    """


    graph = catalog["graph"]
    restaurantes = catalog["restaurantes"]  # Usamos set nativo solo para evitar repetidos en el resultado final

    domiciliarios = mp.new_map(100, 0.5)  # Mapa para domiciliarios únicos
    list_restaurantes = lt.new_list() # Lista para los restaurantes encontrados
    # Medir tiempo de ejecución
    start_time = get_time()

    # Ejecutar DFS desde el origen
    visited_map = dfs.dfs(graph, origen)

        # Obtener el camino como stack propio
    stack_camino = dfs.path_to(destino, visited_map)

    # Verificar si hay camino hasta el destino
    if stack_camino is None:
        return None

    # Convertir el stack a lista propia (de origen a destino)
    camino = lt.new_list() #A -> B -> C
    while not st.is_empty(stack_camino):
        lt.add_last(camino, st.pop(stack_camino))

    # Recorrer el camino para recolectar domiciliarios y restaurantes
    for i in range(lt.size(camino)):
        nodo = lt.get_element(camino, i)
        node_info = gr.get_vertex_information(graph, nodo)

        # Agregar todos los domiciliarios de este nodo (sin repetir)
        delivery_map = node_info["delivery_persons"]
        delivery_keys = mp.key_set(delivery_map)
        for j in range(lt.size(delivery_keys)):
            domiciliario = lt.get_element(delivery_keys, j)
            if not mp.contains(domiciliarios, domiciliario):
                mp.put(domiciliarios, domiciliario, True)

        # Agregar a restaurantes si el nodo es un origen de algún domicilio
        # (Puedes ajustar este criterio según tu modelo)
        
     
        if mp.contains(restaurantes, nodo):
            lt.add_last(list_restaurantes, nodo)

    camino_return = ""
    for punto in camino["elements"]:
        if punto == destino:
            camino_return += str(punto)
        else:
            camino_return += str(punto) + " -> "
    end_time = get_time()
    elapsed_time = round(delta_time(start_time, end_time), 2)

    result  = []
    fila = []
    fila.append(elapsed_time)
    fila.append(lt.size(camino))
    fila.append(camino_return)  
    fila.append(mp.key_set(domiciliarios)["elements"])
    fila.append(list_restaurantes["elements"])
    result.append(fila)

    return result

def bfs_domiciliario(graph, origen, destino, delivery_person_id):
    """
    BFS modificado: solo avanza por nodos donde el domiciliario estuvo.
    Usa la cola propia q y retorna un visited_map con estructura:
    {nodo: {"marked": True, "edge_to": predecesor, "dist_to": distancia}}
    """
    order = gr.order(graph)
    visited_map = mp.new_map(order, 0.5)
    # Inicializa el nodo origen en el mapa de visitados
    mp.put(visited_map, origen, {"marked": True, "edge_to": None, "dist_to": 0})
    queue = q.new_queue()
    q.enqueue(queue, origen)
    encontrado = False

    while not q.is_empty(queue):
        actual = q.dequeue(queue)
        if actual == destino:
            encontrado = True
            break
        adyacentes = gr.adjacents(graph, actual)
        adj_keys = mp.key_set(adyacentes)
        for vecino in adj_keys["elements"]:
            node_info = gr.get_vertex_information(graph, vecino)
            delivery_map = node_info["delivery_persons"]
            # Solo avanza si el domiciliario estuvo en el nodo vecino y no ha sido visitado
            if mp.contains(delivery_map, delivery_person_id) and not mp.contains(visited_map, vecino):
                actual_valor = mp.get(visited_map, actual)
                mp.put(
                    visited_map,
                    vecino,
                    {
                        "marked": True,
                        "edge_to": actual,
                        "dist_to": actual_valor["dist_to"] + 1
                    }
                )
                q.enqueue(queue, vecino)

    return visited_map, encontrado


def req_2(catalog, origen, destino, delivery_person_id):

    graph = catalog["graph"]
    restaurantes = catalog["restaurantes"]  

    domiciliarios = mp.new_map(100, 0.5)  # Mapa para domiciliarios únicos
    list_restaurantes = lt.new_list()      # Lista para los restaurantes encontrados

    start_time = get_time()

    visited_bfs, encontrado = bfs_domiciliario(graph, origen, destino, delivery_person_id)
    
    camino_o_d = bfs.path_to(destino, visited_bfs)
    camino = lt.new_list()  # Lista para el camino encontrado
    for point in camino_o_d["elements"]:
        lt.add_first(camino, point)

    if not encontrado or lt.size(camino) == 0 or lt.get_element(camino, 0) != origen:
        # Si no se encontró un camino o el origen no es el primer punto, retornamos vacío
        return None

    # Recorrer el camino para recolectar domiciliarios y restaurantes
    for nodo in camino["elements"]:
        node_info = gr.get_vertex_information(graph, nodo) #{"delivery_persons": mp.new_map(100, 0.5)} ID's: True
        # Agregar todos los domiciliarios de este nodo (sin repetir)
        delivery_map = node_info["delivery_persons"]
        delivery_keys = mp.key_set(delivery_map)
        for domiciliario in delivery_keys["elements"]:
            if not mp.contains(domiciliarios, domiciliario):
                mp.put(domiciliarios, domiciliario, True)
        # Agregar a restaurantes si el nodo es un origen de algún domicilio
        if mp.contains(restaurantes, nodo):
            lt.add_last(list_restaurantes, nodo)

    # Construir la secuencia del camino como string
    camino_return = ""
    for punto in camino["elements"]:
        if punto == destino:
            camino_return += str(punto) 
        else:
            camino_return += str(punto) + " -> "

    end_time = get_time()
    elapsed_time = round(delta_time(start_time, end_time), 2)

    result = []
    fila = []
    fila.append(elapsed_time)
    fila.append(lt.size(camino))
    if len(camino_return) > 60:
        print("\n",camino_return)
    else:
        fila.append(camino_return)
    fila.append(mp.key_set(domiciliarios)["elements"])
    fila.append(list_restaurantes["elements"])
    result.append(fila)

    return result


def req_3(catalog, punto_geografico):
    """
    Requerimiento 3: Identificar el domiciliario con mayor cantidad de pedidos para un punto geográfico en particular.
    Retorna:
      - Tiempo de ejecución (ms)
      - ID del domiciliario más popular en ese punto
      - Cantidad de pedidos atendidos por ese domiciliario en ese punto
      - Tipo de vehículo que más repitió ese domiciliario en ese punto
    """
    start_time = get_time()
    graph = catalog["graph"]

    # Verifica si el punto existe en el grafo
    if not gr.contains_vertex(graph, punto_geografico):
        return None

    # Obtiene el mapa de domiciliarios del nodo
    node_info = gr.get_vertex_information(graph, punto_geografico)
    delivery_map = node_info["delivery_persons"]

    # Busca el domiciliario con más pedidos en ese punto
    max_pedidos = 0
    domiciliario_popular = None

    delivery_keys = mp.key_set(delivery_map)

    for domiciliario_id in delivery_keys["elements"]:
        pedidos = mp.get(delivery_map, domiciliario_id)[1]  # contador de pedidos
        if pedidos > max_pedidos:
            max_pedidos = pedidos
            domiciliario_popular = domiciliario_id

    # Busca el tipo de vehículo más usado por ese domiciliario
    vehiculo_mas_usado = "-"
    if domiciliario_popular is not None:
        vehicles_map = mp.get(delivery_map, domiciliario_popular)[0]["vehicle type"]
        max_vehiculos = 0
        vehiculos_keys = mp.key_set(vehicles_map)
        for tipo in vehiculos_keys["elements"]:
            cantidad = mp.get(vehicles_map, tipo)
            if cantidad > max_vehiculos:
                max_vehiculos = cantidad
                vehiculo_mas_usado = tipo

    end_time = get_time()
    elapsed_time = round(delta_time(start_time, end_time), 2)

    fila = [
        elapsed_time,
        domiciliario_popular,
        max_pedidos,
        vehiculo_mas_usado
    ]
    return [fila]

def req_4(catalog, origen, destino):

    graph = catalog["graph"]
    start_time = get_time()

    # BFS para encontrar el camino más corto (en puntos) entre origen y destino
    visited_map = bfs.bfs(graph, origen)

    # Reconstruir el camino usando el visited_map
    camino = bfs.path_to(destino, visited_map)

    if not bfs.has_path_to(destino, visited_map) or lt.size(camino) == 0 or lt.get_element(camino, 0) != destino:
        return None
    
    camino_return = lt.new_list()  
    for point in camino["elements"]:
        lt.add_first(camino_return, point)

    # Obtener los domiciliarios de cada nodo del camino

    lista_domiciliarios = mp.new_map(100, 0.5)  # Mapa para domiciliarios únicos
    numero_de_nodos = 0 #cuento los nodos, el id que tenga el mismo numero que el numero de nodos, esta en todos y es la interseccion.
    for nodo in camino["elements"]:
        node_info = gr.get_vertex_information(graph, nodo)
        delivery_map = node_info["delivery_persons"]
        delivery_keys = mp.key_set(delivery_map)
        for delivery_person_id in delivery_keys["elements"]:
            if not mp.contains(lista_domiciliarios, delivery_person_id):
                mp.put(lista_domiciliarios, delivery_person_id, 0)
            mp.put(lista_domiciliarios, delivery_person_id, mp.get(lista_domiciliarios, delivery_person_id) + 1)
        numero_de_nodos += 1
    
    domiciliarios_comunes = lt.new_list()  # Lista para domiciliarios comunes

    for domiciliario_id in mp.key_set(lista_domiciliarios)["elements"]:
        if mp.get(lista_domiciliarios, domiciliario_id) == numero_de_nodos:
            lt.add_last(domiciliarios_comunes, domiciliario_id)
    
    # Construir la secuencia del camino como string
    camino_returnT = ""
    for punto in camino_return["elements"]:
        if punto == destino:
            camino_returnT += str(punto) 
        else:
            camino_returnT += str(punto) + " -> "

    end_time = get_time()
    elapsed_time = round(delta_time(start_time, end_time), 2)

    if len(camino_returnT) <= 60:
        fila = [
            elapsed_time,
            camino_return,
            domiciliarios_comunes["elements"]
        ]
    else:
        print("\n",camino_returnT)
        fila = [
            elapsed_time,
            domiciliarios_comunes["elements"]
        ]
    return [fila]


def req_6(catalog, origen):
    """
    Requerimiento 6: Identificar los caminos de costo mínimo en tiempo desde una ubicación geográfica específica.
    Retorna:
      - Tiempo de ejecución (ms)
      - Cantidad de ubicaciones alcanzables (incluyendo la inicial)
      - Identificadores de las ubicaciones alcanzables (ordenados alfabéticamente)
      - El camino de costo mínimo con mayor tiempo total (secuencia y tiempo)
    """
    graph = catalog["graph"]
    start_time = get_time()

    # Inicializar estructura de Dijkstra desde el origen

    dijkstra = djs.dijkstra(graph, origen)

    #print(mp.key_set(dijkstra["visited"]))

    # Recopilar ubicaciones alcanzables y encontrar la de mayor tiempo
    ubicaciones = lt.new_list()
    mayor_tiempo = -1
    destino_mayor = None
    visited_keys = mp.key_set(dijkstra["visited"])
    for nodo in visited_keys["elements"]:
        lt.add_last(ubicaciones, nodo)
        tiempo = mp.get(dijkstra["visited"], nodo)
        if tiempo > mayor_tiempo and nodo != origen:
            mayor_tiempo = tiempo
            destino_mayor = nodo
    ubicaciones_ordenadas = lt.merge_sort(ubicaciones, compare_alphabetical)

    # Ordenar alfabéticamente las ubicaciones alcanzables

    # Reconstruir el camino de mayor tiempo

    camino_a_mayor = djs.path_to(destino_mayor, dijkstra)

    # Construir la secuencia del camino como string
    camino_return = ""
    for punto in camino_a_mayor:
        if punto == destino_mayor:
            camino_return += str(punto)
        else:
            camino_return += str(punto) + " -> "

    end_time = get_time()
    elapsed_time = round(delta_time(start_time, end_time), 2)

    if len(camino_return) > 60:
        print("\n",camino_return)
        camino_return = "Camino muy largo para mostrar"
        fila = [
            elapsed_time,
            len(ubicaciones["elements"]),
            ubicaciones_ordenadas["elements"],
            round(mayor_tiempo, 2) if mayor_tiempo != -1 else 0
        ]
    else:
        fila = [
            elapsed_time,
            len(ubicaciones),
            ubicaciones_ordenadas["elements"],
            camino_return,
            round(mayor_tiempo, 2) if mayor_tiempo != -1 else 0
        ]
    return [fila]

def compare_alphabetical(a, b):

    return a < b

def req_7(catalog, origen, delivery_person_id):
    """
    Requerimiento 7: Árbol de recubrimiento mínimo (MST) para un domiciliario desde un punto inicial.
    """
    start_time = get_time()
    graph = catalog["graph"]

    # 1. Filtrar nodos donde estuvo el domiciliario
    nodos_domiciliario = []
    all_nodes = gr.vertices(graph)
    for nodo in all_nodes["elements"]:
        node_info = gr.get_vertex_information(graph, nodo)
        delivery_map = node_info["delivery_persons"]
        if mp.contains(delivery_map, delivery_person_id):
            nodos_domiciliario.append(nodo)

    # 2. Crear subgrafo solo con esos nodos y aristas entre ellos
    subgrafo = gr.new_graph(len(nodos_domiciliario))
    for nodo in nodos_domiciliario:
        gr.insert_vertex(subgrafo, nodo, gr.get_vertex_information(graph, nodo))
    for nodo in nodos_domiciliario:
        adyacentes = gr.adjacents(graph, nodo)
        adj_keys = mp.key_set(adyacentes)
        for vecino in adj_keys["elements"]:
            if vecino in nodos_domiciliario:
                peso = mp.get(adyacentes, vecino)["weight"]
                gr.add_edge(subgrafo, nodo, vecino, peso)

    # 3. Ejecutar Prim desde el origen en el subgrafo
    mst = prim_mst(subgrafo, origen)
    mst_edges = mst["edges"]
    mst_nodes = mst["nodes"]
    total_tiempo = 0

    for i in range(lt.size(mst_edges)):
        edge = lt.get_element(mst_edges, i)
        total_tiempo += edge["weight"]

    # Asegurarse de incluir el nodo origen aunque no tenga aristas
    found = False
    for i in range(lt.size(mst_nodes)):
        if lt.get_element(mst_nodes, i) == origen:
            found = True
            break
    if not found:
        lt.add_last(mst_nodes, origen)

    # Ordenar alfabéticamente las ubicaciones
    # Convierte mst_nodes (lista propia) a una nueva lista propia para ordenar
    ubicaciones_lista = lt.new_list()
    for i in range(lt.size(mst_nodes)):
        lt.add_last(ubicaciones_lista, lt.get_element(mst_nodes, i))

    ubicaciones_ordenadas = lt.merge_sort(ubicaciones_lista, compare_alphabetical)["elements"]

    end_time = get_time()
    elapsed_time = round(delta_time(start_time, end_time), 2)

    fila = [
        elapsed_time,
        lt.size(mst_nodes),
        ubicaciones_ordenadas,
        round(total_tiempo, 2)
    ]
    return [fila]

def prim_mst(graph, origen):
    """
    Algoritmo de Prim para Árbol de Recubrimiento Mínimo usando solo listas y mapas.
    Usa pq.insert(heap, element) (sin prioridad explícita).
    """
    vertices = gr.vertices(graph)["elements"]
    visited = mp.new_map(len(vertices), 0.5)
    mst_edges = lt.new_list()
    mst_nodes = lt.new_list()

    # Cola de prioridad propia: (peso, desde, hasta)
    heap = pq.new_heap(cmp_mst)
    pq.insert(heap, (0, None, origen))  # Solo dos argumentos

    while not pq.is_empty(heap):
        peso, desde, actual = pq.remove(heap)
        if mp.contains(visited, actual):
            continue
        mp.put(visited, actual, True)
        # Solo agrega si no está ya en la lista
        found = False
        for i in range(lt.size(mst_nodes)):
            if lt.get_element(mst_nodes, i) == actual:
                found = True
                break
        if not found:
            lt.add_last(mst_nodes, actual)
        if desde is not None:
            lt.add_last(mst_edges, {"vertexA": desde, "vertexB": actual, "weight": peso})
        adyacentes = gr.adjacents(graph, actual)
        adj_keys = mp.key_set(adyacentes)
        for vecino in adj_keys["elements"]:
            if not mp.contains(visited, vecino):
                peso_arista = mp.get(adyacentes, vecino)["weight"]
                pq.insert(heap, (peso_arista, actual, vecino))  # Solo dos argumentos

    return {"edges": mst_edges, "nodes": mst_nodes}
def cmp_mst(a, b):
    """
    Compara dos tuplas (peso, desde, hasta) para el heap del MST.
    Retorna:
        1  si a tiene mayor prioridad (menor peso) que b
        0  si tienen igual peso
        -1 si a tiene menor prioridad (mayor peso) que b
    """
    if a[0] < b[0]:
        return 1
    elif a[0] == b[0]:
        return 0
    else:
        return -1
    
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
