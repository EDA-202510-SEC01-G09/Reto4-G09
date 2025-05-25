import time
import csv
from DataStructures.Graph import digraph as gr
from DataStructures.Map import map_linear_probing as mp
from DataStructures.List import array_list as lt
from DataStructures.Stack import stack as st

def new_logic():
    """
    Crea el catalogo para almacenar las estructuras de datos
    """
    #TODO: Llama a las funciónes de creación de las estructuras de datos
    

    """
    Crea el catálogo para almacenar las estructuras de datos principales.
    """
    catalog = {
        "graph": gr.new_graph(100000),  # Asumiendo que tienes un módulo de grafos
        "delivery_person_history": mp.new_map(100000, 0.5),  # delivery_person_id -> list of (order, origin, destination, time)
        "edges_info": mp.new_map(100000, 0.5)  # (node1, node2) -> list of times (for averaging)
    }
    return catalog

def format_node_id(lat, lon):
    return f"{float(lat):.4f}_{float(lon):.4f}"

def load_data(catalog, filename):
    """
    Carga los datos del reto y construye el grafo y las estructuras auxiliares.
    Optimizada: solo usa tus estructuras propias.
    """
    start_time = get_time()
    with open(filename, encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:
            order_id = row["ID"]
            delivery_person_id = row["Delivery_person_ID"]
            time_taken = float(row["Time_taken"])
            origin = format_node_id(row["Restaurant_latitude"], row["Restaurant_longitude"])
            destination = format_node_id(row["Delivery_location_latitude"], row["Delivery_location_longitude"])

            # Si el nodo no existe en el grafo, lo creo con una lista de domiciliarios
            for node in [origin, destination]:
                if not gr.contains_vertex(catalog["graph"], node):
                    info = {"delivery_persons": lt.new_list()}
                    gr.insert_vertex(catalog["graph"], node, info)

            # Agrego el domiciliario a la lista del nodo si no está presente
            for node in [origin, destination]:
                node_info = gr.get_vertex_information(catalog["graph"], node)
                delivery_list = node_info["delivery_persons"]
                if not lt.is_present(delivery_list, delivery_person_id):
                    lt.add_last(delivery_list, delivery_person_id)

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

            # Guardo el domicilio actual en la historia del domiciliario (push al stack)
            st.push(history, {
                "order_id": order_id,
                "origin": origin,
                "destination": destination,
                "time_taken": time_taken
            })

    # Al final, agrego las aristas al grafo con el tiempo promedio calculado
    edge_keys = mp.key_set(catalog["edges_info"])
    print(lt.size(edge_keys))
    for i in range(lt.size(edge_keys)):
        edge_key = lt.get_element(edge_keys, i)
        edge_info = mp.get(catalog["edges_info"], edge_key)
        avg_time = edge_info["total"] / edge_info["count"] if edge_info["count"] > 0 else 0
        node1, node2 = edge_key.split("|")
        gr.add_edge(catalog["graph"], node1, node2, avg_time)

    end_time = get_time()
    elapsed_time = delta_time(start_time, end_time)
    print(f"Datos cargados en {elapsed_time:.2f} ms")


# Funciones de consulta sobre el catálogo

def get_data(catalog, id):
    """
    Retorna un dato por su ID.
    """
    #TODO: Consulta en las Llamar la función del modelo para obtener un dato
    pass


def req_1(catalog):
    """
    Retorna el resultado del requerimiento 1
    """
    # TODO: Modificar el requerimiento 1
    pass


def req_2(catalog):
    """
    Retorna el resultado del requerimiento 2
    """
    # TODO: Modificar el requerimiento 2
    pass


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
