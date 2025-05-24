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
        "graph": gr.new_graph(directed=False),  # Asumiendo que tienes un módulo de grafos
        "node_delivery_persons": mp.new_map(),  # node_id -> set of delivery person IDs
        "delivery_person_history": mp.new_map(),  # delivery_person_id -> list of (order, origin, destination, time)
        "edges_info": mp.new_map()  # (node1, node2) -> list of times (for averaging)
    }
    return catalog

def format_node_id(lat, lon):
    return f"{float(lat):.4f}_{float(lon):.4f}"

def load_data(catalog, filename):
    """
    Carga los datos del reto
    """
    # TODO: Realizar la carga de datos
    pass

    # Abrir el archivo CSV

    with open(filename, encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for row in reader:
            # 1. Extraer los datos relevantes de la fila
            order_id = row["ID"]
            delivery_person_id = row["Delivery_person_ID"]
            time_taken = float(row["Time_taken"])
            origin = format_node_id(row["Restaurant_latitude"], row["Restaurant_longitude"])
            destination = format_node_id(row["Delivery_location_latitude"], row["Delivery_location_longitude"])

            # 2. Insertar nodos en el grafo si no existen
            if not gr.contains_vertex(catalog["graph"], origin):

                gr.insert_vertex(catalog["graph"], origin)
                # Crear lista de domiciliarios para el nodo origen
                mp.put(catalog["node_delivery_persons"], origin, lt.new_list())

            if not gr.contains_vertex(catalog["graph"], destination):
                gr.insert_vertex(catalog["graph"], destination)
                # Crear lista de domiciliarios para el nodo destino
                mp.put(catalog["node_delivery_persons"], destination, lt.new_list())

            # 3. Agregar el domiciliario a la lista de cada nodo (si no está ya)
            for node in [origin, destination]:
                delivery_list = mp.get(catalog["node_delivery_persons"], node)
                if not lt.is_present(delivery_list, delivery_person_id):
                    lt.add_last(delivery_list, delivery_person_id)

            # Ordena las localizaciones para evitar duplicados
            # mete tiempos a determinado edge para hacer los promedios
            if origin < destination:
                edge_key = origin + "|" + destination
            else:
                edge_key = destination + "|" + origin

            if not mp.contains(catalog["edges_info"], edge_key):
                mp.put(catalog["edges_info"], edge_key, lt.new_list())
            time_list = mp.get(catalog["edges_info"], edge_key)
            lt.add_last(time_list, time_taken)

            # 5. Manejar la historia del domiciliario para crear arista entre destinos consecutivos
            if not mp.contains(catalog["delivery_person_history"], delivery_person_id):
                mp.put(catalog["delivery_person_history"], delivery_person_id, st.new_stack())
            history = mp.get(catalog["delivery_person_history"], delivery_person_id)

            # Si la historia del domiciliario NO está vacía (es decir, ya hizo entregas antes)
            if not st.is_empty(history):
                # Obtener la última entrega realizada por este domiciliario (sin removerla del stack)
                prev = st.top(history)
                prev_dest = prev["destination"]      # Nodo destino de la entrega anterior
                prev_time = prev["time_taken"]       # Tiempo de la entrega anterior

                # Crear una arista entre el destino anterior y el destino actual
                # Se ordenan los nodos para evitar duplicados en el grafo no dirigido
                # Asumo que el orden en el que llegan los pedidos es el el orden en el que se hciieron
                # Hacer comparaciones mediante latitudes a ver cual es mas lejano resulta subjetivo
                if prev_dest < destination:
                    prev_edge_key = prev_dest + "|" + destination
                else:
                    prev_edge_key = destination + "|" + prev_dest

                # Si la arista aún no existe en el registro de tiempos, se crea una nueva lista de tiempos
                if not mp.contains(catalog["edges_info"], prev_edge_key):
                    mp.put(catalog["edges_info"], prev_edge_key, lt.new_list())

                # Obtener la lista de tiempos asociada a esta arista
                prev_time_list = mp.get(catalog["edges_info"], prev_edge_key)

                # Calcular el tiempo promedio entre la entrega anterior y la actual
                avg_time = (prev_time + time_taken) / 2
                # La razón por la que se promedia solo entre la entrega anterior y la actual 
                # (y no se espera a más entregas) es porque cada arista entre dos destinos consecutivos 
                # de un domiciliario representa únicamente ese par de entregas.
                #No se espera que haya más entregas entre esos dos mismos destinos consecutivos para ese domiciliario,
                # Agregar este tiempo promedio a la lista de tiempos de la arista
                lt.add_last(prev_time_list, avg_time)
            # Guardar el domicilio actual en la historia del domiciliario (push al stack)
            st.push(history, {
                "order_id": order_id,
                "origin": origin,
                "destination": destination,
                "time_taken": time_taken
            })

    # 6. Al final, agregar las aristas al grafo con el tiempo promedio
    # Obtener la lista de claves de aristas
    edge_keys = mp.key_set(catalog["edges_info"])
    num_edges = lt.size(edge_keys)

    for edge in edge_keys["elements"]:
        edge_key = edge["key"]
        time_list = mp.get(catalog["edges_info"], edge_key)
        total = 0
        count = lt.size(time_list)
        for j in range(0, count):
            total += lt.get_element(time_list, j)
        avg_time = total / count if count > 0 else 0
        node1, node2 = edge_key.split("|")
        # Agregar la arista al grafo con el tiempo promedio
        gr.add_edge(catalog["graph"], node1, node2, avg_time)


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
