import sys
from App import logic as lg
from DataStructures.List import array_list as ar
from DataStructures.Map import map_linear_probing as lp
from DataStructures.Tree import red_black_tree as rbt
import tabulate as tb
import csv  
from DataStructures.List import array_list as ar

def new_logic():

    return lg.new_logic()

def print_menu():
    print("Bienvenido")
    print("1- Cargar información")
    print("2- Ejecutar Requerimiento 1")
    print("3- Ejecutar Requerimiento 2")
    print("4- Ejecutar Requerimiento 3")
    print("5- Ejecutar Requerimiento 4")
    print("6- Ejecutar Requerimiento 5")
    print("7- Ejecutar Requerimiento 6")
    print("8- Ejecutar Requerimiento 7")
    print("9- Ejecutar Requerimiento 8 (Bono)")
    print("0- Salir")

def load_data(control):

    file = input("Ingrese el nombre del archivo a cargar: ")
    file_path = f"Data/deliverytime_{file}.csv" 

    data = lg.load_data(control, file_path)
    headers_generales = ["Total domicilios", "Total domiciliarios", "Orden", "Arcos", "# de restaurantes únicos", "# de destinos únicos", "Promedio tiempo de entrega", "Tiempo de carga de datos (ms)"]
    print(tb.tabulate(data, headers_generales, tablefmt="pretty"))

def print_req_1(control):

    origen_lat = input("\nIngrese la latitud del origen del camino que busca: ")
    origen_lon = input("Ingrese la longitud del origen del camino que busca: ")
    destino_lat = input("Ingrese la latitud del destino del camino que busca: ")
    destino_lon = input("Ingrese la longitud del destino del camino que busca: ")
    origen = lg.format_node_id(origen_lat, origen_lon)
    destino = lg.format_node_id(destino_lat, destino_lon)


    data = lg.req_1(control, origen, destino)
    if data is None:
        print("No se encontró un camino entre los puntos dados.")
    else:
        headers = ["Tiempo de carga (ms)", "Cantidad de puntos", "Camino", "Domiciliarios", "Restaurantes"]
        print(tb.tabulate(data, headers, tablefmt="pretty"))

def print_req_2(control):

    origen_lat = input("\nIngrese la latitud del origen del camino que busca: ")
    origen_lon = input("Ingrese la longitud del origen del camino que busca: ")
    destino_lat = input("Ingrese la latitud del destino del camino que busca: ")
    destino_lon = input("Ingrese la longitud del destino del camino que busca: ")
    delivery_person_id = input("Ingrese el ID del domiciliario: ")
    origen = lg.format_node_id(origen_lat, origen_lon)
    destino = lg.format_node_id(destino_lat, destino_lon)

    data = lg.req_2(control, origen, destino, delivery_person_id)
    if data is None:
        print("No se encontró un camino entre los puntos dados.")
    elif len(data[0]) == 4:
        print("El camino es muy largo para tabular")
        headers = ["Tiempo de carga (ms)", "Cantidad de puntos", "Domiciliarios", "Restaurantes"]
        print(tb.tabulate(data, headers, tablefmt="pretty"))
    else:
        headers = ["Tiempo de carga (ms)", "Cantidad de puntos", "Camino", "Domiciliarios", "Restaurantes"]
        print(tb.tabulate(data, headers, tablefmt="pretty"))

def print_req_3(control):

    lat = input("Ingrese la latitud del punto geográfico: ")
    lon = input("Ingrese la longitud del punto geográfico: ")
    punto_geografico = lg.format_node_id(lat, lon)

    data = lg.req_3(control, punto_geografico)
    if data is None:
        print("No se encontró información para el punto geográfico dado.")
    else:
        headers = ["Tiempo de carga (ms)", "Domiciliario más popular", "Máximo de pedidos", "Vehículo más usado"]
        print(tb.tabulate(data, headers, tablefmt="pretty"))


def print_req_4(control):
    
    origen_lat = input("\nIngrese la latitud del origen del camino que busca: ")
    origen_lon = input("Ingrese la longitud del origen del camino que busca: ")
    destino_lat = input("Ingrese la latitud del destino del camino que busca: ")
    destino_lon = input("Ingrese la longitud del destino del camino que busca: ")
    origen = lg.format_node_id(origen_lat, origen_lon)
    destino = lg.format_node_id(destino_lat, destino_lon)

    data = lg.req_4(control, origen, destino)

    if data is None:
        print("No se encontró un camino entre los puntos dados.")

    elif len(data[0]) == 2:
        print("El camino es muy largo para tabular")
        headers = ["Tiempo de carga (ms)", "Domiciliarios comunes"]
        print(tb.tabulate(data, headers, tablefmt="pretty"))
    else:
        headers = ["Tiempo de carga (ms)", "Camino", "Domiciliarios comunes"]
        print(tb.tabulate(data, headers, tablefmt="pretty"))



def print_req_5(control):
    """
        Función que imprime la solución del Requerimiento 5 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 5
    pass


def print_req_6(control):
    """
    Función que imprime la solución del Requerimiento 6 en consola
    """
    origen_lat = input("\nIngrese la latitud del origen: ")
    origen_lon = input("Ingrese la longitud del origen: ")
    origen = lg.format_node_id(origen_lat, origen_lon)

    data = lg.req_6(control, origen)
    if data is None or len(data) == 0:
        print("No se encontraron caminos desde el punto dado.")
    elif len(data[0]) == 4:
        print("El camino es muy largo para tabular")
        headers = [
            "Tiempo de carga (ms)",
            "Cantidad de ubicaciones alcanzables",
            "Ubicaciones alcanzables (ordenadas)",
            "Camino de mayor tiempo"
        ]
        print(tb.tabulate(data, headers, tablefmt="pretty"))
    else:
        headers = [
            "Tiempo de carga (ms)",
            "Cantidad de ubicaciones alcanzables",
            "Ubicaciones alcanzables (ordenadas)",
            "Camino de mayor tiempo",
            "Tiempo total del camino"
        ]
        print(tb.tabulate(data, headers, tablefmt="pretty"))

def print_req_7(control):
    """
        Función que imprime la solución del Requerimiento 7 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 7
    pass


def print_req_8(control):
    """
        Función que imprime la solución del Requerimiento 8 en consola
    """
    # TODO: Imprimir el resultado del requerimiento 8
    pass


# Se crea la lógica asociado a la vista
control = new_logic()

# main del ejercicio
def main():
    """
    Menu principal
    """
    working = True
    #ciclo del menu
    while working:
        print_menu()
        inputs = input('Seleccione una opción para continuar\n')
        if int(inputs) == 1:
            print("Cargando información de los archivos ....\n")
            data = load_data(control)
        elif int(inputs) == 2:
            print_req_1(control)

        elif int(inputs) == 3:
            print_req_2(control)

        elif int(inputs) == 4:
            print_req_3(control)

        elif int(inputs) == 5:
            print_req_4(control)

        elif int(inputs) == 6:
            print_req_5(control)

        elif int(inputs) == 7:
            print_req_6(control)

        elif int(inputs) == 8:
            print_req_7(control)

        elif int(inputs) == 9:
            print_req_8(control)

        elif int(inputs) == 0:
            working = False
            print("\nGracias por utilizar el programa") 
        else:
            print("Opción errónea, vuelva a elegir.\n")
    sys.exit(0)
