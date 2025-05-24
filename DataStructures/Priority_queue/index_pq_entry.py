def new_pq_entry(key, index):
    """
    Crea una nueva entrada (de tipo :ref:`index_pq_entry<index-priority-queue-entry>`) de una cola de prioridad.

    La entrada es creada con los siguientes atributos:

    - :attr:`key`: Llave de la entrada. Inicializada con el valor ``None``.
    - :attr:`index`: Indice de la entrada. Inicializada con el valor dado ``index``.

    :param key: Llave de la entrada.
    :type key: any
    :param value: Valor de la entrada.
    :type value: any

    :return: Entrada de una cola de prioridad.
    :rtype: :ref:`index_pq_entry<index-priority-queue-entry>`
    """
    return {
        "key": key,
        "index": index,
    }

def set_key(my_entry, key):
    """
    Establece un valor nuevo a la ``key`` de una entrada recibida.

    :param my_entry: Entrada a modificar.
    :type my_entry: :ref:`index_pq_entry<index-priority-queue-entry>`
    :param key: Llave nueva de la entrada.
    :type key: any

    :return: Entrada con la llave modificada.
    :rtype: :ref:`index_pq_entry<index-priority-queue-entry>`
    """
    my_entry["key"] = key
    return my_entry

def set_index(my_entry, index):
    """
    Establece un indice nuevo al ``index`` de una entrada recibida.

    :param my_entry: Entrada a modificar.
    :type my_entry: :ref:`index_pq_entry<index-priority-queue-entry>`
    :param index: Valor nuevo de la entrada.
    :type index: any

    :return: Entrada con el valor modificado.
    :rtype: :ref:`index_pq_entry<index-priority-queue-entry>`
    """
    my_entry["index"] = index
    return my_entry

def get_key(my_entry):
    """
    Obtiene la llave ``key`` de una entrada recibida.

    :param my_entry: Entrada a modificar.
    :type my_entry: :ref:`index_pq_entry<index-priority-queue-entry>`

    :return: Llave de la entrada.
    :rtype: any
    """
    return my_entry["key"]

def get_index(my_entry):
    """
    Obtiene el indice ``index`` de una entrada recibida.

    :param my_entry: Entrada a modificar.
    :type my_entry: :ref:`index_pq_entry<index-priority-queue-entry>`

    :return: Valor de la entrada.
    :rtype: any
    """
    return my_entry["index"]