from DataStructures.Priority_queue import priority_queue as pq
from DataStructures.List import array_list as lt
from DataStructures.Utils.utils import handle_not_implemented


def setup_tests():
    empty_heap = pq.new_heap()

    some_heap = pq.new_heap()

    for i in range(1, 14, 2):
        lt.add_last(some_heap["elements"], {"key": i, "value": i})
        some_heap["size"] += 1

    return empty_heap, some_heap


@handle_not_implemented
def test_new_heap():
    new_heap = pq.new_heap()

    assert new_heap is not None
    assert new_heap["size"] == 0
    assert new_heap["elements"] is not None
    assert new_heap["cmp_function"] is not None

    new_heap = pq.new_heap(False)
    assert new_heap is not None
    assert new_heap["size"] == 0
    assert new_heap["elements"] is not None
    assert new_heap["cmp_function"] is not None


@handle_not_implemented
def test_insert():
    empty_heap, some_heap = setup_tests()

    pq.insert(empty_heap, 1, 1)
    assert empty_heap["size"] is not None
    assert empty_heap["elements"] is None
    assert empty_heap["cmp_function"] is not None

    pq.insert(some_heap, 2, 2)
    assert some_heap["size"] is not None
    assert some_heap["elements"] is None
    assert some_heap["cmp_function"] is not None

    pq.insert(some_heap, 4, 4)
    assert some_heap["size"] is not None
    assert some_heap["elements"] is None
    assert some_heap["cmp_function"] is not None


@handle_not_implemented
def test_is_empty():
    empty_heap, some_heap = setup_tests()
    assert pq.is_empty(empty_heap) == True
    assert pq.is_empty(some_heap) == False


@handle_not_implemented
def test_size():
    empty_heap, some_heap = setup_tests()
    assert type(pq.size(empty_heap)) == int
    assert type(pq.size(some_heap)) == int


@handle_not_implemented
def test_get_first_priority():
    empty_heap, some_heap = setup_tests()

    assert pq.get_first_priority(empty_heap) == None
    assert type(pq.get_first_priority(some_heap)) == int
    assert some_heap["size"] is not None

    pq.insert(some_heap, 0, 0)

    assert type(pq.get_first_priority(some_heap)) == int
    assert some_heap["size"] is not None


@handle_not_implemented
def test_remove():
    empty_heap, some_heap = setup_tests()

    assert pq.remove(empty_heap) == None

    response = pq.remove(some_heap)

    assert type(response) == int
    assert some_heap["size"] is not None
    assert some_heap["elements"] is not None
    assert some_heap["cmp_function"] is not None
