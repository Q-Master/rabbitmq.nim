import std/lists
export lists

proc empty*[T](L: SomeLinkedCollection[T]): bool = L.head.isNil()

proc popFront*[T](L: var SinglyLinkedList[T]): SinglyLinkedNode[T] =
  result = L.head
  L.remove(result)

proc popBack*[T](L: var SinglyLinkedList[T]): SinglyLinkedNode[T] =
  result = L.tail
  L.remove(result)

proc popFront*[T](L: var DoublyLinkedList[T] | DoublyLinkedRing[T]): DoublyLinkedNode[T] =
  result = L.head
  L.remove(result)

proc popBack*[T](L: var DoublyLinkedList[T]): DoublyLinkedNode[T] =
  result = L.tail
  L.remove(result)

proc reset*[T](L: var SinglyLinkedList[T], n: SinglyLinkedNode[T]) {.inline.} =
  if n != nil and n != L.head:
    L.head = n
    L.tail.next = n

proc reset*[T](L: var DoublyLinkedList[T], n: DoublyLinkedNode[T]) {.inline.} =
  if n != nil and n != L.head:
    n.prev = n
    L.head = n
