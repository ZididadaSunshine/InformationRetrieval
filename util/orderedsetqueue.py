from util.orderedset import OrderedSet
from queue import Queue


class OrderedSetQueue(Queue):
    def __init__(self, maxsize=0):
        super().__init__(maxsize)
        self.queue = OrderedSet()

    def _put(self, item):
        self.queue.add(item)

    def _get(self):
        return self.queue.pop(last=False)


class UrlQueue(OrderedSetQueue):
    def __init__(self, tag='', maxsize=0):
        super().__init__(maxsize)
        self._tag = tag

    def tag(self):
        return self._tag

    def has_tag(self, tag):
        return self._tag == tag

    def __hash__(self):
        return hash(self._tag)

    def __eq__(self, other):
        if isinstance(other, UrlQueue):
            return self.has_tag(other.tag())
        return set(self) == set(other)