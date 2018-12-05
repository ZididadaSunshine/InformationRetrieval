from util.orderedset import OrderedSet
from queue import Queue


class OrderedSetQueue(Queue):
    def _init(self, maxsize):
        self.queue = OrderedSet()

    def _put(self, item):
        self.queue.add(item)

    def _get(self):
        return self.queue.pop()


class UrlQueue(OrderedSetQueue):
    def _init(self, tag):
        self.queue = OrderedSet()
        self.tag = tag

    def tag(self):
        return self.tag

    def has_tag(self, tag):
        return self.tag == tag

    def __eq__(self, other):
        if isinstance(other, UrlQueue):
            return self.has_tag(other.tag())
        return set(self) == set(other)