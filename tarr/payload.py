'''
Immutable data structure to hold the results and history of transformations.

Functions/methods constructing new objects:
    new(input)
    payload.with_new_result(transform_name, key, value[, new_input])
    payload.with_new_input(transform_name, new_input)
    payload.with_key_removed(transform_name, key)

>>> p1 = new(u'input string')
>>> first_word = p1.input.split()[0]
>>> p2 = p1.with_new_result(
...     u'demo1', u'first word', first_word,
...     new_input=p1.input[len(first_word):])
>>> p2.input
u' string'
>>> list(p2.keys())
[u'first word']
>>> p2.transform_name, p2.parent.transform_name
[u'demo1', u'START']
>>> # XXX: not yet
>>> p2.history()
[u'demo1', u'START']
'''


NO_NEW_INPUT = object()


class BasePayload(object):

    input = object

    def keys(self):
        return list()

    def __getitem__(self, key):
        raise KeyError(key)

    def with_new_result(self, transform_name, key, value, new_input=NO_NEW_INPUT):
        return AddResult(self, transform_name, key, value, new_input)

    def with_key_removed(self, transform_name, key):
        return RemoveKey(self, transform_name, key)

    def with_new_input(self, transform_name, new_input):
        return NewInput(self, transform_name, new_input)


class New(BasePayload):

    transform_name = u'START'

    def __init__(self, input):
        self.input = input


new = New


class AddResult(BasePayload):

    def __init__(self, parent, transform_name, key, value, new_input):
        self.transform_name = transform_name
        self.parent = parent
        self.key = key
        self.value = value
        self.input = parent.input if new_input is NO_NEW_INPUT else new_input

    def keys(self):
        yield self.key
        for key in self.parent.keys():
            if self.key != key:
                yield key

    def __getitem__(self, key):
        return self.value if key == self.key else self.parent[key]


class RemoveKey(BasePayload):

    def __init__(self, parent, transform_name, key_to_remove):
        self.transform_name = transform_name
        self.parent = parent
        self.key_to_remove = key_to_remove

    def keys(self):
        for key in self.parent.keys():
            if key != self.key_to_remove:
                yield key

    @property
    def input(self):
        return self.parent.input

    def __getitem__(self, key):
        if key == self.key_to_remove:
            raise KeyError(u'{} has been removed'.format(key))
        else:
            return self.parent[key]


class NewInput(BasePayload):

    def __init__(self, parent, transform_name, new_input):
        self.transform_name = transform_name
        self.parent = parent
        self.input = new_input

    def keys(self):
        return self.parent.keys()

    def __getitem__(self, key):
        return self.parent[key]
