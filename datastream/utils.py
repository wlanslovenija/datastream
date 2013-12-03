class hashabledict(dict):
    def __key(self, d):
        return tuple((k, d[k]) for k in sorted(d))

    def __hash__(self):
        return hash(self.__key(self))

    def __eq__(self, other):
        return self.__key(self) == self.__key(other)


class class_property(property):
    def __get__(self, instance, type):
        if instance is None:
            return super(class_property, self).__get__(type, type)
        return super(class_property, self).__get__(instance, type)
