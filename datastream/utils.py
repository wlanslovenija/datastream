class class_property(property):
    def __get__(self, instance, typ):
        if instance is None:
            return super(class_property, self).__get__(typ, typ)
        return super(class_property, self).__get__(instance, typ)
