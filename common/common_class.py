# coding: utf-8
# __author__: "John"


class AttributeDict(dict):
    """
    能够把dict的key当作class的attribute
    """
    def __getattr__(self, attr):
        return self[attr]

    def __setattr__(self, attr, value):
        self[attr] = value


if __name__ == u"__main__":
    a = AttributeDict()
    a[u"a"] = 1
    print a.a




