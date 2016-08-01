# coding: utf-8
# __author__: "John"
import chardet
from debug_helper import debug


def smart_decoder(var):
    if isinstance(var, unicode):
        return var
    else:
        debug(chardet.detect(var))
        return var.decode(chardet.detect(var)[u"encoding"])


if __name__ == u"__main__":
    test_string = u"测试"
    print smart_decoder(test_string)
    test_string = "测试"
    print smart_decoder(test_string)
    test_string = u"测试".encode(u"gb2312")
    print smart_decoder(test_string)

