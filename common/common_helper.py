# coding: utf-8
# __author__: u"John"
import settings
import time


def time_elapse(function):
    def wrapper(*args, **kwargs):
        if settings.INFO:
            start = time.time()
            ret = function(*args, **kwargs)
            print "function '{0}' elapse {1} seconds".format(function.__name__, time.time() - start)
        else:
            ret = function(*args, **kwargs)
        return ret
    return wrapper

#  test
@time_elapse
def foo(cat):
    time.sleep(1)
    print cat
    return

# foo("kitty")

