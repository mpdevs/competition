# coding: utf-8\
# __author__: "John"
from settings import DEBUG, INFO


def debug(var):
    if DEBUG:
        print (var)


def info(var):
    if INFO:
        print (var)
