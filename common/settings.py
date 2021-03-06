# coding: utf-8
# __author__: "John"
from os import path


PICKLE_FILE_PATH = path.join(path.dirname(path.abspath(__file__)), u"pickle_files")

TRAIN_MODEL_PICKLE = path.join(PICKLE_FILE_PATH, u"train_model")
TRAIN_X_POSITIVE_PICKLE = path.join(PICKLE_FILE_PATH, u"train_x_positive")
TRAIN_X_NEGATIVE_PICKLE = path.join(PICKLE_FILE_PATH, u"train_x_negative")
TEST_X_PICKLE = path.join(PICKLE_FILE_PATH, u"test_x")
TAG_DICT_PICKLE = path.join(PICKLE_FILE_PATH, u"tag_dict")
NAME_ATTRIBUTE_PICKLE = path.join(PICKLE_FILE_PATH, u"name_attribute")
TAGGED_ITEMS_ATTR_LIST = path.join(PICKLE_FILE_PATH, u"tagged_items_attr_list")


DEBUG = True
INFO = True
WARNING = True
ERROR = True
FATAL = True


HOST = u"172.16.1.120"
USER = u"dev"
PASSWD = u"Dev_123123"
DB = u"mp_portal"
