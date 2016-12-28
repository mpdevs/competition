# coding: utf-8
import pickle
import numpy as np
from pprint import pprint


def pickle_load(file_name):
    if u".pickle" in file_name:
        pickle_name = file_name
    else:
        pickle_name = file_name + u".pickle"

    f = open(pickle_name, u"rb")
    load_object = pickle.load(f)
    f.close()
    return load_object
# df = pickle_load(u"todo_df.pickle")
# # pprint(df)
# print df[df.source_item == 530228506892]
# lst = df.source_item.values.tolist()
#
# for line in lst:
#     if str(line) == "530228506892":
#         print "in"

df = pickle_load(u"evaluations.pickle")
df = df[["source_item", "target_item"]].values.tolist()
# for column in df.columns:
#     for item in df[column].unique().tolist():
#         print item
for index, line in enumerate(df):
    source_item, target_item = line
    if str(source_item).replace(" ", "") == str(530228506892) and str(target_item).replace(" ", "") == str(528223884374):
        print index, " get"

# O_O = df.groupby([u"source_item", u"target_item"]).mean().reset_index()
# ret = O_O.values
# ret[:, 2] = np.around(ret[:, 2].astype(np.double), decimals=4)
# ret = np.concatenate((ret, np.asarray([[long(50000671)] * len(ret)]).T), axis=1).tolist()

