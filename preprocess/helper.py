# -*- coding: utf-8 -*-
__author__ = 'Dragon'

import numpy as np


def parser_label(a_list, dict_head):
    n = len(a_list)
    label = np.zeros((n, len(dict_head)))
    for i in xrange(n):
        for x in a_list[i].split(','):
            if x!='':
                label[i][dict_head[x]] = 1
    return label


# import pandas as pd
# def parser_label(a_list, head):
#     n = len(a_list)
#     df_label = pd.DataFrame(np.zeros((n, len(head))))
#     df_label.columns = head
#     for i in xrange(n):
#         for x in a_list[i].split(','):
#             if x!='':
#                 df_label[x][i] = 1
#     return df_label

