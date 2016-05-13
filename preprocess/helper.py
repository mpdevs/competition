# -*- coding: utf-8 -*-
__author__ = 'Dragon'

import os
import numpy as np
import pandas as pd



def parser_label(a_list, dict_head):
    n = len(a_list)
    label = np.zeros((n, len(dict_head)))
    
    df = pd.read_excel(os.path.dirname(__file__)+'/Combine.xlsx')#t,o
    d = []
    for i in df.index:
        t = df['t'][i]
        for j in df['o'][i].split(','):
            d.append((j, t))
       
    for i in xrange(n): 
        t = a_list[i].strip("'")
        for x in d:
            t = t.replace(x[0],x[1])
        for x in set(t.split(',')):
            if x!='':
                label[i][dict_head[x]] = 1
    return label


