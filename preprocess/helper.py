# -*- coding: utf-8 -*-
__author__ = 'Dragon'

import os
import numpy as np
import pandas as pd



def parser_label(a_list, dict_head):
    n = len(a_list)
    label = np.zeros((n, len(dict_head)))
    
    df = pd.read_excel(os.path.dirname(__file__)+'/Combine.xlsx', encoding='utf8')#t,o
    d = []
    for i in df.index:
        t = df['t'][i]
        for j in df['o'][i].split(','):
            d.append((j, t))
       
    for i in xrange(n): 
        t = a_list[i].strip("'")
        for x in d:
            t = t.replace(x[0], x[1])
        for x in set(t.split(',')):
            if x!='':
                label[i][dict_head[x]] = 1
    return label
    

def getcut(miu, head):
    result = []
    for i in miu:
        t = []
        for x in i:
            t.append([])
            for j in xrange(len(head)):
                if head[j].find(x)+1:
                    t[-1].append(j)
            if not t[-1]:
                del t[-1]
        result.append(t)
    return result


def Jaca(u,v):
    t = np.bitwise_or(u != 0, v != 0)
    q = t.sum()
    if q == 0: return 0
    return 1 - float(np.bitwise_and((u != v), t).sum())/ q

'''
def WJacca(x, y, cut, weights):
    result = 0.0
    n0 = len(cut[0])
    n1 = len(cut[1])
    a0 = weights[0]/n0

    for i in xrange(n0):
        t = cut[0][i]
        result += a0 * Jaca(x[t],y[t])
    
    w = [True] * n1
    for i in xrange(n1):
        t = cut[1][i]
        if np.bitwise_and(x[t] != 0, y[t] != 0).sum() != 0:
            w[i] = False
    
    n = w.count(False)
    if n == 0: 
        return result
    else:
        a1 = min(a0, weights[1]/n)
    
        for i in xrange(n1):
            if w[i]: continue
            t = cut[1][i]
            result += a1 * Jaca(x[t],y[t])
         
        return result
'''
'''
def WJacca(u, v, cut, weights):
    result = 0.0
    n0 = len(cut[0])
    n1 = len(cut[1])
    a0 = weights[0]/n0
    
    t = np.bitwise_or(u != 0, v != 0)
    y = np.double(t)
    x = np.double(np.bitwise_and((u != v), t))
    
    for i in xrange(n0):
        t = cut[0][i]
        tt = y[t].sum()
        if tt != 0:
            result += a0* x[t].sum()/tt
    
    r = 0
    c = 0.0
    for i in xrange(n1):
        t = cut[1][i]
        if np.bitwise_and(u[t] != 0, v[t] != 0).sum() != 0:  
            c += 1           
            r += x[t].sum()/y[t].sum()
    
    if c == 0: 
        return result
    return result + weights[1]*r/c
'''
def WJacca(x, y, cut, weights):
    result = 0
    n0 = len(cut[0])
    n1 = len(cut[1])
    a0 = weights[0]/n0

    for i in xrange(n0):
        t = cut[0][i]
        result += Jaca(x[t],y[t])
    result *= a0
    
    c = 0.0
    r = 0
    for i in xrange(n1):
        t = cut[1][i]
        if np.bitwise_and(x[t] != 0, y[t] != 0).sum() != 0:
            c += 1
            r += Jaca(x[t],y[t])
    
    if c == 0:
        return result
    else:
        return result + min(weights[1]/c, a0) * r
