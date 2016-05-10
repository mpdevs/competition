# -*- coding: utf-8 -*-

def getcut(miu, head):
    result = []
    for i in miu:
        t = []
        for x in i:
            t.append([])
            for j in xrange(len(head)):
                if head[j].find(x)+1:
                    t[-1].append(j)
            if len(t[-1]) == 0:
                del t[-1]
        result.append(t)

    return result


def Jaca(x,y):
    a = x.dot(y)
    b = sum(x+y) - a
    if a==0:
        return 0
    else:
        return float(a)/b


def WJacca(x, y, cut, weights):
    result = 0.0
    n0 = len(cut[0])
    n1 = len(cut[1])
    a0 = weights[0]/n0

    for i in xrange(n0):
        t = cut[0][i]
        result += a0 * Jaca(x[t],y[t])
    
    w = [False] * n1
    for i in xrange(n1):
        t = cut[1][i] 
        if sum(x[t]) == 0 or sum(y[t]) == 0:
            w[i] = True

    n = w.count(1)   
    if n == 0: 
        return result
    else:
        a1 = min(a0, weights[1]/n)
    
    for i in xrange(n1):
        if w[i]: continue
        t = cut[1][i]
        result += a1 * Jaca(x[t],y[t])
      
    return result

