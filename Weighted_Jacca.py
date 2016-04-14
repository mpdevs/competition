import numpy as np


def getcut(important, unimportant, head):
    cut1 = []
    print 'Can not find:'
    for x in important:
        cut1.append([])
        for i in range(len(head)):
            if head[i].find(x)+1:
                cut1[-1].append(i)
        if len(cut1[-1]) == 0:
            del cut1[-1]
            print x
    cut2 = []
    for x in unimportant:
        cut2.append([])
        for i in range(len(head)):
            if head[i].find(x)+1:
                cut2[-1].append(i)
        if len(cut2[-1]) == 0:
            del cut2[-1]
            print x
               
    return [cut1] + [cut2]

    
def Jaca(x,y):
    a = x.dot(y)
    b = sum(x+y) - a
    if a==0:
        return 0
    else:
        return float(a)/b
       

def WJacca(x, y, cut, weights):
    assert len(x) == len(y)
    assert len(weights)==len(cut)
    
    w = []
    temp = True
    for i in range(len(cut)):
        w.append([])
        for j in range(len(cut[i])):
            if sum(x[cut[i][j]]) == 0 or sum(y[cut[i][j]]) == 0:
                w[i].append(0)
            else:
                w[i].append(1)
                temp = False
    if temp:
        return 0
    temp = 0.0
    t = []
    for i in range(len(w)):
        t.append(w[i].count(1))
    for i in xrange(len(w)):
        for j in xrange(len(w[i])):
            if w[i][j] != 0:
                temp += weights[i]/t[i] * Jaca(x[cut[i][j]],y[cut[i][j]])
    return temp


'''
x=np.array([1,0,1])
y=np.array([1,1,1])
weights = [0.6,0.4]
important = ['1']
unimportant = ['2']
head = ['1','2','23']
cut = getcut(important, unimportant, head)
print WJacca(x,y,cut,weights)
'''  
