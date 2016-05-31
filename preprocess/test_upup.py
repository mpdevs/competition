from scipy.spatial.distance import cdist, jaccard
import numpy as np
from helper import Jaca
from datetime import datetime

def nJaca(u,v):
    t = np.bitwise_or(u != 0, v != 0)
    q = t.sum()
    if q == 0: return 0
    return 1 - float(np.bitwise_and((u != v), t).sum())/ q


u = np.random.randint(2,size=5)
v = np.random.randint(2,size=5)
print np.bitwise_and(u != 0, v != 0).sum()

print sum(u) == sum(v) > 0 and (u-v).any() != 0
print (u-v).sum() == 0 and u.sum()

print nJaca(u,v)
print Jaca(u,v)
t=datetime.now()

for i in xrange(100000):
    sum(u) == sum(v) > 0 and (u-v).any() != 0
    #nJaca(u,v)
print datetime.now()-t
t=datetime.now()
for i in xrange(100000):    
    (u-v).sum() ==0 and u.sum()
    #Jaca(u,v)
print datetime.now()-t

