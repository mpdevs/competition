import pymongo
import numpy as np
import pandas as pd
from datetime import datetime

mongo_db = 'mongodb://dev:marcpoint@192.168.1.120:27017/'

def parser(x):
    d = []
    p = []
    s = []
    for i, j in x.iteritems():
        if i == u'_id':
            index = j
        else:
            d.append(int(i))
            p.append(int(j[u'p']))     
            s.append(int(j[u'ds']))   
    
    return (index, d, np.asarray(p), np.asarray(s))
        
client = pymongo.MongoClient(mongo_db)
db = client['item_daily_sales']
cursor = db.item_last_60.find({})
print datetime.now()
n = cursor.count()
data = []
for i in xrange(n):
    t = parser(cursor.next())      
    if len(set(t[2])) > 1 and (t[3] >= 0).all() and np.average(t[3])>=10:
        data.append(t)         
print datetime.now(), len(data)

clean = []
for x in data:
    flag = True
    t = sorted(zip(x[1], x[2], x[3]))
    for i in xrange(1, len(t)):
        if (t[i][1] - t[i-1][1]) * (t[i][2] - t[i-1][2]) < 0:
            flag = False
            break
    
    if flag:
        clean.append((x[0], t))

print len(clean)

#df = pd.DataFrame(data)
#df.to_excel('data.xlsx',  index=False)
