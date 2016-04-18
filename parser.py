import pandas as pd 
import numpy as np 


def parser_label(a_list, head):
    n = len(a_list)
    df_label = pd.DataFrame(np.zeros((n, len(head))))
    df_label.columns = head
    for i in xrange(n): 
        for x in a_list[i].split(','):
            if x!='':
                df_label[x][i] = 1
    return df_label
    
