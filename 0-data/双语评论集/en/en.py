import os
import xml.dom.minidom
import pandas as pd
os.chdir('D:/1/毕业论文/180430-KT/数据/00-data/en')

if __name__=='__main__':
    DOMTree = xml.dom.minidom.parse(r"all.xml")
    collection = DOMTree.documentElement
    movies = collection.getElementsByTagName("RECORD")
    d1,d2,d3,d4=[],[],[],[]
    for i in range(len(movies)):
        print(str(i)+'/'+str(len(movies)))
        try:
            d1.append(movies[i].getElementsByTagName('summary')[0].childNodes[0].data.strip().replace('\n',''))
            d2.append(movies[i].getElementsByTagName('rating')[0].childNodes[0].data)
            d3.append(movies[i].getElementsByTagName('text')[0].childNodes[0].data.strip().replace('\n',''))
            d4.append(movies[i].getElementsByTagName('index1')[0].childNodes[0].data)
        except:
            print(str(i)+'行出错----------------------')
    df=pd.DataFrame()
    df['commentid'],df['goodRateShow'],df['content'],df['index1']=d1,d2,d3,d4
    df.to_csv("test2.csv",index=False,sep=',')


import numpy as np
ss=np.zeros((3,5),dtype=int)
ss[1,2]-=1
