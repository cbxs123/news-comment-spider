# -*- coding: utf-8 -*-
import json 
import re
import jieba
import numpy as np
import pandas as pd
from gensim.models import word2vec  
import os
import queue
import threading
os.chdir('/home/ys/桌面/数据/cn')



def foo(i):
    def stopwordslist(filepath):  
        stopwords = [line.strip() for line in open(filepath, 'r', encoding='utf-8').readlines()]  
        return stopwords 

    def re_sentence(sentence):
        if sentence==None:
            sentence=''
        else:
            sentence=re.sub('[’!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~\s+0-9a-zA-Z、，；。！\～：（）\《》★一\【】]+','',sentence)
        return sentence

    def seg_sentence(sentence):
        sentence_seged = jieba.cut(re_sentence(sentence))
        outstr = '' 
        stopwords = stopwordslist('stopwords.txt')  
        for word in sentence_seged:  
            if word not in stopwords:  
                if word != '\t':  
                    outstr += word  
                    outstr += " "  
        return outstr 
    data1[i]=str(seg_sentence(data['content'][i]))
    print(str(i))
    return data1[i]
    
    


def thread(n_thread,nameList):
    class myThread(threading.Thread):
        def __init__(self, threadID, name, q):
            threading.Thread.__init__(self)
            self.threadID = threadID
            self.name = name
            self.q = q
        def run(self):
            print("开启线程：" + self.name)
            process_data(self.name, self.q)
            print("退出线程：" + self.name)
    def process_data(threadName, q):
        while not exitFlag:
            queueLock.acquire()
            if not workQueue.empty():
                data = q.get()
                queueLock.release()
                print("%s processing %s" % (threadName, data))
                foo(data)
            else:
                queueLock.release()
    exitFlag = 0
    threadList = ["Thread-" + str(i + 1) for i in range(n_thread)]
    queueLock = threading.Lock()
    workQueue = queue.Queue(1000000)
    threads = []
    threadID = 1
    for tName in threadList:
        thread = myThread(threadID, tName, workQueue)
        thread.start()
        threads.append(thread)
        threadID += 1
    queueLock.acquire()
    for word in nameList:
        workQueue.put(word)
    queueLock.release()
    while not workQueue.empty():
        pass
    exitFlag = 1
    for t in threads:
        t.join()
    print ("退出主线程")

def ips2():
    d3 = list(range(len(data)))
    n_thread = 1500
    thread(n_thread, d3)



data=pd.DataFrame(json.load(open(r"ymxcn.json","r"))['RECORDS'])
data1=pd.Series(map(str,list(range(len(data)))))
###    threading
#ips2()

###    map
#list(map(lambda x:foo(x),list(range(len(data)))))

###    ThreadPool
from multiprocessing import Pool
pool = Pool(16)
data['content']=pool.map(foo,list(range(len(data))))
pool.close()
pool.join()

data.to_csv("test18all.csv",index=False,sep=',')

#file = open('Corpus18all.txt',mode='w+',encoding='utf-8')
#file.close()
# model=word2vec.Word2Vec(word2vec.Text8Corpus(u"Corpus18all.txt"),size=300)
# model.wv.save_word2vec_format(u"model18all300.bin", binary=True)
        
    
