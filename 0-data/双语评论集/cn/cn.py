import os
import re
import jieba
import xml.dom.minidom
import pandas as pd
import queue
import threading
os.chdir('D:/1/毕业论文/180430-KT/数据/00-data/cn')

def foo(host):
    def stopwordslist(filepath):
        stopwords = [line.strip() for line in open(filepath, 'r', encoding='utf-8').readlines()]
        return stopwords
    def re_sentence(sentence):
        if sentence == None:
            sentence = ''
        else:
            sentence = re.sub('[’!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~\s+0-9a-zA-Z、，；。！\～：（）\《》★一\【】]+', '', sentence)
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
    try:
        file = open('Corpus1.txt', mode='a+', encoding='utf-8')
        file.write(seg_sentence(host) + '\n')
        file.close()
    except:
        pass

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
    workQueue = queue.Queue(5000000)
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
    file = open(r'Corpus1.txt', mode='w+', encoding='utf-8')
    file.close()
    d3 = list(pd.read_csv(r'test1.csv')['content'])
    n_thread = 5000
    thread(n_thread, d3)

if __name__=='__main__':
    DOMTree = xml.dom.minidom.parse(r"all.xml")
    collection = DOMTree.documentElement
    movies = collection.getElementsByTagName("RECORD")
    d1,d2,d3,d4=[],[],[],[]
    for i in range(len(movies)):
        print(str(i)+'/'+str(len(movies)))
        d1.append(movies[i].getElementsByTagName('commentid')[0].childNodes[0].data)
        d2.append(movies[i].getElementsByTagName('goodRateShow')[0].childNodes[0].data)
        d3.append(movies[i].getElementsByTagName('content')[0].childNodes[0].data)
        d4.append(movies[i].getElementsByTagName('index1')[0].childNodes[0].data)
    df=pd.DataFrame()
    df['commentid'],df['goodRateShow'],df['content'],df['index1']=d1,d2,d3,d4
    df.to_csv("test1.csv",index=False,sep=',')
    ips2()









