import os
import re
import gc
import time
import queue
import random
import datetime
import threading
import pymysql.cursors
from lxml import etree
from selenium import webdriver
from multiprocessing import Pool
os.chdir("D:/1/毕业论文/180430-KT/数据/06-一带一路")

def log(ma):
    file = open(r'必应-一带一路.txt', mode='a+', encoding='utf-8')
    file.write(ma+'\n')
    file.close()

def foo(u):
    def thread1(n_thread, nameList):
        class myThread(threading.Thread):
            def __init__(self, threadID, name, q):
                threading.Thread.__init__(self)
                self.threadID = threadID
                self.name = name
                self.q = q
            def run(self):
                process_data1(self.name, self.q)

        def process_data1(threadName, q):
            while not exitFlag:
                queueLock.acquire()
                if not workQueue.empty():
                    data = q.get()
                    queueLock.release()
                    foo1(data)
                else:
                    queueLock.release()

        exitFlag = 0
        threadList = ["Thread-" + str(i + 1) for i in range(n_thread)]
        queueLock = threading.Lock()
        workQueue = queue.Queue(10000)
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
        print("退出主线程")

    def foo1(i):
        time.sleep(random.random()*5)
        try:
            urls = root1.xpath('//ol[@id="b_results"]/li[' + str(i) + ']//h2/a/@href')[0]
            title = ''.join(root1.xpath('//ol[@id="b_results"]/li[' + str(i) + ']//h2//text()'))
            abs1=root1.xpath('//ol[@id="b_results"]/li[' + str(i) + ']//p//text()')
            if len(abs1)>1:
                abstract = ''.join(abs1).split('\u2002')[-1]
            else:
                abstract = re.sub('[\u2002·]', '', ''.join(abs1))
            try:
                time1 = ''.join(abs1).split('\u2002')[0]
                times = datetime.datetime.strptime(time1, '%Y-%m-%d').strftime("%Y-%m-%d %H:%M:%S")
                timestamp1 = str(int(time.mktime(time.strptime(times, "%Y-%m-%d %H:%M:%S"))))
            except:
                times='NULL'
                timestamp1='NULL'
                pass
            iid = str(j2*10+i-9 if j2>0 else i)
            connection = pymysql.connect(host='localhost', user='root', password='asd123', db='road',charset='utf8mb4')
            try:
                with connection.cursor() as cursor:
                    sql = " insert into `cn_bing`(`iid`,`urls`,`title`,`times`,`timestamp1`,`abstract`) values (%s,%s,%s,%s,%s,%s)"
                    cursor.execute(sql, (iid,urls,title,times,timestamp1,abstract))
                    connection.commit()
                    print("6-1--写入成功")
            except:
                print("6-2--写入失败")
                pass
            finally:
                connection.close()
        except:
            ma = '4---第 '+s+' 天:j=' + str(j2) + '天: ' + str(m) + '页: 第'+str(i)+'条解析出错: cn 多条检查浏览器 ' + time.asctime(time.localtime(time.time()))
            print(ma)
            log(ma)
            pass

    def thread2(n_thread, nameList):
        class myThread(threading.Thread):
            def __init__(self, threadID, name, q):
                threading.Thread.__init__(self)
                self.threadID = threadID
                self.name = name
                self.q = q
            def run(self):
                process_data2(self.name, self.q)

        def process_data2(threadName, q):
            while not exitFlag:
                queueLock.acquire()
                if not workQueue.empty():
                    data = q.get()
                    queueLock.release()
                    foo2(data)
                else:
                    queueLock.release()

        exitFlag = 0
        threadList = ["Thread-" + str(i + 1) for i in range(n_thread)]
        queueLock = threading.Lock()
        workQueue = queue.Queue(10000)
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
        print("退出主线程")

    def foo2(i):
        time.sleep(random.random()*5)
        try:
            urls = root2.xpath('//ol[@id="b_results"]/li[' + str(i) + ']/h2/a/@href')[0]
            title = ''.join(root2.xpath('//ol[@id="b_results"]/li[' + str(i) + ']/h2//text()'))
            abs1=root2.xpath('//ol[@id="b_results"]/li[' + str(i) + ']//p//text()')
            if len(abs1)>1:
                abstract = re.sub('[\u3000,\xa0·]', '',''.join(abs1[1:]))
            else:
                abstract = re.sub('[\u3000,\xa0·]', '', ''.join(abs1[0]))
            try:
                time1=root2.xpath('//ol[@id="b_results"]/li[' + str(i) + ']//p/span/text()')[0]
                times = datetime.datetime.strptime(time1, '%b %d, %Y').strftime("%Y-%m-%d %H:%M:%S")
                timestamp1 = str(int(time.mktime(time.strptime(times, "%Y-%m-%d %H:%M:%S"))))
            except:
                times='NULL'
                timestamp1='NULL'
                pass
            iid = str(j2*14+i-13 if j2>0 else i)
            connection = pymysql.connect(host='localhost', user='root', password='asd123', db='road',charset='utf8mb4')
            try:
                with connection.cursor() as cursor:
                    sql = " insert into `cn_bing`(`iid`,`urls`,`title`,`times`,`timestamp1`,`abstract`) values (%s,%s,%s,%s,%s,%s)"
                    cursor.execute(sql, (iid,urls,title,times,timestamp1,abstract))
                    connection.commit()
                    print("6-1--写入成功")
            except:
                print("6-2--写入失败")
                pass
            finally:
                connection.close()
        except:
            ma = '5---第 '+s+' 天:j=' + str(j2) + '天: ' + str(m) + '页: 第'+str(i)+'条解析出错: en 多条检查浏览器 '+ time.asctime(time.localtime(time.time()))
            print(ma)
            log(ma)
            pass

    j,w,q,n1,n2,s1=u
    s2=int(int(time.mktime(time.strptime(s1 + " 00:00:00", "%Y-%m-%d %H:%M:%S")))+86400*q*j)
    s=time.strftime("%Y-%m-%d",time.localtime(s2))
    j2=int(int(s2-int(time.mktime(time.strptime("2015-03-28 00:00:00", "%Y-%m-%d %H:%M:%S"))))/86400)
    j1= j2+16522
    r=list(range(1,30))
    random.shuffle(r)
    print('1---正在获取从 '+s+' 天: j= '+str(j2)+' 天数据'+':正常情况 '+time.asctime(time.localtime(time.time())))
    driver = webdriver.Chrome()
    for m in r:
        try:
            url = 'https://www.bing.com/search?q=the+Belt+and+Road&filters=ex1%3a%22ez5_' + str(j1) + '_' + str(
                j1 + q) + '%22&pq=the+Belt+and+Road&qpvt=the+Belt+and+Road&ensearch=0&first=' + str(m*10)
            driver.get(url)
            time.sleep(random.random() * 5)
            html = driver.page_source
            root1 = etree.HTML(html)
            thread1(n1, list(range(1, n1 + 1)))
        except:
            ma = '2---第 '+s+' 天:j=' + str(j2) + '天: ' + str(m) + '页导航出错: cn 刷新浏览器 ' + time.asctime(
                time.localtime(time.time()))
            print(ma)
            log(ma)
            pass
        try:
            url = 'https://cn.bing.com/search?q=the+Belt+and+Road&filters=ex1%3a%22ez5_' + str(j1) + '_' + str(
                j1 + q) + '%22&pq=the+belt+and+road&qpvt=the+Belt+and+Road&ensearch=1&first=' + str(m*14)
            driver.get(url)
            time.sleep(random.random() * 5)
            html = driver.page_source
            root2 = etree.HTML(html)
            thread2(n2, list(range(1, n2 + 1)))
        except:
            ma = '3---第 '+s+' 天:j=' + str(j2) + '天: ' + str(m) + '页导航出错: en 刷新浏览器 ' + time.asctime(
                time.localtime(time.time()))
            print(ma)
            log(ma)
            pass
    driver.close()
def main():
    q1 = int((time.mktime(time.strptime(e+" 00:00:00", "%Y-%m-%d %H:%M:%S"))-time.mktime(time.strptime(s+" 00:00:00", "%Y-%m-%d %H:%M:%S")))/86400/q)
    q2=list(range(q1))
    random.shuffle(q2)
    time.sleep(random.random()*5)
    pool1 = Pool(p)
    pool1.map(foo,list(map(lambda x:(x,w,q,n1,n2,s),q2)))
    pool1.close()
    pool1.join()
    time.sleep(random.random()*5)
    gc.collect()

if __name__ == "__main__":
    g=int(input(' 请输入1: 默认(w-模拟0,p-进程3,q-分期1,n1中线程10,n2英线程14,s-开始2018-03-01,e-结束2018-10-25) \n 或输入0: 自选(0,3,1,10,14,2018-03-01,2018-10-25)\n'))
    if g==1:
        w,p,q,n1,n2,s,e=0,2,3,10,14,'2015-05-28','2018-10-31'
    else:
        l=input(' 自选示例:0,3,1,10,14,2016-06-23,2018-10-31\n').split(',')
        w,p,q,n1,n2=list(map(int,l[:5]))
        s,e=l[5:7]
    main()
