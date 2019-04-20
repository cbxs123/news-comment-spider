import os
import re
import gc
import time
import queue
import random
import requests
import threading
import pymysql.cursors
from lxml import etree
from fake_useragent import UserAgent
from multiprocessing import Pool
from selenium import webdriver
os.chdir("D:/1/毕业论文/180430-KT/数据/07-朝核问题")

def log(ma):
    file = open(r'新浪-朝核问题.txt', mode='a+', encoding='utf-8')
    file.write(ma+'\n')
    file.close()

def headers():
    header = {'User-Agent': UserAgent().random,
              'Connection': 'keep-alive'}
    return header

def ip():
    ips = [line.strip() for line in open(r"ips-1.txt", "r")]
    if len(ips) < 50:
        ips = [line.strip() for line in open(r"ips.txt", "r")]
    return ips

def get_url1(url):
    code,i=503,0
    while code!=200 and i<200:
        i += 1
        host = random.choice(ip())
        proxy = {'http': 'http://' + host, 'https': 'https://' + host}
        try:
            res = requests.get(url=url, headers=headers(),proxies=proxy,timeout=30)
            code = res.status_code
            if code == 200:
                return etree.HTML(res.text)
        except:
            pass
        if i == 200:
            res = requests.get(url=url, headers=headers())
            return etree.HTML(res.text)

def get_url2(url):
    code,i=503,0
    while code!=200 and i<200:
        i += 1
        host = random.choice(ip())
        proxy = {'http': 'http://' + host, 'https': 'https://' + host}
        try:
            res = requests.get(url=url, headers=headers(),proxies=proxy,timeout=20)
            res.encoding = 'utf-8'
            code = res.status_code
            if code == 200:
                return etree.HTML(res.text)
        except:
            pass
        if i == 200:
            res = requests.get(url=url, headers=headers())
            res.encoding = 'utf-8'
            return etree.HTML(res.text)

def texts(urls):
    root1=get_url2(urls)
    sig=0
    try:
        text1=re.sub('[\xa0,\u3000]','',''.join(root1.xpath('//div[@class="article"]/p/text()')))
        if len(text1)>10:
            sig+=1
            return text1
    except:
        pass
    try:
        text2 =re.sub('[\xa0,\u3000]','',''.join(root1.xpath('//div[@class="article"]/div//text()')))
        if len(text2) > 10:
            sig += 1
            return text2
    except:
        pass
    try:
        text3 =re.sub('[\xa0,\u3000]','',''.join(root1.xpath('//div[@class="content"]/p/text()')))
        if len(text3) > 10:
            sig += 1
            return text3
    except:
        pass
    try:
        text4 =re.sub('[\xa0,\u3000]', '', ''.join(root1.xpath('//div[@class="article clearfix"]/p/text()')))
        if len(text4) > 10:
            sig += 1
            return text4
    except:
        pass
    if sig==0:
        return 'NULL'

def foo1(u):
    def thread(n_thread, nameList):
        class myThread(threading.Thread):
            def __init__(self, threadID, name, q):
                threading.Thread.__init__(self)
                self.threadID = threadID
                self.name = name
                self.q = q
            def run(self):
                process_data(self.name, self.q)

        def process_data(threadName, q):
            while not exitFlag:
                queueLock.acquire()
                if not workQueue.empty():
                    data = q.get()
                    queueLock.release()
                    foo(data)
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

    def foo(i):
        time.sleep(random.random()*3)
        try:
            urls = root.xpath('//div[@data-sudaclick="blk_result_' + str(i) + '"]//h2/a/@href')[0]
            title = ''.join(root.xpath('//div[@data-sudaclick="blk_result_' + str(i) + '"]//h2//text()')[:-1])
            website = root.xpath('//div[@data-sudaclick="blk_result_' + str(i) + '"]//h2//text()')[-1].split()[0]
            times = ' '.join(root.xpath('//div[@data-sudaclick="blk_result_' + str(i) + '"]//h2//text()')[-1].split()[1:])
            timestamp1 = str(int(time.mktime(time.strptime(times, "%Y-%m-%d %H:%M:%S"))))
            abstract = ''.join(root.xpath('//div[@data-sudaclick="blk_result_' + str(i) + '"]//p[@class="content"]//text()'))
            fulltext1 = texts(urls)
            iid=str(j*n+i-n+1 if j>0 else i)
            connection = pymysql.connect(host='localhost', user='root', password='asd123', db='nuclear',charset='utf8mb4')
            try:
                with connection.cursor() as cursor:
                    sql = " insert into `cn_sina`(`iid`,`urls`,`title`,`website`,`times`,`timestamp1`,`abstract`,`fulltext1`) values (%s,%s,%s,%s,%s,%s,%s,%s)"
                    cursor.execute(sql, (iid,urls,title,website,times,timestamp1,abstract,fulltext1))
                    connection.commit()
                    print("6-1--写入成功")
            except:
                print("6-2--写入失败")
                pass
            finally:
                connection.close()
        except:
            ma = '3---第'+str(a)+':'+str(b)+'天:j='+str(j)+'m页='+str(m)+'i条'+str(i)+'具体解析:检查浏览器 '+time.asctime(time.localtime(time.time()))
            print(ma)
            log(ma)
            pass

    def get1():
        url1 = 'http://search.sina.com.cn/?c=news&q=%B3%AF%BA%CB%CE%CA%CC%E2&range=all&time=custom&stime=' + str(
            a) + '&etime=' + str(b) + '&num=20&page=' + str(random.randint(2, 9))
        root1 = get_url1(url1)
        time.sleep(random.random()*5)
        try:
            t=int(int(re.sub('[找到相关新闻,篇]', '', root1.xpath('//div[@class="l_v2"]/text()')[0])) / 20)
        except:
            ma = '0---' + str(a) + ':' + str(b) + '天 :链接被和谐：将关闭浏览器  ' + time.asctime(time.localtime(time.time()))
            print(ma)
            log(ma)
            t=-1
            pass
        return t

    def get2():
        c = 0
        while c < 10:
            t1=get1()
            time.sleep(random.random() * 30)
            t2=get1()
            if t1==t2:
                c=11
                return t1
            else:
                c+=1
            if c == 10:
                return -1
    j,w,q,n,s=u
    f = time.mktime(time.strptime(s+" 00:00:00", "%Y-%m-%d %H:%M:%S"))
    a = time.strftime("%Y-%m-%d", time.localtime(f + 86400 * j * random.randint(q, q+2)))
    b = time.strftime("%Y-%m-%d", time.localtime(f + 86400 * (j + 1) * random.randint(q+2, q+4)))
    r = list(range(1, get2()))
    random.shuffle(r)
    print('1---正在获取第'+str(a)+':'+str(b)+'天:j='+str(j)+':正常情况 '+time.asctime(time.localtime(time.time())))

    if w==1:
        for m in r:
            try:
                url = 'http://search.sina.com.cn/?c=news&q=%B3%AF%BA%CB%CE%CA%CC%E2&range=all&time=custom&stime=' + str(
                    a) + '&etime=' + str(b) + '&num=20&page=' + str(m)
                root = get_url1(url)
                time.sleep(random.random() * 3)
                thread(n, list(range(n)))
                url = 'http://search.sina.com.cn/?c=news&q=%B3%AF%BA%CB%CE%CA%CC%E2&range=all&time=custom&stime=' + str(
                    a) + '&etime=' + str(b) + '&num=20&sort=rel&page=' + str(m)
                root = get_url1(url)
                time.sleep(random.random() * 3)
                thread(n, list(range(n)))
            except:
                ma = '2---第'+str(a)+':'+str(b)+'天:j='+str(j)+'m页='+str(m)+'导航出错: 检查浏览器 '+time.asctime(time.localtime(time.time()))
                print(ma)
                log(ma)
                pass
    else:
        driver = webdriver.Chrome()
        for m in r:
            try:
                url = 'http://search.sina.com.cn/?c=news&q=%B3%AF%BA%CB%CE%CA%CC%E2&range=all&time=custom&stime=' + str(
                    a) + '&etime=' + str(b) + '&num=20&page=' + str(m)
                driver.get(url)
                time.sleep(random.random() * 5)
                html = driver.page_source
                root = etree.HTML(html)
                thread(n, list(range(n)))
                url = 'http://search.sina.com.cn/?c=news&q=%B3%AF%BA%CB%CE%CA%CC%E2&range=all&time=custom&stime=' + str(
                    a) + '&etime=' + str(b) + '&num=20&sort=rel&page=' + str(m)
                driver.get(url)
                time.sleep(random.random() * 5)
                html = driver.page_source
                root = etree.HTML(html)
                time.sleep(random.random() * 5)
                thread(n, list(range(n)))
            except:
                ma = '2---第'+str(a)+':'+str(b)+'天:j='+str(j)+'m页='+str(m)+'导航出错: 检查浏览器 '+time.asctime(time.localtime(time.time()))
                print(ma)
                log(ma)
                pass
        driver.close()

def main():
    q1 = int((time.mktime(time.strptime(e+" 00:00:00", "%Y-%m-%d %H:%M:%S"))-time.mktime(time.strptime(s+" 00:00:00", "%Y-%m-%d %H:%M:%S")))/86400/q)
    time.sleep(random.random()*5)
    pool1 = Pool(p)
    pool1.map(foo1,list(map(lambda x:(x,w,q,n,s),list(range(q1)))))
    pool1.close()
    pool1.join()
    time.sleep(random.random()*5)
    gc.collect()

if __name__ == "__main__":
    g=int(input(' 请输入1: 默认(w-模拟0,p-进程30,q-分期7,n线程20,s-开始2018-03-01,e-结束2018-10-25) \n 或输入0: 自选(1,3,7,20,2018-03-01,2018-10-25)\n'))
    if g==1:
        w,p,q,n,s,e=1,20,90,20,'2003-03-01','2018-10-31'
    else:
        l=input(' 自选示例:1,3,7,20,2018-03-01,2018-10-25\n').split(',')
        w,p,q,n=list(map(int,l[:4]))
        s,e=l[4:6]
    main()
