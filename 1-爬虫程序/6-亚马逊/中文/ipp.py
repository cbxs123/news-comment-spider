import os
import time
import queue
import random
import requests
import threading
from lxml import etree
from fake_useragent import UserAgent
os.chdir("D:/1/毕业论文/180430-KT/数据/02-亚马逊/中文")

def foo(host):
    proxy = {'http': 'http://' + host, 'https': 'https://' + host}
    test_url = "https://www.amazon.cn"
    try:
        res = requests.get(test_url, headers=headers(), proxies=proxy, timeout=30)
        code = res.status_code
        if code == 200:
            print("OK---" + host)
            file = open('ips-1.txt', mode='a+', encoding='utf-8')
            file.write(host + '\n')
            file.close()
    except:
        print("NO***" + host)
        pass

def ips1():
    ips = [line.strip() for line in open(r"ips.txt", "r")]
    try:
        ip_url = "http://www.thebigproxylist.com/"
        root = get_url1(ip_url,ips)
        nameList = root.xpath('//div[@id="site-wrapper"]//script/text()')[0].split('hostPort": ')[1:]
        nameList =[nameList[i].split(',')[0][1:-1] for i in range(len(nameList))]
        ma='0----0----连接成功---'+time.asctime(time.localtime(time.time()))
        print(ma)
    except:
        nameList = ips
        ma='0----1-----自定义连接---'+time.asctime(time.localtime(time.time()))
        print(ma)
        pass
    n_thread = 500
    thread(n_thread, nameList)


def headers():
    header = {'User-Agent': UserAgent().random,
              'Connection': 'keep-alive'}
    return header

def get_url1(url,ips):
    code,i=503,0
    while code!=200 and i<200:
        i += 1
        host = random.choice(ips)
        proxy = {'http': 'http://' + host, 'https': 'https://' + host}
        try:
            res = requests.get(url=url, headers=headers(),proxies=proxy)
            code = res.status_code
            if code == 200:
                return etree.HTML(res.text)
        except:
            pass
        if i == 200:
            res = requests.get(url=url, headers=headers())
            return etree.HTML(res.text)

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
    workQueue = queue.Queue(3000)
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

if __name__ == "__main__":
    for i in range(1000):
        try:
            file = open(r'ips-1.txt', mode='w+', encoding='utf-8')
            file.close()
			print("第"+str(i)+"次")
            ips1()
        except:
            pass
        finally:
            rant=random.randint(600,650)
            for i in range(rant):
                time.sleep(1)
                print("刷新倒计时"+str(rant-i)+'s')

