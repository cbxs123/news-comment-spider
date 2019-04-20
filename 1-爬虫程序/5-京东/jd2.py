import os
import re
import time
import queue
import random
import requests
import threading
import pymysql.cursors
from lxml import etree
from multiprocessing import Pool
from fake_useragent import UserAgent
os.chdir("D:/1/毕业论文/180430-KT/数据/09-jd")

def log(ma):
    file = open(r'京东-智能家电.txt', mode='a+', encoding='utf-8')
    print(ma)
    file.write(ma+'\n')
    file.close()

def headers():
    header = {'User-Agent': UserAgent(use_cache_server=False).random,
              'Connection': 'keep-alive'}
    return header

def ip():
    ips = [line.strip() for line in open(r"ips-1.txt", "r")]
    if len(ips) < 50:
        ips = [line.strip() for line in open(r"ips.txt", "r")]
    return ips

def get_url1(url,sig=0):
    code,i=503,0
    while code!=200 and i<200:
        i += 1
        while True:
            try:
                host = random.choice(ip())
                break
            except:
                pass
                continue
        proxy = {'http': 'http://' + host, 'https': 'https://' + host}
        cookie=[line.strip() for line in open(r"cookies.txt", "r")][0]
        cookies2 = dict(map(lambda x: x.split('='), cookie.split(";")))
        try:
            if sig==1:
                res = requests.get(url=url, headers=headers(),cookies=cookies2,proxies=proxy,timeout=30)
                sl=len(etree.HTML(res.text).xpath('//li[@class="gl-item"]/@data-sku'))
                if res.status_code == 200 and sl==30:
                    return res.text
                else:
                    print('1-0-cookie过期-%d'%i)
                    time.sleep(1)
            else:
                res = requests.get(url=url, headers=headers(),proxies=proxy,timeout=30)
                if res.status_code == 200:
                    return res.text
        except:
            print('1-1-第%d次尝试' % i)
            pass
        if i == 200:
            return requests.get(url=url, headers=headers()).text

def foo1(o):
    def thread(n_thread, nameList):
        class myThread(threading.Thread):
            def __init__(self, threadID, name, q):
                threading.Thread.__init__(self)
                self.threadID = threadID
                self.name = name
                self.q = q
            def run(self):
                process_data(self.q)

        def process_data(q):
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
        try:
            print("3---开始获取商品%s:第%d页评论"%(sku,i))
            percent_url1 = "https://sclub.jd.com/comment/productPageComments.action?callback=fetchJSON_comment98vv"
            percent_url2 = "&productId="+sku+"&score=0&sortType=5&page="+str(i)+"&pageSize=10&isShadowSku=0&fold=1"
            time.sleep(random.random())
            html=get_url1(percent_url1+str(vvid)+percent_url2)

            label=re.findall(r'"name":(.*?),"count":(.*?),"canBeFiltered"', html)
            lable=''.join(list(map(lambda x:x[0].split(',')[0][1:-1]+':'+x[1].split(',')[0]+',',label)))
            all_percent = re.findall(r'"goodRateShow":(.*?),', html)[0] + '%'
            sum_comment = re.findall(r'"commentCount":(.*?),', html)[0]
            sum_tu = re.findall(r'"imageListCount":(.*?),', html)[0]
            sum_zhui = re.findall(r'"afterCount":(.*?),', html)[0]
            sum_good = re.findall(r'"goodCount":(.*?),', html)[0]
            sum_mid = re.findall(r'"generalCount":(.*?),', html)[0]
            sum_bad = re.findall(r'"poorCount":(.*?),', html)[0]
            coms = re.findall(r'"topped":(.*?),"userImageUrl":(.*?),', html)

            for com in coms:
                try:
                    # 商品名称、商家名称、价格、整体好评度、总评论数、晒图、追评、好评、中评、差评、评论者ID、评论、星级、赞、回复数等
                    pro_id = sku
                    pro_url = url1
                    pro_price = price
                    pro_name= name1

                    goodRateShow = all_percent
                    commentCount = sum_comment
                    imageListCount = sum_tu
                    afterCount = sum_zhui
                    goodCount = sum_good
                    generalCount = sum_mid
                    poorCount = sum_bad

                    comment_id = re.findall(r'"guid":(.*?),', com[0])[0]
                    guid = re.findall(r'"guid":(.*?),', com[0])[0]
                    content = re.findall(r'"content":(.*?),', com[0])[0]
                    creationTime = re.findall(r'"creationTime":(.*?),', com[0])[0][1:-1]
                    replyCount = re.findall(r'"replyCount":(.*?),', com[0])[0]
                    score = re.findall(r'"score":(.*?),', com[0])[0]
                    usefulVoteCount = re.findall(r'"usefulVoteCount":(.*?),', com[0])[0]
                    connection = pymysql.connect(host='localhost', user='root', password='asd123',
                                                                 db='jd2', charset='utf8mb4')
                    try:
                        with connection.cursor() as cursor:
                            sql = " insert into `jd_jiadian`(`lei`,`proid`,`proname`,`lable`,`prourl`,`proprice`,`goodRateShow`,`commentCount`," \
                                "`imageListCount`,`afterCount`,`goodCount`,`generalCount`,`poorCount`,`commentid`,`guid`,`content`,`creationTime`," \
                                "`replyCount`,`score`,`usefulVoteCount`) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                            cursor.execute(sql, (u, pro_id, pro_name,lable,pro_url, pro_price, goodRateShow, commentCount, imageListCount,afterCount, goodCount,
                                        generalCount, poorCount, comment_id, guid, content,creationTime, replyCount,score, usefulVoteCount))
                            connection.commit()
                        print("4-1--写入成功")
                    except:
                        print("4-2--写入失败")
                    finally:
                        connection.close()
                except:
                    log("6---出错-商品%s-%d页-具体评论出错"%(sku,i))
                    pass
        except:
            log("9---出错-商品%s-%d页-解析错误" % (sku,i))
            pass

    u,sku=o.split(':')
    try:
        url1 = "https://item.jd.com/" + sku + '.html'
        time.sleep(random.random())
        root3 = etree.HTML(get_url1(url1))
        name1 = root3.xpath('//head/title/text()')[0][:-16]
        ss = root3.xpath('//head/script/text()')[0].split()
        vvid = re.match(r'(.*?)commentVersion:\'(.*?)\'(.*?)', ''.join(ss)).group(2)
        priceurl = "https://p.3.cn/prices/mgets?&pduid="+str(random.randint(1,1000))+"&skuids=J_" + sku
        time.sleep(random.random())
        price = re.compile('"p":"(.*?)"').findall(get_url1(priceurl))[0]
        thread(20,list(range(20)))
    except:
        log('2---第%s类-商品%s-出错-%s'%(u,sku,time.asctime(time.localtime(time.time()))))
        pass

def getsku1(i):
    urls = ['智能电视机',
            '%E6%99%BA%E8%83%BD%E7%A9%BA%E8%B0%83',
            '%E6%99%BA%E8%83%BD%E6%B4%97%E8%A1%A3%E6%9C%BA',
            '%E6%99%BA%E8%83%BD%E6%B4%97%E7%A2%97%E6%9C%BA',
            '%E6%99%BA%E8%83%BD%E5%86%B0%E7%AE%B1',
            '%E6%99%BA%E8%83%BD%E9%9F%B3%E7%AE%B1',
            '%E6%99%BA%E8%83%BD%E6%8A%95%E5%BD%B1%E4%BB%AA',
            '%E6%99%BA%E8%83%BD%E9%97%A8%E9%94%81',
            '%E6%99%BA%E8%83%BD%E7%9B%91%E6%8E%A7%E5%99%A8']
    s, d = i
    print(str(s))
    print(str(d))
    r = list(range(s))
    random.shuffle(r)
    for m in r:
        try:
            url = 'https://search.jd.com/Search?keyword=' + urls[d] + '&enc=utf-8&qrst=1&rt=1&stop=1&vt=2&psort=4&stock=1&page=' + str(2 * m + 1)
            root=etree.HTML(get_url1(url,sig=1))
            sku = root.xpath('//li[@class="gl-item"]/@data-sku')
            f = open('sku.txt', 'a+')
            if len(sku) == 30:
                log("1-0--正在获取-%d类-%d页-商品sku" % (d, m))
                list(map(lambda x: f.write(x + '\n'), list(map(lambda y: str(d) + ':' + y, sku))))
            else:
                log("1-1--出错-%d类-%d页-商品不为30" % (d, m))
            f.close()
        except:
            log("1-2--出错-%d类-%d页-商品解析错误" % (d, m))
            pass

def main1():
    open('sku.txt','w+',encoding='utf-8').close()
    pool1 = Pool(p)
    pool1.map(getsku1,list(map(lambda x:(s,x),list(range(p)))))
    pool1.close()
    pool1.join()

def main2():
    sku1=[line.strip() for line in open('sku.txt','r+',encoding='utf-8')]
    random.shuffle(sku1)
    pool2 = Pool(p*3)
    pool2.map(foo1,sku1)
    pool2.close()
    pool2.join()

if __name__ == "__main__":
    g=int(input(' 请输入1: 默认(p-进程9,s-每类页数(*30)10 \n 或输入0: 自选(9,10)\n'))
    if g==1:
        p,s=9,10
    else:
        l=input(' 自选示例:9,20\n').split(',')
        p,s=list(map(int,l))
    main1()
    main2()
	
