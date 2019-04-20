import os
import re
import time
import queue
import random
import requests
import threading
import pymysql.cursors
from lxml import etree
from selenium import webdriver
from fake_useragent import UserAgent
os.chdir("D:/1/毕业论文/180430-KT/数据/02-亚马逊/英文")

def log(ma):
    file = open(r'18_钟表.txt', mode='a+', encoding='utf-8')
    file.write(ma+'\n')
    file.close()

def headers():
    header = {'User-Agent': UserAgent().random,
              'Connection': 'keep-alive'}
    return header

def get_url1(url,ips):
    code,i,s=503,0,'0'
    while s!='0' and i<201:
        i += 1
        host = random.choice(ips)
        proxy = {'http': 'http://' + host, 'https': 'https://' + host}
        try:
            res = requests.get(url=url, headers=headers(),proxies=proxy,timeout=30)
            code = res.status_code
            s=etree.HTML(res.text).xpath('//div[@class="a-row"]/span[@data-hook="total-review-count"]/text()')[0]
            if code == 200 and s!='0':
                return etree.HTML(res.text)
        except:
            pass
        if i == 200:
            res = requests.get(url=url, headers=headers())
            return etree.HTML(res.text)

def get_url2(url):
    count,i=24,0
    while count<25 and i<200:
        i+=1
        browser = webdriver.Chrome()
        browser.get(url)
        try:
            time.sleep(random.randint(5,8))
            html = browser.page_source
            root=etree.HTML(html)
            sku=root.xpath('//div[@id="atfResults"]//@data-asin')
            if len(sku)>24:
                return sku
            browser.quit()
        except:
            pass
        finally:
            browser.quit()
        if i==200:
            return None

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

def foo(skuj):
    try:
        url3 = "https://www.amazon.com/dp/" + str(skuj) + '/ref=sr_1_181_sspa/&ie=UTF8'
        root = get_url1(url3,ips)
        sum_comment = root.xpath('//div[@class="a-row"]/span[@data-hook="total-review-count"]/text()')[0]
        all_percent = root.xpath('//div[@class="a-row a-spacing-small"]//span[@data-hook="rating-out-of-text"]/text()')[0][:3]
        sum_good = str(int(int(sum_comment) * float(all_percent) / 5))
        sum_bad = str(int(sum_comment) - int(sum_good))
        comment_id1 = root.xpath('//div[@id="cm-cr-dp-review-list"]/div/@id')
        score1 = root.xpath('//div[@id="cm-cr-dp-review-list"]//i[@data-hook="review-star-rating"]/span/text()')
        title1 = root.xpath('//div[@id="cm-cr-dp-review-list"]//a[@data-hook="review-title"]/text()')
        guid1 = root.xpath('//div[@id="cm-cr-dp-review-list"]//span[@class="a-profile-name"]/text()')
        creationTime1 = root.xpath('//div[@id="cm-cr-dp-review-list"]//span[@data-hook="review-date"]/text()')
        content1 = root.xpath('//div[@id="cm-cr-dp-review-list"]//div[@data-hook="review-collapsed"]/text()')
        useful1 = ''.join(root.xpath('//div[@id="cm-cr-dp-review-list"]//text()')).split('报告滥用情况')
        print(" 2-1--正在获取{}商品!".format(skuj))
        for m in range(len(comment_id1)):
            try:
                pro_id = skuj
                pro_url = url3
                goodRateShow = all_percent
                commentCount = sum_comment
                goodCount = sum_good
                poorCount = sum_bad
                comment_id = comment_id1[m]
                title = title1[m]
                guid = guid1[m]
                content = content1[m]
                creationTime = creationTime1[m]
                score = score1[m][:3]
                usefulCount = re.findall(r'(.*?)个人发现此评论有用.*', useful1[m])
                if usefulCount:usefulVoteCount = usefulCount[0].strip()
                else:usefulVoteCount = '0'
                print(content)
                connection = pymysql.connect(host='localhost', user='root', password='asd123', db='amazoncn',charset='utf8mb4')
                try:
                    with connection.cursor() as cursor:
                        sql = " insert into `amazon_cn_18zhongbiao`(`proid`,`prourl`,`goodRateShow`,`commentCount`," \
                              "`goodCount`,`poorCount`,`commentid`,`guid`,`content`,`reviewtitle`,`creationTime`," \
                              "`score`,`usefulVoteCount`) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                        cursor.execute(sql, (pro_id, pro_url, goodRateShow, commentCount, goodCount, poorCount, comment_id, guid,
                                             content, title, creationTime, score, usefulVoteCount))
                        connection.commit()
                        print("6-1--写入成功")
                except:
                    print("6-1--写入失败")
                    pass
                finally:
                    connection.close()
            except:
                ma = '007-1-具体评论-出错:' + str(skuj) + '商品---' + time.asctime(time.localtime(time.time()))
                print(ma)
                log(ma)
                pass
            continue
        if int(sum_comment)>10:
            try:
                url1 = "https://www.amazon.cn/product-reviews/" + str(skuj) + '/ie=UTF8&reviewerType=all_reviews'
                root = etree.HTML(get_url1(url1, ips).text)
                time.sleep(random.random()*8)
                sum_comment = root.xpath('//div[@class="a-row"]/span[@data-hook="total-review-count"]/text()')[0]
                all_percent = root.xpath('//div[@class="a-row averageStarRatingNumerical"]//span[@data-hook="rating-out-of-text"]/text()')[0][:3]
                sum_good = root.xpath('//div[@class="a-row"]/span[@class="a-declarative"]/a/text()')[0].split()[1]
                sum_bad = root.xpath('//div[@class="a-row"]/span[@class="a-declarative"]/a/text()')[1].split()[1]
                print(" 2-2--正在获取{}商品!".format(skuj))
                for i in range(1,int(int(sum_comment) / 10)+1):
                    try:
                        i=i+1
                        percent_url1 = "https://www.amazon.cn/product-reviews/"
                        percent_url2 = "/ie=UTF8&reviewerType=all_reviews/ref=cm_cr_arp_d_paging_btm_"
                        percent_url3 = "?pageNumber="
                        url2 = percent_url1 + str(skuj) + percent_url2 + str(i) + percent_url3 + str(i)
                        root = etree.HTML(get_url1(url2, ips).text)
                        time.sleep(random.random() * 10)
                        try:
                            comment_id1 = root.xpath('//div[@id="cm_cr-review_list"]/div/@id')
                            score1 = root.xpath('//div[@id="cm_cr-review_list"]//i[@data-hook="review-star-rating"]/span/text()')
                            title1 = root.xpath('//div[@id="cm_cr-review_list"]//a[@data-hook="review-title"]/text()')
                            guid1 = root.xpath('//div[@id="cm_cr-review_list"]//a[@data-hook="review-author"]/text()')
                            creationTime1 = root.xpath('//div[@id="cm_cr-review_list"]//span[@data-hook="review-date"]/text()')
                            content1 = root.xpath('//div[@id="cm_cr-review_list"]//span[@data-hook="review-body"]/text()')
                            useful1 = ''.join(root.xpath('//div[@id="cm_cr-review_list"]//text()')).split('留言者')
                            for m in range(len(comment_id1)):
                                pro_id = skuj
                                pro_url = url2
                                goodRateShow = all_percent
                                commentCount = sum_comment
                                goodCount = sum_good
                                poorCount = sum_bad
                                comment_id = comment_id1[m]
                                title = title1[m]
                                guid = guid1[m]
                                content = content1[m]
                                creationTime = creationTime1[m][2:]
                                score = score1[m][:3]
                                usefulCount = re.findall(r'(.*?)个人发现此评论有用.*', useful1[m + 1])
                                if usefulCount:usefulVoteCount = usefulCount[0].strip()
                                else:usefulVoteCount = '0'
                                print(content)
                                connection = pymysql.connect(host='localhost', user='root', password='asd123',db='amazoncn',charset='utf8mb4')
                                try:
                                    with connection.cursor() as cursor:
                                        sql = " insert into `amazon_cn_18zhongbiao`(`proid`,`prourl`,`goodRateShow`,`commentCount`," \
                                              "`goodCount`,`poorCount`,`commentid`,`guid`,`content`,`reviewtitle`,`creationTime`," \
                                              "`score`,`usefulVoteCount`) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                                        cursor.execute(sql, (pro_id, pro_url, goodRateShow, commentCount, goodCount, poorCount,
                                                             comment_id, guid,content, title, creationTime, score, usefulVoteCount))
                                        connection.commit()
                                        print("6-2--写入成功")
                                except:
                                    pass
                                finally:
                                    connection.close()
                        except:
                            ma = '007-2-具体评论-出错:'  + str(skuj) + '商品---' + time.asctime(time.localtime(time.time()))
                            print(ma)
                            log(ma)
                            pass
                    except:
                        ma = '008-2-评论页网络-出错:' + str(skuj) + '商品---' + time.asctime(time.localtime(time.time()))
                        print(ma)
                        log(ma)
                        pass
                    continue
            except:
                ma = '009-2-评论页网络-出错:' +  str(skuj) + '商品---' + time.asctime(time.localtime(time.time()))
                print(ma)
                log(ma)
                pass
    except:
        ma = '009-1-评论页网络-出错:' + str(skuj) + '商品---' + time.asctime(time.localtime(time.time()))
        print(ma)
        log(ma)
        pass


if __name__ == "__main__":
    for k in range(1,400):
        ips = [line.strip() for line in open(r"ips-1.txt", "r")]
        if len(ips)<10:
            ips = [line.strip() for line in open(r"ips.txt", "r")]
        time.sleep(random.random() * 10)
        ma='1---正在获取第' + str(k) + '页:  '+time.asctime(time.localtime(time.time()))
        print(ma)
        log(ma)
        url1="?fst=as%3Aoff&rh=n%3A172282%2Ck%3Awatches%2Cp_72%3A1248882011&page="
        url2="&sort=review-rank&keywords=watches&ie=UTF8&lo=electronics"
        url="https://www.amazon.com/s/ref=sr_pg_"+str(k)+url1+str(k)+url2
        sku = get_url2(url)
        try:
            n_thread = random.randint(50,200)
            thread(n_thread, sku)
        except:
            ma='010--商品页-出错:' + str(k) + '页'+time.asctime(time.localtime(time.time()))
            print(ma)
            log(ma)
            pass
        print(str(k)+'--->Next')


