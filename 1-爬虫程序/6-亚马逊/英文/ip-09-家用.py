import os
import re
import time
import queue
import random
import requests
import threading
import pymysql.cursors
from lxml import etree
from fake_useragent import UserAgent
os.chdir("D:/1")

def log(ma):
    file = open(r'09_家用.txt', mode='a+', encoding='utf-8')
    file.write(ma+'\n')
    file.close()

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
            res = requests.get(url=url, headers=headers(),proxies=proxy,timeout=30)
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

def foo(skuj):
    try:
        url3 = "https://www.amazon.cn/dp/" + str(skuj) + '/ref=cm_cr_arp_d_bdcrb_top?ie=UTF8'
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
                        sql = " insert into `amazon_cn_09jiayong`(`proid`,`prourl`,`goodRateShow`,`commentCount`," \
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
                root = get_url1(url1,ips)
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
                        root = get_url1(url2,ips)
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
                                        sql = " insert into `amazon_cn_09jiayong`(`proid`,`prourl`,`goodRateShow`,`commentCount`," \
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
        if len(ips)<50:
            ips = [line.strip() for line in open(r"ips.txt", "r")]
        time.sleep(random.random() * 10)
        ma='1---正在获取第' + str(k) + '页'+time.asctime(time.localtime(time.time()))
        print(ma)
        log(ma)
        url="https://www.amazon.cn/s/ref=sr_pg_"+str(k)+"?sf=sbc%2Cqz&rh=n%3A2016126051%2Ck%3A%E5%AE%B6%E7%94%A8&page="+str(k)+"&sort=review-rank&keywords=%E5%AE%B6%E7%94%A8&ie=UTF8&lo=home"
        #    https://www.amazon.cn/s/ref=sr_pg_3?rh=n%3A816482051%2Ck%3A%E7%8F%A0%E5%AE%9D%E9%A6%96%E9%A5%B0&page=3&sort=review-rank&keywords=%E7%8F%A0%E5%AE%9D%E9%A6%96%E9%A5%B0&ie=UTF8&qid=1538900232&lo=mi
        try:
            root=get_url1(url,ips)
            sku = root.xpath('//div[@id="atfResults"]//@data-asin')
            n_thread = random.randint(50,200)
            thread(n_thread, sku)
        except:
            ma='010--商品页-出错:' + str(k) + '页'+time.asctime(time.localtime(time.time()))
            print(ma)
            log(ma)
            pass
        print(str(k)+'--->Next')