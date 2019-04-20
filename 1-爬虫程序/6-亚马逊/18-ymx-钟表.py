import re
import os
import requests
import time
import random
import pymysql.cursors
from lxml import etree
os.chdir("D:/1/毕业论文/180430-KT/数据/02-亚马逊")
# 京东钟表信息采集：商品ID、价格、整体好评度、总评论数、晒图、追评、好评、中评、差评、
#               评论者ID、评论、星级、赞、回复数等
# bsObj = BeautifulSoup(urlopen(url), 'lxml')
# selector = Selector(res)

def log(ma):
    file = open('18_钟表.txt', mode='a+', encoding='utf-8')
    file.write(ma)
    file.close()

def get_url(url):
    user_agent_list = ["Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 "
                       "(KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1",
                       "Mozilla/5.0 (X11; CrOS i686 2268.111.0) AppleWebKit/536.11 "
                       "(KHTML, like Gecko) Chrome/20.0.1132.57 Safari/536.11",
                       "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.6 "
                       "(KHTML, like Gecko) Chrome/20.0.1092.0 Safari/536.6",
                       "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.6 "
                       "(KHTML, like Gecko) Chrome/20.0.1090.0 Safari/536.6",
                       "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.1 "
                       "(KHTML, like Gecko) Chrome/19.77.34.5 Safari/537.1",
                       "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/536.5 "
                       "(KHTML, like Gecko) Chrome/19.0.1084.9 Safari/536.5",
                       "Mozilla/5.0 (Windows NT 6.0) AppleWebKit/536.5 "
                       "(KHTML, like Gecko) Chrome/19.0.1084.36 Safari/536.5",
                       "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 "
                       "(KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
                       "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/536.3 "
                       "(KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
                       "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_0) AppleWebKit/536.3 "
                       "(KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
                       "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 "
                       "(KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3",
                       "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 "
                       "(KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3",
                       "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 "
                       "(KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
                       "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 "
                       "(KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
                       "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/536.3 "
                       "(KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
                       "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 "
                       "(KHTML, like Gecko) Chrome/19.0.1061.0 Safari/536.3",
                       "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/535.24 "
                       "(KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24",
                       "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/535.24 "
                       "(KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24"]
    code,i=503,0
    while code!=200 or i<15:
        header1 = {
            'Pragma': 'no-cache',
            'Cache-Control': 'no-cache',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': random.choice(user_agent_list),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.8,en;q=0.6,fr;q=0.4,zh-TW;q=0.2',
        }
        time.sleep(random.randint(1,2))
        res = requests.get(url=url, headers=header1, timeout=30)
        code = res.status_code
        i+=1
        if code==200:
            return etree.HTML(res.text)
        if i==14:
            return etree.HTML(res.text)

for k in range(1,50):
    print("1---正在获取第{}页!".format(k))
    url="https://www.amazon.cn/s/ref=sr_pg_"+str(k)+"?rh=n%3A1953164051%2Ck%3A%E9%92%9F%E8%A1%A8%E8%85%95%E8%A1%A8&page="+str(k)+"&sort=relevancerank&keywords=%E9%92%9F%E8%A1%A8%E8%85%95%E8%A1%A8&ie=UTF8"
    root=get_url(url)
    time.sleep(random.randint(10,15))
    sku = root.xpath('//div[@id="atfResults"]//@data-asin')
    try:
        for j in range(len(sku)):
            try:
                url1="https://www.amazon.cn/product-reviews/" + str(sku[j])+'/ie=UTF8&reviewerType=all_reviews'
                root = get_url(url1)
                time.sleep(random.randint(8,12))
                print(" 2---正在获取{}商品!".format(sku[j]))
                sum_comment=root.xpath('//div[@class="a-row"]/span[@data-hook="total-review-count"]/text()')[0]
                all_percent=root.xpath('//div[@class="a-row averageStarRatingNumerical"]//span[@data-hook="rating-out-of-text"]/text()')[0][:3]
                sum_good=root.xpath('//div[@class="a-row"]/span[@class="a-declarative"]/a/text()')[0].split()[1]
                sum_bad=root.xpath('//div[@class="a-row"]/span[@class="a-declarative"]/a/text()')[1].split()[1]
                try:
                    for i in range(int(int(sum_comment)/10)):
                        i=i+1
                        percent_url1="https://www.amazon.cn/product-reviews/"
                        percent_url2="/ie=UTF8&reviewerType=all_reviews/ref=cm_cr_arp_d_paging_btm_"
                        percent_url3="?pageNumber="
                        url2=percent_url1+str(sku[j])+percent_url2+str(i)+percent_url3+str(i)
                        root = get_url(url2)
                        time.sleep(random.randint(10,15))
                        print('  3---'+str(k)+'页-'+str(j)+':'+str(sku[j])+'商品-'+str(i)+'页评论-')

                        comment_id1=root.xpath('//div[@id="cm_cr-review_list"]/div/@id')
                        score1=root.xpath('//div[@id="cm_cr-review_list"]//i[@data-hook="review-star-rating"]/span/text()')
                        title1 = root.xpath('//div[@id="cm_cr-review_list"]//a[@data-hook="review-title"]/text()')
                        guid1 = root.xpath('//div[@id="cm_cr-review_list"]//a[@data-hook="review-author"]/text()')
                        creationTime1=root.xpath('//div[@id="cm_cr-review_list"]//span[@data-hook="review-date"]/text()')
                        content1=root.xpath('//div[@id="cm_cr-review_list"]//span[@data-hook="review-body"]/text()')
                        useful1=''.join(root.xpath('//div[@id="cm_cr-review_list"]//text()')).split('留言者')
                        try:
                            for m in range(len(comment_id1)):
                                # 商品名称、商家名称、价格、整体好评度、总评论数、晒图、追评、好评、中评、差评、评论者ID、评论、星级、赞、回复数等
                                pro_id=sku[j]
                                pro_url=url2
                                goodRateShow=all_percent
                                commentCount=sum_comment
                                goodCount=sum_good
                                poorCount=sum_bad
                                comment_id=comment_id1[m]
                                title=title1[m]
                                guid=guid1[m]
                                content=content1[m]
                                creationTime=creationTime1[m][2:]
                                score=score1[m][:3]
                                usefulCount=re.findall(r'(.*?)个人发现此评论有用.*', useful1[m+1])
                                if usefulCount:usefulVoteCount=usefulCount[0].strip()
                                else:usefulVoteCount='0'

                                print(content)
                                connection = pymysql.connect(host='localhost', user='root', password='asd123',db='amazoncn', charset='utf8mb4')
                                try:
                                    with connection.cursor() as cursor:
                                        sql = " insert into `amazon_cn_18zhongbiao`(`proid`,`prourl`,`goodRateShow`,`commentCount`," \
                                             "`goodCount`,`poorCount`,`commentid`,`guid`,`content`,`reviewtitle`,`creationTime`," \
                                             "`score`,`usefulVoteCount`) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                                        cursor.execute(sql, (pro_id,pro_url,goodRateShow,commentCount,goodCount,poorCount,comment_id,guid,
                                                             content,title,creationTime,score,usefulVoteCount))
                                        connection.commit()
                                        print("6---写入成功")
                                finally:
                                    connection.close()
                        except:
                            ma='007--具体评论-出错:'+str(k)+'页-'+str(j)+':'+str(sku[j])+'商品-'+str(i)+'页评论-'+time.asctime(time.localtime(time.time()))
                            print(ma)
                            log(ma)
                            pass
                except:
                    ma='008--评论页-出错:' + str(k) + '页-' + str(j) + ':' + str(sku[j]) + '商品-'+time.asctime(time.localtime(time.time()))
                    print(ma)
                    log(ma)
                    pass
            except:
                ma='009--具体商品-出错:' + str(k) + '页-' + str(j) + ':' + str(sku[j]) + '商品'+time.asctime(time.localtime(time.time()))
                print(ma)
                log(ma)
                pass
    except:
        ma='010--商品页-出错:' + str(k) + '页'+time.asctime(time.localtime(time.time()))
        print(ma)
        log(ma)
        pass
