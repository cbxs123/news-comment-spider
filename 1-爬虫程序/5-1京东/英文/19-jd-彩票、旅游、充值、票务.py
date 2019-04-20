import pandas as pd
import re
import os
import requests
import time
import random
import pymysql.cursors
from lxml import etree
from pandas import DataFrame

os.chdir("D:/1/毕业论文/180430-KT/数据/01-京东")
# 京东手机信息采集：商品ID、价格、整体好评度、总评论数、晒图、追评、好评、中评、差评、
#               评论者ID、评论、星级、赞、回复数等
# bsObj = BeautifulSoup(urlopen(url), 'lxml')
# selector = Selector(res)
for k in range(1,10):
    print("正在获取第{}页!".format(k))
    time.sleep(random.randint(5,10))
    url="https://list.jd.com/list.html?cat=9987,653,655&page="+str(k)
    res=requests.get(url)
    res.encoding='utf-8'
    root=etree.HTML(res.text)
    sku=root.xpath('//li[@class="gl-item"]/div/@data-sku')
    
    for j in range(len(sku)):
            print("正在获取{}商品!".format(sku[j]))
            time.sleep(random.randint(10,15))
            url1="https://item.jd.com/" + str(sku[j])+'.html'
            res = requests.get(url1)
            root = etree.HTML(res.text)
            ss=root.xpath('//script[@charset="gbk"]/text()')[0].split()
            vvid=re.match( r'(.*?)commentVersion:\'(.*?)\'(.*?)', ''.join(ss)).group(2)
            priceurl="https://p.3.cn/prices/mgets?skuids=J_"+str(sku[j])
            pricedata=requests.get(priceurl)
            pricepat='"p":"(.*?)"}'
            thisprice=re.compile(pricepat).findall(pricedata.text)
            price=thisprice[0]

            for i in range(10):
                print("正在获取{}页评论数据!".format(i))
                time.sleep(random.randint(15,25))
                percent_url1="https://sclub.jd.com/comment/productPageComments.action?callback=fetchJSON_comment98vv"
                percent_url2="&productId="
                percent_url3="&score=0&sortType=5&page="
                percent_url4="&pageSize=10&isShadowSku=0&fold=1"
                html=requests.get(percent_url1+str(vvid)+percent_url2+str(sku[j])+percent_url3+str(i)+percent_url4).text
                print(str(vvid)+'-'+str(sku[j])+'-'+str(i))

                all_percent=re.findall(r'"goodRateShow":(.*?),', html)[0]+'%'
                sum_comment=re.findall(r'"commentCount":(.*?),', html)[0]
                sum_tu=re.findall(r'"imageListCount":(.*?),', html)[0]
                sum_zhui=re.findall(r'"afterCount":(.*?),', html)[0]
                sum_good=re.findall(r'"goodCount":(.*?),', html)[0]
                sum_mid=re.findall(r'"generalCount":(.*?),', html)[0]
                sum_bad=re.findall(r'"poorCount":(.*?),', html)[0]
                coms =re.findall(r'"topped":(.*?),"userImageUrl":(.*?),', html)

                for com in coms:
                # 商品名称、商家名称、价格、整体好评度、总评论数、晒图、追评、好评、中评、差评、评论者ID、评论、星级、赞、回复数等
                    pro_id=sku[j]
                    pro_url=url1
                    pro_price=price

                    goodRateShow=all_percent
                    commentCount=sum_comment
                    imageListCount=sum_tu
                    afterCount=sum_zhui
                    goodCount=sum_good
                    generalCount=sum_mid
                    poorCount=sum_bad

                    comment_id=re.findall(r'"guid":(.*?),',com[0])[0]
                    guid=re.findall(r'"guid":(.*?),',com[0])[0]
                    content=re.findall(r'"content":(.*?),',com[0])[0]
                    creationTime=re.findall(r'"creationTime":(.*?),',com[0])[0][1:-1]
                    replyCount=re.findall(r'"replyCount":(.*?),',com[0])[0]
                    score=re.findall(r'"score":(.*?),',com[0])[0]
                    usefulVoteCount=re.findall(r'"usefulVoteCount":(.*?),',com[0])[0]
                    
                    print(content)
                    connection = pymysql.connect(host='localhost', user='root', password='asd123',db='jd1', charset='utf8mb4')
                    try:
                        with connection.cursor() as cursor:
                            sql = " insert into `jd_180828`(`proid`,`prourl`,`proprice`,`goodRateShow`,`commentCount`," \
                              "`imageListCount`,`afterCount`,`goodCount`,`generalCount`,`poorCount`,`commentid`,`guid`,`content`,`creationTime`," \
                              "`replyCount`,`score`,`usefulVoteCount`) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                            cursor.execute(sql, (pro_id,pro_url,pro_price,goodRateShow,commentCount,imageListCount,
                                         afterCount,goodCount,generalCount,poorCount,comment_id,guid,content,creationTime,replyCount,
                                         score,usefulVoteCount))
                            connection.commit()
                            print("---------------------- 哈哈-------------------------")
                    finally:
                        connection.close()

