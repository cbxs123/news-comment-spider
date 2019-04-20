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
# 京东家居信息采集：商品ID、价格、整体好评度、总评论数、晒图、追评、好评、中评、差评、
#               评论者ID、评论、星级、赞、回复数等
# bsObj = BeautifulSoup(urlopen(url), 'lxml')
# selector = Selector(res)

for k in range(0,20):
    print("1---正在获取第{}页!".format(k))
    time.sleep(random.randint(5,10))
    url="https://search.jd.com/Search?keyword=%E5%AE%B6%E5%B1%85&enc=utf-8&qrst=1&rt=1&stop=1&vt=2&wq=%E5%AE%B6%E5%B1%85&psort=4&stock=1&page="+str(2*k+1)
    res=requests.get(url)
    res.encoding='utf-8'
    root=etree.HTML(res.text)
    sku=root.xpath('//li[@class="gl-item"]/@data-sku')
    try:
        for j in range(len(sku)):
            try:
                print("2---正在获取{}商品!".format(sku[j]))
                time.sleep(random.randint(5,8))
                url1="https://item.jd.com/" + str(sku[j])+'.html'
                res = requests.get(url1)
                root = etree.HTML(res.text)
                ss=root.xpath('//script/text()')[0].split()
                vvid=re.match( r'(.*?)commentVersion:\'(.*?)\'(.*?)',''.join(ss)).group(2)
                priceurl="https://p.3.cn/prices/mgets?skuids=J_"+str(sku[j])
                pricedata=requests.get(priceurl)
                pricepat='"p":"(.*?)"'
                thisprice=re.compile(pricepat).findall(pricedata.text)
                price=thisprice[0]
                try:
                    for i in range(10):
                        print("3---正在获取{}页评论数据!".format(i))
                        time.sleep(random.randint(5,15))
                        percent_url1="https://sclub.jd.com/comment/productPageComments.action?callback=fetchJSON_comment98vv"
                        percent_url2="&productId="
                        percent_url3="&score=0&sortType=5&page="
                        percent_url4="&pageSize=10&isShadowSku=0&fold=1"
                        html=requests.get(percent_url1+str(vvid)+percent_url2+str(sku[j])+percent_url3+str(i)+percent_url4).text
                        print('4---'+str(k)+'页-'+str(j)+':'+str(sku[j])+'商品-'+str(i)+'页评论-'+str(vvid))

                        all_percent=re.findall(r'"goodRateShow":(.*?),', html)[0]+'%'
                        sum_comment=re.findall(r'"commentCount":(.*?),', html)[0]
                        sum_tu=re.findall(r'"imageListCount":(.*?),', html)[0]
                        sum_zhui=re.findall(r'"afterCount":(.*?),', html)[0]
                        sum_good=re.findall(r'"goodCount":(.*?),', html)[0]
                        sum_mid=re.findall(r'"generalCount":(.*?),', html)[0]
                        sum_bad=re.findall(r'"poorCount":(.*?),', html)[0]
                        coms =re.findall(r'"topped":(.*?),"userImageUrl":(.*?),', html)
                        try:
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
                                        sql = " insert into `jd_180831_jiaju`(`proid`,`prourl`,`proprice`,`goodRateShow`,`commentCount`," \
                                             "`imageListCount`,`afterCount`,`goodCount`,`generalCount`,`poorCount`,`commentid`,`guid`,`content`,`creationTime`," \
                                             "`replyCount`,`score`,`usefulVoteCount`) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                                        cursor.execute(sql, (pro_id,pro_url,pro_price,goodRateShow,commentCount,imageListCount,
                                                        afterCount,goodCount,generalCount,poorCount,comment_id,guid,content,creationTime,replyCount,
                                                        score,usefulVoteCount))
                                        connection.commit()
                                        print("6---写入成功")
                                finally:
                                    connection.close()
                        except:
                            print('007---出错:'+str(k)+'页-'+str(j)+':'+str(sku[j])+'商品-'+str(i)+'页评论-'+str(vvid))
                            pass
                except:
                    print('008---出错:' + str(k) + '页-' + str(j) + ':' + str(sku[j]) + '商品-'  + str(vvid))
                    pass
            except:
                print('009---出错:' + str(k) + '页-' + str(j) + ':' + str(sku[j]) + '商品')
                pass
    except:
        print('010---出错:' + str(k) + '页')
        pass




