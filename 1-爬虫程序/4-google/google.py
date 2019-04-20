import os
import re
import execjs
import random
import requests
from multiprocessing import Pool
from fake_useragent import UserAgent

class Py4Js():
    def __init__(self):
        self.ctx = execjs.compile("""
        function TL(a) {
        var k = "";
        var b = 406644;
        var b1 = 3293161072;
        var jd = ".";
        var $b = "+-a^+6";
        var Zb = "+-3^+b+-f";
        for (var e = [], f = 0, g = 0; g < a.length; g++) {
            var m = a.charCodeAt(g);
            128 > m ? e[f++] = m : (2048 > m ? e[f++] = m >> 6 | 192 : (55296 == (m & 64512) && g + 1 < a.length && 56320 == (a.charCodeAt(g + 1) & 64512) ? (m = 65536 + ((m & 1023) << 10) + (a.charCodeAt(++g) & 1023),
            e[f++] = m >> 18 | 240,
            e[f++] = m >> 12 & 63 | 128) : e[f++] = m >> 12 | 224,
            e[f++] = m >> 6 & 63 | 128),
            e[f++] = m & 63 | 128)
        }
        a = b;
        for (f = 0; f < e.length; f++) a += e[f],
        a = RL(a, $b);
        a = RL(a, Zb);
        a ^= b1 || 0;
        0 > a && (a = (a & 2147483647) + 2147483648);
        a %= 1E6;
        return a.toString() + jd + (a ^ b)
    };
    function RL(a, b) {
        var t = "a";
        var Yb = "+";
        for (var c = 0; c < b.length - 2; c += 3) {
            var d = b.charAt(c + 2),
            d = d >= t ? d.charCodeAt(0) - 87 : Number(d),
            d = b.charAt(c + 1) == Yb ? a >>> d: a << d;
            a = b.charAt(c) == Yb ? a + d & 4294967295 : a ^ d
        }
        return a
    }
    """)
    def getTk(self, text):
        return self.ctx.call("TL", text)

def get_url1(url,param):
    def headers():
        header = {'User-Agent': UserAgent().random,
                  'Connection': 'keep-alive'}
        return header
    code,i=503,0
    # https://ip.ihuan.me/tqdl.html
    # http://www.thebigproxylist.com/
    # https://topsiteproxy.com/
    # https://superproxylist.com/
    while code!=200 and i<300:
        ips = [line.strip() for line in open(r"ips-1.txt", "r")]
        if len(ips) < 50:
            ips = [line.strip() for line in open(r"ips.txt", "r")]
        i += 1
        while True:
            try:
                host = random.choice(ips)
                break
            except:
                pass
                continue
        proxy = {'http': 'http://' + host, 'https': 'https://' + host}
        try:
            res = requests.get(url=url, headers=headers(),proxies=proxy,params=param,timeout=30)
            if res.status_code == 200:
                return res
        except:
            pass
        print('第%d次尝试'%i)
        if i == 300:
            return requests.get(url=url, headers=headers(),params=param)

def translate(tk, content,dl2,sig=0):
    param = {'tk': tk, 'q': content}
    if sig==0:
        url="""http://translate.google.cn/translate_a/single?client=t&sl=en
            &tl=zh-CN&hl=zh-CN&dt=at&dt=bd&dt=ex&dt=ld&dt=md&dt=qca&dt=rw&dt=rm&dt=ss
            &dt=t&ie=UTF-8&oe=UTF-8&clearbtn=1&otf=1&pc=1&srcrom=0&ssel=0&tsel=0&kc=2"""
        result = get_url1(url, param)
        s1 = ''
        for text in result.json()[0][:-1]:
            s1 += text[0]
        file1 = open(dl2, 'a+')
        file1.write(''.join(s1).replace('\n', ' ')+'\n')
        file1.close()
    else:
        url="""http://translate.google.cn/translate_a/single?client=t&sl=zh-CN
            &tl=en&hl=zh-CN&dt=at&dt=bd&dt=ex&dt=ld&dt=md&dt=qca&dt=rw&dt=rm&dt=ss
            &dt=t&ie=UTF-8&oe=UTF-8&clearbtn=1&otf=1&pc=1&srcrom=0&ssel=0&tsel=0&kc=2"""
        result = get_url1(url, param)
        s1 = ''
        for text in result.json()[0][:-1]:
            s1 += text[0].lower()
        file1 = open(dl2, 'a+')
        file1.write(''.join(s1).replace('\n', ' ')+'\n')
        file1.close()

if __name__ == "__main__":
    # dls=os.listdir()
    # def foo(dl):
    #     if re.match(r'CrossLDA.*',dl):
    #         top=int(re.match(r'.*ge(.*?)_lamda.*',dl).group(1))
    #         content1=[line.strip() for line in open(dl, "r")]
    #         dl1=dl[:-4]+'-en-cn.txt'
    #         open(dl1, 'w+').close()
    #         for i in range(top):
    #             print(str(i))
    #             js = Py4Js()
    #             tk = js.getTk(content1[i].replace(' ', '\n'))
    #             translate(tk, content1[i].replace(' ', '\n'), dl1,sig=0)
    #         for i in range(top, 2 * top):
    #             print(str(i))
    #             js = Py4Js()
    #             tk = js.getTk(content1[i].replace(' ', '\n'))
    #             translate(tk, content1[i].replace(' ', '\n'), dl1,sig=1)
    # pool=Pool(10)
    # pool.map(foo,dls)
    # pool.close()
    # pool.join()
    while True:
        print('temp1.txt---源语言(以\\\分割段落)；temp2.txt---目标语言；temp1.txt不输入则退出')
        open('temp1.txt','w+').close()
        open('temp2.txt','w+').close()
        os.system('notepad temp1.txt')
        content1=' '.join([line.strip() for line in open('temp1.txt', "r",encoding='utf-8')]).replace('- ','').split('\\\\')
        if len(content1[0])>0:
            for i in range(len(content1)):
                print('%d/%d'%(i,len(content1)))
                js = Py4Js()
                tk = js.getTk(content1[i])
                translate(tk, content1[i],'temp2.txt',sig=0)
            os.system('notepad temp2.txt')
        else:
            break
