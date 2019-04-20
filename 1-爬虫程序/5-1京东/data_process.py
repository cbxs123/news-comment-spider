# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
import os
import json 
import re
import jieba
import pandas as pd
import numpy as np
from gensim.models import word2vec  
os.chdir("/home/ys/桌面/数据/01-京东")

#f=open(r"/home/ys/桌面/数据/01-京东/中文-data/jd.json","r")

def stopwordslist(filepath):  
    stopwords = [line.strip() for line in open(filepath, 'r', encoding='utf-8').readlines()]  
    return stopwords 

def re_sentence(sentence):
    if sentence==None:
        sentence=''
    else:
        sentence=re.sub('[’!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~\s+0-9a-zA-Z、，；。！\～：（）\《》★一\【】]+','',sentence)
    return sentence

def seg_sentence(sentence):
    sentence_seged = jieba.cut(sentence)
    outstr = '' 
    stopwords = stopwordslist('stopwords.txt')  
    for word in sentence_seged:  
        if word not in stopwords:  
            if word != '\t':  
                outstr += word  
                outstr += " "  
    return outstr 

with open(r"/home/ys/桌面/数据/01-京东/中文-data/jd.json","r") as f:
    data=pd.DataFrame(json.load(f)['RECORDS'])
    file = open('Corpus.txt',mode='w+',encoding='utf-8')
    for i in range(len(data)):
        print(str(i)+'/'+str(len(data)))
        data['commentid'][i]=data['commentid'][i][1:-1]
        data['guid'][i]=data['guid'][i][1:-1]
        data['content'][i]=re_sentence(data['content'][i][1:-1])
        file.write( seg_sentence(data['content'][i])+'\n')
    file.close()
    data.to_csv("test.csv",index=False,sep=',')
    model=word2vec.Word2Vec(word2vec.Text8Corpus(u"Corpus.txt"),size=64,min_count=1)
    model.wv.save_word2vec_format(u"model.bin", binary=True)
        
    






