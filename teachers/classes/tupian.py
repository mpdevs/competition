# -*- coding: gbk -*-

import sys,urllib,re,os,sys,re,pickle
import string
import urllib2
import re
import time
import json
from BeautifulSoup import BeautifulSoup
##import datetime
#reload(sys)
#sys.setdefaultencoding( "utf-8" )


#----------- 处理页面上的各种标签 -----------
class HTML_Tool:
    # 用非 贪婪模式 匹配 \t 或者 \n 或者 空格 或者 超链接 或者 图片
    BgnCharToNoneRex = re.compile("(\t|\n| |<a.*?>|<img.*?>)")
    
    # 用非 贪婪模式 匹配 任意<>标签
    EndCharToNoneRex = re.compile("<.*?>")

    # 用非 贪婪模式 匹配 任意<p>标签
    BgnPartRex = re.compile("<p.*?>")
    CharToNewLineRex = re.compile("(<br/>|</p>|<tr>|<div>|</div>)")
    CharToNextTabRex = re.compile("<td>")

    # 将一些html的符号实体转变为原始符号
    replaceTab = [("&lt;","<"),("&gt;",">"),("&amp;","&"),("&amp;","\""),("&nbsp;"," ")]
    
    def Replace_Char(self,x):
        x = self.BgnCharToNoneRex.sub("",x)
        x = self.BgnPartRex.sub("\n    ",x)
        x = self.CharToNewLineRex.sub("\n",x)
        x = self.CharToNextTabRex.sub("\t",x)
        x = self.EndCharToNoneRex.sub("",x)

        for t in self.replaceTab:  
            x = x.replace(t[0],t[1])  
        return x

class Spider:
    # 申明相关的属性
    def __init__(self,url,fileName):  
        self.myUrl = url
        self.myTool = HTML_Tool()
        self.filename = fileName
        self.getmaxpage=True

    # 初始化加载页面并将其转码储存
    def start(self):
        # 设置保存的文件路径
        names = []
        
        f = file('sample.txt')
        #f.readline()
        while True:
            line = f.readline().rstrip()
            if len(line) == 0: 
                break
            names.append(line)
        f.close()
        
        # 加载页面数据到数组中
        for i in names:
            if i=='':
                continue
            #print i
            
            self.getmaxpage=True
            maxpage = 1
            pagenow = 1
            while pagenow <= maxpage:
                time.sleep(0.3)
                urlnow = 'https://item.taobao.com/item.htm?id='+i.split('\t')[0]
                while True:
                    try:
                        if self.getmaxpage==True:
                            self.get_data(urlnow,i.split('\t')[0])
##                            print maxpage
                            self.getmaxpage=False
                        else:
                            self.get_data(urlnow,i.split('\t')[0])
                        break
                    except Exception,e:
                        f = open('cuo.txt','a+')
                        f.write(i.replace('\n','').replace('\r','')+'\n')
                        f.close()
                        print e
                        break
                pagenow+=1
            


    # 获取页面源码并将其存储到数组中
    def get_data(self,url,uid):
        print url
        req_header = {
            'cookie':'cna=WqSwEEXVsQ4CAWVR50DyACUX; t=a443967dc3c1b643bec1119e25e27006; _tb_token_=dFwik7jNgq7N; cookie2=13b9db4e0c0c52ef7f2e94f7d5d7dfbd; pnm_cku822=047UW5TcyMNYQwiAiwQRHhBfEF8QXtHcklnMWc%3D%7CUm5Ockt%2BQn5EfElxSHFOcyU%3D%7CU2xMHDJ7G2AHYg8hAS8WLgAgDlIzVTleIFp0InQ%3D%7CVGhXd1llXGlVaVNrXmZfZllkU25Mckp1QXhNdkN8R3tDe0V7QnhWAA%3D%3D%7CVWldfS0RMQ4zDTAQJQUrFmdZNwkvEi4AVgA%3D%7CVmhIGCQZJgY9AjoaJhkjFjYIMgg3FysfIB09ATwJNBQoHCMePgI%2FBjttOw%3D%3D%7CV25Tbk5zU2xMcEl1VWtTaUlwJg%3D%3D; cq=ccp%3D1; l=AqmpjcyBfFdTBhzwNfFNCMzwOVsDcp2o; isg=AqCgHw9f7KslT1DMBGYvFNuEca6PPYRzixFWyBqxaLtOFUA_wrlUA3an2yB3',

            'user-agent':'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.101 Safari/537.36',

        }
        req_timeout = 5
        req = urllib2.Request(url,None,req_header)
        resp = urllib2.urlopen(req,None,req_timeout)
        myPage = resp.read()  
               
        # 将myPage中的html代码处理并存储到datas里面
        soup = BeautifulSoup(str(myPage),fromEncoding="UTF-8")
        #print soup,raw_input()
        if soup.find('ul',{'class':'attributes-list'}):
            MainPicUrl=soup.find('div',{'class':'tb-booth tb-pic tb-main-pic'}).find('img')['src'].encode('utf8')
        else:
            MainPicUrl=soup.find('div',{'class':'tb-booth'}).find('img')['src'].encode('utf8')
        urllib.urlretrieve('http:'+MainPicUrl, '%s.jpg' %uid)
        data = ''
        #保存文件
        f = open(self.filename,'a+')
        f.write(data+'\n')
        f.close()
        #返回最大页码


        



#http://i.service.autohome.com.cn/clubapp/OtherTopic-3921432-all-1.html
myurl = 'http://tieba.baidu.com/p/'
filename = '鼻炎话题.txt'
#调用
f = file(filename,'w')
f.write('PostID\tPostUrl\tPostTitle\tReplyPostCount\tFloorNO\tUserName\tUserID\tIsLouzhu\tTiebaRank\tPostContent\tPostDate\tReplyContentGroup\tCreatedDate\n')
f.close()
mySpider = Spider(myurl,filename)
mySpider.start()













            
