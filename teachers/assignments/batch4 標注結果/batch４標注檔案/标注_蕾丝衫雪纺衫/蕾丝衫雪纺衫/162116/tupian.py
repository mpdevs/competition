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


#----------- ����ҳ���ϵĸ��ֱ�ǩ -----------
class HTML_Tool:
    # �÷� ̰��ģʽ ƥ�� \t ���� \n ���� �ո� ���� ������ ���� ͼƬ
    BgnCharToNoneRex = re.compile("(\t|\n| |<a.*?>|<img.*?>)")
    
    # �÷� ̰��ģʽ ƥ�� ����<>��ǩ
    EndCharToNoneRex = re.compile("<.*?>")

    # �÷� ̰��ģʽ ƥ�� ����<p>��ǩ
    BgnPartRex = re.compile("<p.*?>")
    CharToNewLineRex = re.compile("(<br/>|</p>|<tr>|<div>|</div>)")
    CharToNextTabRex = re.compile("<td>")

    # ��һЩhtml�ķ���ʵ��ת��Ϊԭʼ����
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
    # ������ص�����
    def __init__(self,url,fileName):  
        self.myUrl = url
        self.myTool = HTML_Tool()
        self.filename = fileName
        self.getmaxpage=True

    # ��ʼ������ҳ�沢����ת�봢��
    def start(self):
        # ���ñ�����ļ�·��
        names = []
        
        f = file('photo_162116_2.txt')
        #f.readline()
        while True:
            line = f.readline().rstrip()
            if len(line) == 0: 
                break
            names.append(line)
        f.close()
        
        # ����ҳ�����ݵ�������
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
            


    # ��ȡҳ��Դ�벢����洢��������
    def get_data(self,url,uid):
        print url
        req_header = {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.63 Safari/537.36',
        }
        req_timeout = 5
        req = urllib2.Request(url,None,req_header)
        resp = urllib2.urlopen(req,None,req_timeout)
        myPage = resp.read()  
               
        # ��myPage�е�html���봦���洢��datas����
        soup = BeautifulSoup(str(myPage),fromEncoding="UTF-8")
        #print soup,raw_input()
        if soup.find('ul',{'class':'attributes-list'}):
            MainPicUrl=soup.find('div',{'class':'tb-booth tb-pic tb-main-pic'}).find('img')['src'].encode('utf8')
        else:
            MainPicUrl=soup.find('div',{'class':'tb-booth'}).find('img')['src'].encode('utf8')
        urllib.urlretrieve('http:'+MainPicUrl, '%s.jpg' %uid)
        data = ''
        #�����ļ�
        f = open(self.filename,'a+')
        f.write(data+'\n')
        f.close()
        #�������ҳ��


        



#http://i.service.autohome.com.cn/clubapp/OtherTopic-3921432-all-1.html
myurl = 'http://tieba.baidu.com/p/'
filename = '���׻���.txt'
#����
f = file(filename,'w')
f.write('PostID\tPostUrl\tPostTitle\tReplyPostCount\tFloorNO\tUserName\tUserID\tIsLouzhu\tTiebaRank\tPostContent\tPostDate\tReplyContentGroup\tCreatedDate\n')
f.close()
mySpider = Spider(myurl,filename)
mySpider.start()













            
