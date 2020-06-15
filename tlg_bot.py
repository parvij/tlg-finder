# -*- coding: utf-8 -*-
"""
Created on Fri Jun 24 13:35:29 2016

@author: parvij
"""

import time
import urllib.request as urllib2
from bs4 import BeautifulSoup
import pandas as pd
import re

sent_log = pd.DataFrame({'telegram_id':[],'url':[],'msg':[]})

###########################################################################################################
class extractor:
    
    def __init__(self,url,additional_filter,path,content_path,message_shape):
        self.url = url
        self.path = path
        self.content_path = content_path
        self.message_shape = message_shape
        self.additional_filter = additional_filter
        
    def extract_url(self):
        page_content=urllib2.urlopen(self.url).read().decode("utf-8")
        soup = BeautifulSoup(page_content,"lxml")
        self.soup = soup        

    def extract_path(self):
        sub_soup=self.soup
        for p in self.path:
            sub_soup=sub_soup.find(*p[:1],**p[1])
        self.sub_soup = sub_soup
        
    def get_list(self):
        self.content = self.sub_soup.find_all(*self.content_path[:1],**self.content_path[1])
        
    def extract_items(self,post):
        items={}
        for shaper_key,shaper_lambda in self.message_shape.items():
            items[shaper_key] = shaper_lambda(post)
        return items
    
    def content_shaper(self,post_dic):
        msg='# '
        for shaper_key,value in post_dic.items():
            msg+=value+'\n'
        return msg
        
    def pass_additional_filter(self,post):
        return True
    
    def get_list_of_msg_url(self):
        result = []
        for raw_post in self.content:  
            post_dic = self.extract_items(raw_post)
            print('.',end='')
            if self.pass_additional_filter(post_dic):
                result.append((self.content_shaper(post_dic),post_dic['url']))
        return result

########################################################################################################
class person_telegram:
    import telepot
    bot = telepot.Bot('218094652:AAFbG7-_JTViC-wnZZ5VZC-uwvlNJ4EGx2w')
    #bot.getUpdates(offset=100000001)
    #bot.sendMessage(91686406, '<b>Good morning!</b>',parse_mode='HTML')


    def __init__(self,tlgid):
        self.tlgid = tlgid
        self.load_sent()
        self.bot.sendMessage(self.tlgid,'New version deployed')
        
        
    def load_sent(self):
        global sent_log
        try:
            sent_log = pd.read_csv('sent_log.csv')
        except:
            pass
    
    def has_sent(self,telegram_id,url):
        global sent_log
        if len(sent_log[(sent_log.telegram_id == telegram_id) & 
                        (sent_log.url == url)]) > 0 :
            return True
        return False
    
    def add_to_sent(self,telegram_id,url,msg):
        global sent_log
        sent_log = sent_log.append({'telegram_id': telegram_id,
                                    'url':url,
                                    'msg':msg  }, 
                                   ignore_index=True)
        
    
    def sendMessage(self,text,url):
        if not self.has_sent(self.tlgid,url):
            self.bot.sendMessage(self.tlgid,text,parse_mode='markdown')
            self.add_to_sent(self.tlgid,url,text)
    
    def update_saved_sent(self):
        global sent_log
        sent_log.to_csv('sent_log.csv', index=False)
########################################################################################################

kijiji_pattern = {'path':[['div',{'attrs':{'id':'mainPageContent'}}],
                                  ['div',{'attrs':{'class':'layout-3'}}],
                                  ['div',{'attrs':{'class':'col-2'}}]],
          'content_path':['div',{'attrs':{'class':'search-item regular-ad'}}],
           'message_shape':{'title' :       lambda x : '**'+re.sub('\s+',' ',x.find('a',class_='title').text)+'**',
                                          'price' :       lambda x : re.sub('\s','',x.find('div',class_='price').text),
                                          'image' :       lambda x : x.find('img')['data-src'],
                                          'distance' :    lambda x : re.sub('\s','',x.find('div',class_='distance').text),
                                          'url' :         lambda x :'kijiji.ca'+x.find('a',class_='title')['href'],
                                          'description' : lambda x : '_'+re.sub('\s+',' ',x.find('div',class_='description').text)+'_',
                                          }
          }
########################################################################################################
    
parviz_finder_kijiji_mountain_M = extractor( url = 'https://www.kijiji.ca/b-mountain-bike/ottawa/medium/c647l1700185a92?radius=10.0&ad=offering&price=150__500&minNumberOfImages=1&address=207+Bell+Street+North%2C+Ottawa%2C+ON&ll=45.406056,-75.705357',
                                     additional_filter='',
                                    **kijiji_pattern)

parviz_finder_kijiji_mountain_S = extractor( url = 'https://www.kijiji.ca/b-mountain-bike/ottawa/small/c647l1700185a92?radius=10.0&ad=offering&price=150__500&minNumberOfImages=1&address=207+Bell+Street+North%2C+Ottawa%2C+ON&ll=45.406056,-75.705357',
                                     additional_filter='',
                                    **kijiji_pattern)

parviz_finder_kijiji_hybrid_M = extractor(url = 'https://www.kijiji.ca/b-cruiser-commuter-hybrid/ottawa/medium/c15096001l1700185a92?radius=10.0&ad=offering&price=150__500&minNumberOfImages=1&address=207+Bell+Street+North%2C+Ottawa%2C+ON&ll=45.406056,-75.705357',
                                     additional_filter='',
                                    **kijiji_pattern)

parviz_finder_kijiji_hybrid_S = extractor(url = 'https://www.kijiji.ca/b-cruiser-commuter-hybrid/ottawa/small/c15096001l1700185a92?radius=10.0&ad=offering&price=150__500&minNumberOfImages=1&address=207+Bell+Street+North%2C+Ottawa%2C+ON&ll=45.406056,-75.705357',
                                     additional_filter='',
                                    **kijiji_pattern)

parviz_telegram = person_telegram(tlgid = 91686406)
#########################################################################################################

tasks=[{'source':parviz_finder_kijiji_mountain_M,
        'receiver':parviz_telegram},
       {'source':parviz_finder_kijiji_hybrid_M,
        'receiver':parviz_telegram},
{'source':parviz_finder_kijiji_mountain_S,
        'receiver':parviz_telegram},
       {'source':parviz_finder_kijiji_hybrid_S,
        'receiver':parviz_telegram}
       ]


#########################################################################################################
#########################################################################################################
#########################################################################################################
    
while 1:
    try:
        for t in tasks:
            t['source'].extract_url()
            t['source'].extract_path()
            t['source'].get_list()
            for post,url in t['source'].get_list_of_msg_url():
                t['receiver'].sendMessage(post,url)
            
            t['receiver'].update_saved_sent()
    
    
    except Exception  as e:
        raise e
        print('err_r',end='')

    time.sleep(1500)
