import os
import re
import urllib.request, urllib.error, urllib.parse
from urllib.parse import urlparse, urljoin 
from pyquery import PyQuery 
from collections import defaultdict
from robots import Robots
import json

class Crawler:
    def __init__(self, local_folder_name, url_list, user_agent):
        self.queue = url_list
        self.local_folder = local_folder_name
        self.robot_parser = Robots(user_agent)
        self.backlinks = {}

    def add_to_backlinks(self, link, reference):
        if link in self.backlinks.keys():
            self.backlinks[link].append(reference)
        else:
            self.backlinks[link] = list()                        
    def is_absolute(self, url):
        return bool(urlparse(url).netloc)

    def get_root(self, url):
        return urlparse(url).scheme +"://"+ urlparse(url).netloc    

    def process_queue(self):
        count = 0
        while self.queue and count < 2000:
            link = self.queue.pop(0)
            print(link)
            if not self.is_visited(link):
                count = count + 1
                try:
                    root = self.get_root(link)
                    if root != self.robot_parser.get_url():
                        self.robot_parser.set_url(root)
                        if self.robot_parser.check_robots_txt():
                            if self.robot_parser.can_parse_file(link):
                                html_page = urllib.request.urlopen(link).read().decode('utf-8')
                                if self.robot_parser.can_parse_page(html_page, "nofollow"):    
                                    self.save_page(html_page, link)
                                    self.parse_page(html_page, link)          
                except Exception as ex:
                    print('Exception occurred at file ' + link)
                    print(ex)
        print(self.backlinks)                        
        backlinks_file = open("backlinks.json", "w+")
        backlinks_file.write(json.dumps(self.backlinks))
        backlinks_file.close()                    

    def is_visited(self, link):
        if not os.path.exists(self.local_folder +"/"+ link.replace("://", "/")):
            return False
        return True    

    def parse_page(self, page, link):
        doc = PyQuery(page)
        hrefs = doc.find('a[href]')
        for href in hrefs:
            href_attr = PyQuery(href).attr("href")
            href_no_qs = href_attr.split("?")[0]
            if href_no_qs not in self.queue:
                if not self.is_absolute(href_attr):
                    self.queue.append(urljoin(link, href_no_qs))
                    self.add_to_backlinks(link, urljoin(link, href_no_qs))
                else:
                    self.queue.append(href_no_qs)
                    self.add_to_backlinks(link,href_no_qs) 

    def save_page(self, page, link):
        local_folder = self.local_folder
        path_folders = link.replace("://", "/").split("/")
        path_folders = [i for i in path_folders if i]
        actual_folder = local_folder+"/"
        for x in range(len(path_folders)-1):
            if len(path_folders[x]) > 0:
                actual_folder = actual_folder +"/"+path_folders[x]
                if not os.path.exists(actual_folder):
                    os.mkdir(actual_folder)     
        actual_folder =  actual_folder +"/"+ path_folders[len(path_folders)-1]       
        if not os.path.exists(actual_folder):                
            f = open(actual_folder+".html", "w+", encoding="utf-8")
            f.write(page)  
            f.close()  



