from crawler import Crawler
import os
local_folder = "crawler"
os.mkdir(local_folder)
queue = ["http://dmoztools.net"]
user_agent = "*"
crawler = Crawler(local_folder, queue, user_agent)
crawler.process_queue()
