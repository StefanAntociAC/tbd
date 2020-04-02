import urllib.robotparser, urllib.request, urllib.parse
from urllib.parse import urljoin
from pyquery import PyQuery 
class Robots:
	def __init__(self, user_agent):
		self.robot_parser = urllib.robotparser.RobotFileParser()
		self.user_agent = user_agent
		self.url = ""
		self.has_robots_txt = False

	def get_url(self):
		return self.url
	def set_url(self, url):
		self.url = url
	def parse_robots_txt(self, file_content):
			robot = self.robot_parser.parse(file_content)
			return robot
	def check_robots_txt(self):
		try:
			robot_txt = urllib.request.urlopen(urljoin(self.url, "robots.txt")).read().decode('utf-8')
			self.parse_robots_txt(robot_txt)
			self.has_robots_txt = True
			return self.has_robots_txt
		except:
			self.has_robots_txt = False
			return self.has_robots_txt

	def can_parse_file(self, url):
		return self.robot_parser.can_fetch(self.user_agent, url)
	def can_parse_page(self, page, rule):
		doc = PyQuery(str(page))
		robot_rules = doc.find('meta[name="robots"]')
		for rule in robot_rules:
			content = PyQuery(rule).attr("content")
			if str(rule) in content:
				return False
		return True
	def can_parse(self, url, page, rule):
		if self.can_parse_file(url) and self.can_parse_page(page, rule):
			return True
		else:
			return False    		
			
