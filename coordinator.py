from mpi4py import MPI
import json
class Coordinator:
	def __init__(self, path_to_json, tag_map, tag_reduce, tag_end, rank, comm, status):
		self.comm = comm
		self.rank = rank
		self.proc = self.comm.Get_size()
		self.data = None
		self.path_to_json = path_to_json
		self.tag_map = tag_map
		self.tag_reduce = tag_reduce
		self.tag_end = tag_end
		self.status = status
		self.processed_files = []
	def read_json(self):
		with open(self.path_to_json) as json_file:
			self.data = json.load(json_file)
	def get_free_item(self):
		req = self.comm.recv(None,MPI.ANY_SOURCE, MPI.ANY_TAG, status=self.status)
		return self.status.Get_source()		
	def new_file_found(self, path):
		if path not in self.processed_files:
			return True
		return False
	def send_data(self,value,dest,tag):
		self.comm.send(value,dest,tag)	
	def do_job(self):
		self.read_json()
		self.map()
		self.reduce()
		for x in range(1,self.proc):
			self.send_data("end", x, self.tag_end)	
	def map(self):
		counter = 1
		next_rank = None
		if (self.rank == 0):
			for key in self.data.keys():
				if counter < self.proc:
					self.send_data(key, counter,self.tag_map)
					counter = counter+1
				else:
					next_rank = self.get_free_item()
					if type(next_rank) is int:
						self.send_data(key, next_rank, self.tag_map)	
	def reduce(self):
		counter = 1
		next_rank = None
		if (self.rank == 0):
			for key in self.data.keys():
				for value in self.data[key]:
					if self.new_file_found(value):
						if counter < self.proc:
							self.send_data(value, counter,self.tag_reduce)
							counter = counter + 1
						else:
							next_rank = self.get_free_item()
							if type(next_rank) is int: 
								self.send_data(value, next_rank,self.tag_reduce)
						self.processed_files.append(value)								