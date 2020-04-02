from mpi4py import MPI
import json
class Worker:
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
		self.reverse_index = {}
		self.mapped_data = []
	def read_json(self):
		with open(self.path_to_json) as json_file:
			self.data = json.load(json_file)
	def get_key_from_reverse_index(self, key):
		if key not in self.reverse_index.keys():
			self.reverse_index[key] = []
		return self.reverse_index[key]	
	def ack_coord_im_free(self):
		self.comm.send("done", 0, 4)
	def write_in_file(self, general_name, data):
		file = open(general_name+str(self.rank)+".json", "w+" )
		file.write(json.dumps(data))
		file.close()
	def do_job(self):
		self.read_json()
		while True:
			data = self.comm.recv(None, 0, MPI.ANY_TAG, status=self.status)
			tag = self.status.Get_tag()
			if tag == self.tag_map:
				self.map(data)
				self.ack_coord_im_free()
			elif tag == self.tag_reduce:
				self.reduce(data)	
				self.ack_coord_im_free()
			elif tag == self.tag_end:
				break
		self.write_in_file("mapped_data_rank", self.mapped_data)		
		self.write_in_file("reverse_index_rank", self.reverse_index)			
	def map(self, key):
		if key in self.data.keys():
			for value in self.data[key]:
				self.mapped_data.append( (value, key) )	
	def reduce(self, key):
		for item in self.mapped_data:
			if item[0] == key:
				self.create_reverse_index(item)		
	def create_reverse_index(self,element):
		if element[0] not in self.reverse_index.keys():
			self.reverse_index[element[0]] = list()
		self.reverse_index[element[0]].append(element[1])				


