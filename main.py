#from crawler import Crawler
from mpi4py import MPI
from coordinator import Coordinator
from worker import Worker
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
proc = comm.Get_size()
status = MPI.Status()
#print(rank, flush=True)
#print(proc, flush=True)
coordinator_rank = 0
tag_map = 1
tag_reduce = 2
tag_end = 3
json_path = "backlinks.json"
if rank == coordinator_rank:
	#print("boss found", flush=True)
	coord = Coordinator(json_path, tag_map, tag_reduce, tag_end, rank, comm, status)
	#print("boss starts to work", flush=True)
	coord.do_job()
else:
	worker = Worker(json_path, tag_map, tag_reduce, tag_end, rank, comm, status)	
	worker.do_job()

MPI.Finalize()	

# local_folder = "crawler"
# queue = ["http://dmoztools.net"]
# user_agent = "*"
# crawler = Crawler(local_folder, queue, user_agent)
# crawler.process_queue()
#coord = Coordinator("backlinks.json")
#coord.reduce()