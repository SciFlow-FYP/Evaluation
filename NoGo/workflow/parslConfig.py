import parsl

###################=============CONFIGURATION ===================###############

from configs.local_threads import local_threads
#from configs.local_htex import local_htex
#from configs.remote_htex import remote_htex

parsl.load(local_threads)
#parsl.load(local_htex)
#parsl.load(remote_htex)

