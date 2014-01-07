import os
import sys
import time
import socket
import random
from chord import *

nnodes = int(sys.argv[1])
print "Creating chord network with : %s nodes" % nnodes

# create ports
ports_list = [random.randrange(10000, 60000) for x in range(nnodes)]
print "Ports list : %s" % ports_list
# create addresses
address_list = map(lambda port: Address('127.0.0.1', port), ports_list)
# keep unique ones
address_list = sorted(set(address_list))
# hash the addresses
hash_list 	 = map(lambda addr: addr.__hash__(), address_list)
# create the nodes
locals_list   = []
for i in range(0, len(address_list)):
	if len(locals_list) == 0:
		local = Local(address_list[i])
	else:
		# use a random already created peer's address
		# as a remote
		local = Local(address_list[i], locals_list[random.randrange(len(locals_list))].address_)
	locals_list.append(local)
	time.sleep(0.5)

print "Done creating peers, our pid %s is" % os.getpid()
