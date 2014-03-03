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
		# use a random already created peer's address as a remote
		remote = locals_list[random.randrange(len(locals_list))].address_
		local = Local(address_list[i], remote)
	local.start()
	print "Created at %s"  % address_list[i]
	locals_list.append(local)
	time.sleep(0.5)

print "Done creating peers, our pid %s is" % os.getpid()

while 1:
	command = raw_input("Command: ")
	if command == "add_node":
		while 1:
			address = Address("127.0.0.1", random.randrange(10000, 60000))
			if not address.__hash__() in hash_list:
				ports_list.append(address.port)
				print "New node at port %s" % address.port
				address_list.append(address)
				locals_list.append(Local(address, locals_list[random.randrange(len(locals_list))].address))
				break
	else:
		address = address_list[random.randrange(len(address_list))]
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		s.connect((address.ip, address.port))
		s.sendall(command + "\r\n")
		print "Response : '%s'" % s.recv(10000)
		s.close()
