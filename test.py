import os
import time
import socket
import random
from chord import *


def check_key_lookup(peers, hash_list):
	print "Running key lookup consistency test"
	for key in range(SIZE):
		# select random node
		node = peers[random.randrange(len(peers))]
		# get the successor
		target = node.find_successor(key)
		for i in range(len(peers)):
			if inrange(key, hash_list[i]+1, hash_list[(i+1)%len(peers)]+1):
				tries = 1
				while 1:
					try:
						assert target.id() == hash_list[(i+1)%len(peers)]
						break
					except Exception, e:
						print "Fail number %s, %s to abort" % (tries, 4-tries)
						tries += 1
						if tries > 4:
							raise e
						time.sleep(1.5 ** tries)
	print "Finished key lookup consistency test, all good"

"""
def data_fusser(peers):
	print "Running data fusser trying to detect failures"
	data = {}
	for i in range(1000):
		if random.random() < 0.4 and len(data.keys()):
			key = data.keys()[random.randrange(len(data.keys()))]
			tries = 0
			while 1:
				try:
					assert peers[random.randrange(len(peers))].get(key) == data[key]
					break
				except Exception, e:
					time.sleep(1<<tries)
					tries += 1
					if tries == 5:
						print "We are failing on run %i" % i
						print "Expected : '%s', got '%s'" % (data[key], peers[random.randrange(len(peers))].get(key))
						raise e
		else:
			key = str(random.randrange(1000))
			value = str(random.randrange(1000))
			data[key] = value
			peers[random.randrange(len(peers))].set(key, value)
	print "Finished running data fusser, all good"
"""

# create addresses
address_list = map(lambda addr: Address('127.0.0.1', addr), list(set(map(lambda x: random.randrange(40000,50000), range(10)))))
# keep unique ones
address_list = sorted(set(address_list))
# hash the addresses
hash_list 	 = map(lambda addr: addr.__hash__(), address_list)
hash_list.sort()
# create the nodes
locals_list   = []
for i in range(0, len(address_list)):
	try:
		if len(locals_list) == 0:
			local = Local(address_list[i])
		else:
			# use a random already created peer's address
			# as a remote
			local = Local(address_list[i], locals_list[random.randrange(len(locals_list))].address_)
	except socket.error: # socket bussy
		del hash_list[address_list[i].__hash__()]
	local.start()
	locals_list.append(local)
	time.sleep(0.1)

# We need to give it some time to stabilize
time.sleep(20)

print "done creating peers, our pid is %s (for `kill -9`)" % os.getpid()

# check key lookup consistency
check_key_lookup(locals_list, hash_list)

# check data consistency with fuzzer
#data_fusser(locals_list)

# shutdown peers
for local in locals_list:
	msocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	msocket.connect((local.address_.ip, local.address_.port))
	msocket.sendall('shutdown\r\n')
	msocket.close()