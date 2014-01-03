import os
import socket
from peer import *

def check_finger_table_integrity(id, finger_table, hash_list):
	print "checking the finger table integrity of %s" % id
	for i in range(0, SIZE):
		finger_id = finger_table[i].id()
		# finger_id is in [id + 2^i, id)
		assert inrange(finger_id, (id + (1<<i)) % (1<<SIZE), id)
		for j in range(0, len(hash_list)):
			if (hash_list[j] == finger_id):
				continue
			# there's no other finger in between
			assert (id + (1<<i)) % (1<<SIZE) == finger_id or not inrange(hash_list[j], (id + (1<<i)) % (1<<SIZE), finger_id)

address_list = map(lambda addr: Address('127.0.0.1', addr), range(10100, 10200, 7))
hash_list 	 = map(lambda addr: addr.hash(), address_list)
peers_list   = []
for i in range(0, len(address_list)):
	if i == 0:
		peer = Peer(address_list[i])
	else:
		peer = Peer(address_list[i], address_list[i-1])
	peers_list.append(peer)
	peer.start()
print hash_list
print "done creating peers, test pid %s" % os.getpid()
hash_list.sort()
print hash_list


for peer in peers_list:
	check_finger_table_integrity(peer.local_.id(), peer.local_.finger_, hash_list)

for peer in peers_list:
	msocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	msocket.connect((peer.local_.address_.ip, peer.local_.address_.port))
	msocket.sendall('shutdown')
	msocket.close()

