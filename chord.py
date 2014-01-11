#!/bin/python
import os
import json
import socket
import threading
import random
import time
import mutex

# log size of the ring
LOGSIZE = 15
# size of the ring
SIZE = 1<<LOGSIZE
# successors list size
N_SUCCESSORS = 4

# Helper function to determine if a key falls within a range
def inrange(c, a, b):
	# is c in [a,b)?, if a == b then it assumes a full circle
	# on the DHT, so it returns True.
	a = a % SIZE
	b = b % SIZE
	c = c % SIZE
	if a < b:
		return a <= c and c < b
	return a <= c or c < b

class Address(object):
	def __init__(self, ip, port):
		self.ip = ip
		self.port = int(port)

	def __hash__(self):
		return hash(("%s:%s" % (self.ip, self.port))) % SIZE

	def __cmp__(self, other):
		return other.__hash__() < self.__hash__()

	def __eq__(self, other):
		return other.__hash__() == self.__hash__()

	def __str__(self):
		return "<%s>:%s" % (self.ip, self.port)

# decorator
def requires_connection(func):
	""" initiates and cleans up connections with remote server """
	def inner(self, *args, **kwargs):
		self.mutex_.acquire()

		self.open_connection()
		ret = func(self, *args, **kwargs)
		self.close_connection()
		self.mutex_.release()

		return ret
	return inner

# reads from socket until "\r\n"
def read_from_socket(s):
	result = ""
	while 1:
		data = s.recv(256)
		if data[-2:] == "\r\n":
			result += data[:-2]
			break
		result += data
	return result

# class representing a remote peer
class Remote(object):
	def __init__(self, remote_address):
		self.address_ = remote_address
		self.mutex_ = threading.Lock()

	def open_connection(self):
		self.socket_ = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.socket_.connect((self.address_.ip, self.address_.port))

	def close_connection(self):
		self.socket_.close()
		self.socket_ = None

	def __str__(self):
		return "Remote %s" % self.address_

	def id(self, offset = 0):
		return (self.address_.__hash__() + offset) % SIZE

	def send(self, msg):
		self.socket_.sendall(msg + "\r\n")
		self.last_msg_send_ = msg
		# print "send: %s <%s>" % (msg, self.address_)

	def recv(self):
		# we use to have more complicated logic here
		# and we might have again, so I'm not getting rid of this yet
		return read_from_socket(self.socket_)

	def ping(self):
		try:
			s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			s.connect((self.address_.ip, self.address_.port))
			s.sendall("\r\n")
			s.close()
			return True
		except socket.error:
			return False

	@requires_connection
	def get(self, key):
		self.send('get %s' % key)

		response = self.recv()
		return response

	@requires_connection
	def set(self, key, value):
		self.send('set %s %s' % (key, value))

	@requires_connection
	def get_successors(self):
		self.send('get_successors')

		response = self.recv()
		# if our next guy doesn't have successors, return empty list
		if response == "":
			return []
		response = json.loads(response)
		return map(lambda address: Remote(Address(address[0], address[1])) ,response)

	@requires_connection
	def successor(self):
		self.send('get_successor')

		response = json.loads(self.recv())
		return Remote(Address(response['ip'], response['port']))

	@requires_connection
	def predecessor(self):
		self.send('get_predecessor')

		response = self.recv()
		if response == "":
			return None
		response = json.loads(response)
		return Remote(Address(response['ip'], response['port']))

	@requires_connection
	def find_successor(self, id):
		self.send('find_successor %s' % id)

		response = json.loads(self.recv())
		return Remote(Address(response['ip'], response['port']))

	@requires_connection
	def closest_preceding_finger(self, id):
		self.send('closest_preceding_finger %s' % id)

		response = json.loads(self.recv())
		return Remote(Address(response['ip'], response['port']))

	@requires_connection
	def notify(self, node):
		self.send('notify %s %s' % (node.address_.ip, node.address_.port))


# deamon to run local's run method
class Daemon(threading.Thread):
	def __init__(self, obj, method):
		threading.Thread.__init__(self)
		self.obj_ = obj
		self.method_ = method

	def run(self):
		getattr(self.obj_, self.method_)()

def repeat_and_sleep(sleep_time):
	def decorator(func):
		def inner(self, *args, **kwargs):
			while 1:
				time.sleep(sleep_time)
				if self.shutdown_:
					return
				ret = func(self, *args, **kwargs)
				if not ret:
					return
		return inner
	return decorator

def retry_on_socket_error(retry_limit):
	def decorator(func):
		def inner(self, *args, **kwargs):
			retry_count = 0
			while retry_count < retry_limit:
				try:
					ret = func(self, *args, **kwargs)
					return ret
				except socket.error:
					# exp retry time
					time.sleep(2 ** retry_count)
					retry_count += 1
			if retry_count == retry_limit:
				print "Retry count limit reached, aborting.."
				self.shutdown_ = True
				os.exit(-1)
		return inner
	return decorator

# class representing a local peer
class Local(object):
	def __init__(self, local_address, remote_address = None):
		self.address_ = local_address
		print "self id = %s" % self.id()
		# listen to incomming connections
		self.socket_ = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.socket_.bind((self.address_.ip, int(self.address_.port)))
		self.socket_.listen(10)
		self.shutdown_ = False

		# set data to empty dictionary
		self.data_ = {}

		# list of successors
		self.successors_ = []

		# join the DHT
		self.join(remote_address)

		# start the daemons
		self.daemons_ = {}
		self.daemons_['run'] = Daemon(self, 'run')
		self.daemons_['fix_fingers'] = Daemon(self, 'fix_fingers')
		self.daemons_['stabilize'] = Daemon(self, 'stabilize')
		self.daemons_['distribute_data'] = Daemon(self, 'distribute_data')
		self.daemons_['update_successors'] = Daemon(self, 'update_successors')
		for key in self.daemons_:
			self.daemons_[key].start()

	def __del__(self):
		if self.socket_:
			self.socket_.close()

	def ping(self):
		return True

	def join(self, remote_address = None):
		# initially just set successor
		self.finger_ = map(lambda x: None, range(LOGSIZE))

		self.predecessor_ = None

		if remote_address:
			remote = Remote(remote_address)
			self.finger_[0] = remote.find_successor(self.id())
		else:
			self.finger_[0] = self

	@repeat_and_sleep(1)
	@retry_on_socket_error(4)
	def stabilize(self):
		suc = self.successor()
		# We may have found that x is our new successor iff
		# - x = pred(suc(n))
		# - x exists
		# - x is in range (n, suc(n))
		# - [n+1, suc(n)) is non-empty
		# fix finger_[0] if successor failed
		if suc.id() != self.finger_[0].id():
			self.finger_[0] = suc
		x = suc.predecessor()
		if x != None and \
		   inrange(x.id(), self.id(1), suc.id()) and \
		   self.id(1) != suc.id() and \
		   x.ping():
			self.finger_[0] = x
		# We notify our new successor about us
		self.successor().notify(self)
		# Keep calling us
		return True

	def notify(self, remote):
		# Someone thinks they are our predecessor, they are iff
		# - we don't have a predecessor
		# OR
		# - the new node r is in the range (pred(n), n)
		# OR
		# - our previous predecessor is dead
		if self.predecessor() == None or \
		   inrange(remote.id(), self.predecessor().id(1), self.id()) or \
		   not self.predecessor().ping():
			self.predecessor_ = remote

	@repeat_and_sleep(4)
	def fix_fingers(self):
		# Randomly select an entry in finger_ table and update its value
		i = random.randrange(LOGSIZE - 1) + 1
		self.finger_[i] = self.find_successor(self.id(1<<i))
		# Keep calling us
		return True

	@repeat_and_sleep(30)
	def distribute_data(self):
		# make sure that we own all the keys else
		to_remove = []
		# to prevent from RE in case data gets updated by other thread
		keys = self.data_.keys()
		for key in keys:
			if self.predecessor() and \
			   not inrange(hash(key), self.predecessor().id(1), self.id(1)):
				try:
					node = self.find_successor(hash(key))
					node.set(key, self.data_[key])
					# print "moved %s into %s" % (key, node.id())
					to_remove.append(key)
				except socket.error:
					# we'll migrate it next time
					pass
		# remove all we don't own or failed to move
		for key in to_remove:
			del self.data_[key]
		# Keep calling us
		return True

	@repeat_and_sleep(5)
	@retry_on_socket_error(6)
	def update_successors(self):
		suc = self.successor()
		# if we are not alone in the ring, calculate
		if suc.id() != self.id():
			successors = [suc]
			suc_list = suc.get_successors()
			if suc_list and len(suc_list):
				successors += suc_list
			# if everything worked, we update
			self.successors_ = successors
		return True

	def get_successors(self):
		return map(lambda node: (node.address_.ip, node.address_.port), self.successors_[:N_SUCCESSORS-1])

	def id(self, offset = 0):
		return (self.address_.__hash__() + offset) % SIZE

	def successor(self):
		# We make sure to return an existing successor, there `might`
		# be redundance between finger_[0] and successors_[0], but
		# it doesn't harm
		for remote in [self.finger_[0]] + self.successors_:
			if remote.ping():
				return remote
		print "No successor available, aborting"
		self.shutdown_ = True
		os.exit(-1)

	def predecessor(self):
		return self.predecessor_

	@retry_on_socket_error(3)
	def find_successor(self, id):
		# The successor of a key can be us iff
		# - we have a pred(n)
		# - id is in (pred(n), n]
		if self.predecessor() and \
		   inrange(id, self.predecessor().id(1), self.id(1)):
			return self
		node = self.find_predecessor(id)
		return node.successor()

	@retry_on_socket_error(3)
	def find_predecessor(self, id):
		node = self
		# If we are alone in the ring, we are the pred(id)
		if node.successor().id() == node.id():
			return node
		# While id is not in (n, suc(n)] we are not alone in the ring
		while not inrange(id, node.id(1), node.successor().id(1)):
			node = node.closest_preceding_finger(id)
		return node

	def closest_preceding_finger(self, id):
		# first fingers in decreasing distance, then successors in
		# increasing distance.
		for remote in reversed(self.successors_ + self.finger_):
			if remote != None and inrange(remote.id(), self.id(1), id) and remote.ping():
				return remote
		return self

	@retry_on_socket_error(3)
	def get(self, key):
		try:
			return self.data_[key]
		except Exception:
			# not in our range
			if not inrange(hash(key) % SIZE, self.predecessor().id(1), self.id(1)):
				return self.find_successor(hash(key)).get(key)
			return ""

	def set(self, key, value):
		# print "key %s with value %s set here" % (key, value)
		self.data_[key] = value

	def run(self):
		while 1:
			try:
				conn, addr = self.socket_.accept()
			except socket.error:
				self.shutdown_ = True
				break

			# split by spaces, we might have to join later though
			request = read_from_socket(conn).split(' ')

			if request[0] == 'get_successor':
				conn.sendall(json.dumps({'ip': self.successor().address_.ip,
										 'port': self.successor().address_.port}) +
							 "\r\n")
			if request[0] == 'get_predecessor':
				if self.predecessor_ != None:
					conn.sendall(json.dumps({'ip': self.predecessor_.address_.ip,
											 'port': self.predecessor_.address_.port})+
							 "\r\n")
				else:
					conn.sendall(json.dumps("")+
							 "\r\n")
			if request[0] == 'find_successor':
				successor = self.find_successor(int(request[1]))
				conn.sendall(json.dumps({'ip': successor.address_.ip,
										 'port': successor.address_.port})+
							 "\r\n")
			if request[0] == 'closest_preceding_finger':
				closest = self.closest_preceding_finger(int(request[1]))
				conn.sendall(json.dumps({'ip': closest.address_.ip,
										 'port': closest.address_.port})+
							 "\r\n")
			if request[0] == 'get':
				conn.sendall(self.get(request[1])+
							 "\r\n")
			if request[0] == 'set':
				self.set(request[1], " ".join(request[2:]))
			if request[0] == 'notify':
				self.notify(Remote(Address(request[1], int(request[2]))))
			if request[0] == 'get_successors':
				conn.sendall(json.dumps(self.get_successors())+
							 "\r\n")

			conn.close()

			if request[0] == 'shutdown':
				self.socket_.close()
				self.shutdown_ = True
				break

# data structure that represents a distributed hash table
class DHT(threading.Thread):
	def __init__(self, local_address, remote_address = None):
		self.local_ = Local(local_address, remote_address)

	def get(self, key):
		remote = self.local_.find_successor(hash(key))
		return remote.get(key)

	def set(self, key, value):
		remote = self.local_.find_successor(hash(key))
		remote.set(key, value)

if __name__ == "__main__":
	import sys
	if len(sys.argv) == 2:
		local = Local(Address("127.0.0.1", sys.argv[1]))
	else:
		local = Local(Address("127.0.0.1", sys.argv[1]), Address("127.0.0.1", sys.argv[2]))
	local.run()
