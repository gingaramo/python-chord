#!/bin/python
import json
import socket
import threading
import random
import time
import mutex

# log size of the ring
LOGSIZE = 8
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
		self.socket_.sendall(msg)
		self.last_msg_send_ = msg
		# print "send: %s <%s>" % (msg, self.address_)

	def recv(self):
		result = ""
		while 1:
			data = self.socket_.recv(256)
			if len(data) == 0:
				break
			result += data
		try:
			return json.loads(result)
		except Exception as e:
			print "bad result:'" + result + "' for '"+ self.last_msg_send_ +"'"
			return None

	@requires_connection
	def get(self, key):
		self.send('get %s' % key)

		response = self.recv()
		return response['response']

	@requires_connection
	def set(self, key, value):
		self.send('set %s %s' % (key, value))

	@requires_connection
	def get_successors(self):
		self.send('get_successors')

		response = self.recv()
		# if our next guy doesn't have successors, return empty list
		if response == None:
			return []
		return map(lambda address: Remote(Address(address[0], address[1])) ,response[:N_SUCCESSORS-1])

	@requires_connection
	def successor(self):
		self.send('get_successor')

		response = self.recv()
		return Remote(Address(response['ip'], response['port']))

	@requires_connection
	def predecessor(self):
		self.send('get_predecessor')

		response = self.recv()
		if 'ip' in response and 'port' in response:
			return Remote(Address(response['ip'], response['port']))
		return None

	@requires_connection
	def find_successor(self, id):
		self.send('find_successor %s' % id)

		response = self.recv()
		return Remote(Address(response['ip'], response['port']))

	@requires_connection
	def set_predecessor(self, address):
		self.send('set_predecessor %s %s' % (address.ip, address.port))

	@requires_connection
	def closest_preceding_finger(self, id):
		self.send('closest_preceding_finger %s' % id)

		response = self.recv()
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

	def join(self, remote_address = None):
		# initially just set successor
		self.finger_ = list()
		for i in range(LOGSIZE):
			self.finger_.append(None)

		self.predecessor_ = None

		if remote_address:
			remote = Remote(remote_address)
			self.finger_[0] = remote.find_successor(self.id())
		else:
			self.finger_[0] = self

	def stabilize(self):
		while  1:
			time.sleep(random.random() * 0.5 + 0.1)
			if self.shutdown_:
				break

			# We may have found that x is our new successor iff
			# - x = pred(suc(n))
			# - x exists
			# - x is in range (n, suc(n))
			# - [n+1, suc(n)) is non-empty
			x = self.successor().predecessor()
			if x != None and \
			   inrange(x.id(), self.id(1), self.successor().id()) and \
			   self.id(1) != self.successor().id():
				self.finger_[0] = x
			# We notify our successor about us
			self.successor().notify(self)

	def notify(self, remote):
		# Someone thinks they are our predecessor, they are iff
		# - we don't have a predecessor
		# OR
		# - the new node r is in the range (pred(n), n)
		if self.predecessor() == None or \
		   inrange(remote.id(), self.predecessor().id(1), self.id()):
			self.predecessor_ = remote

	def fix_fingers(self):
		while 1:
			time.sleep(random.random() * 0.5 + 0.1)
			if self.shutdown_:
				break
			# Randomly select an entry in finger_ table and update its value
			i = random.randrange(LOGSIZE - 1) + 1
			self.finger_[i] = self.find_successor(self.id(1<<i))

	def distribute_data(self):
		while 1:
			time.sleep(random.random() * 0.5 + 0.5)
			if self.shutdown_:
				break
			# make sure that we own all the keys else
			to_remove = []
			for key in self.data_:
				if self.predecessor() and \
				   not inrange(hash(key), self.predecessor().id(1), self.id()):
				   node = self.find_successor(hash(key))
				   node.set(key, self.data_[key])
				   print "moved %s into %s" % (key, node.id())
				   to_remove.append(key)
			# remove all we don't own
			for key in to_remove:
				del self.data_[key]

	def update_successors(self):
		while 1:
			time.sleep(random.random() * 5 + 2)
			if self.shutdown_:
				break
			if self.successor().id() == self.id():
				continue

			successors = [self.successor()]
			suc_list = self.successor().get_successors()
			if suc_list:
				successors += suc_list
			self.successors_ = successors

	def get_successors(self):
		print self.successors_
		return map(lambda node: (node.address_.ip, node.address_.port), self.successors_)

	def id(self, offset = 0):
		return (self.address_.__hash__() + offset) % SIZE

	def successor(self):
		return self.finger_[0]

	def predecessor(self):
		return self.predecessor_

	def find_successor(self, id):
		# The successor of a key can be us iff
		# - we have a pred(n)
		# - id is in (pred(n), n]
		if self.predecessor() and \
		   inrange(id, self.predecessor().id(1), self.id(1)):
			return self
		node = self.find_predecessor(id)
		return node.successor()

	def find_predecessor(self, id):
		node = self
		# If we are alone in the ring, we are the pred(id)
		if node.successor() == node:
			return node
		# While id is not in (n, suc(n)] we are not alone in the ring
		while not inrange(id, node.id(1), node.successor().id(1)):
			node = node.closest_preceding_finger(id)
		return node

	def closest_preceding_finger(self, id):
		for i in range(LOGSIZE-1,-1,-1):
			if self.finger_[i] != None and inrange(self.finger_[i].id(), self.id(1), id):
				return self.finger_[i]
		return self

	def get(self, key):
		try:
			return self.data_[key]
		except Exception:
			return None

	def set(self, key, value):
		print "key %s with value %s set here" % (key, value)
		self.data_[key] = value

	def run(self):
		while 1:
			conn, addr = self.socket_.accept()
			request = conn.recv(1024)
			# telnet connection
			if request[-2:] == "\r\n":
				request = request[:-2]

			#print "recv: '%s' <%s>" % (request, addr)
			request = request.split(' ')
			if request[0] == 'get_successor':
				conn.sendall(json.dumps({'ip': self.successor().address_.ip,
										 'port': self.successor().address_.port}))
			if request[0] == 'get_predecessor':
				if self.predecessor_ != None:
					conn.sendall(json.dumps({'ip': self.predecessor_.address_.ip,
											 'port': self.predecessor_.address_.port}))
				else:
					conn.sendall(json.dumps({'response': -1}))
			if request[0] == 'find_successor':
				successor = self.find_successor(int(request[1]))
				conn.sendall(json.dumps({'ip': successor.address_.ip,
										 'port': successor.address_.port}))
			if request[0] == 'closest_preceding_finger':
				closest = self.closest_preceding_finger(int(request[1]))
				conn.sendall(json.dumps({'ip': closest.address_.ip,
										 'port': closest.address_.port}))
			if request[0] == 'get':
				conn.sendall(json.dumps({'response': self.get(request[1])}))
			if request[0] == 'set':
				self.set(request[1], request[2])
			if request[0] == 'notify':
				self.notify(Remote(Address(request[1], int(request[2]))))
			if request[0] == 'get_successors':
				conn.sendall(json.dumps(self.get_successors()))

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
