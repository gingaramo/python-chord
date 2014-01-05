#!/bin/python
import json
import socket
import threading


# log size of the ring
LOGSIZE = 8
# size of the ring
SIZE = 1<<LOGSIZE

# helper function to determine if a key falls within a range
def inrange(c, a, b):
	"""
		is c in [a,b)?, if a == b then it assumes a full circle
		on the DHT, so it returns True.
	"""
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
		self.open_connection()
		ret = func(self, *args, **kwargs)
		self.close_connection()
		return ret
	return inner

# class representing a remote peer
class Remote(object):
	def __init__(self, remote_address):
		self.address_ = remote_address

	def open_connection(self):
		self.socket_ = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.socket_.connect((self.address_.ip, self.address_.port))

	def close_connection(self):
		self.socket_.close()
		self.socket_ = None

	def __str__(self):
		return "Remote %s" % self.address_

	def id(self):
		return self.address_.__hash__()

	def send(self, msg):
		self.socket_.sendall(msg)
#		print "send: %s <%s>" % (msg, self.address_)

	def recv(self):
		result = ""
		while 1:
			data = self.socket_.recv(256)
			if len(data) == 0:
				break
			result += data
		return json.loads(result)

	@requires_connection
	def get(self, key):
		self.send('get %s' % key)
		response = self.recv()

		return response['response']

	@requires_connection
	def set(self, key, value):
		self.send('set %s %s' % (key, value))
		
	@requires_connection
	def successor(self):
		self.send('get_successor')

		response = self.recv()
		return Remote(Address(response['ip'], response['port']))

	@requires_connection
	def predecessor(self):
		self.send('get_predecessor')

		response = self.recv()
		return Remote(Address(response['ip'], response['port']))

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
	def update_finger_table(self, address, i):
		self.send('update_finger_table %s %s %s' % (address.ip, address.port, i))

# deamon to run local's run method
class Daemon(threading.Thread):
	def __init__(self, local):
		threading.Thread.__init__(self)
		self.local_ = local

	def run(self):
		self.local_.run()

# class representing a local peer
class Local(object):
	def __init__(self, local_address, remote_address = None):
		self.address_ = local_address
		print "self id = %s" % self.id()
		# listen to incomming connections
		self.socket_ = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.socket_.bind((self.address_.ip, int(self.address_.port)))
		self.socket_.listen(100)

		self.data_ = {}

		if remote_address:
			remote = Remote(remote_address)
			self.finger_ = range(0, LOGSIZE)
			self.init_finger_table(remote)
			self.update_others()
		else:
			# first node on ring
			self.finger_ = list()
			for i in range(0, LOGSIZE):
				self.finger_.append(self)
			self.predecessor_ = self

		# start the daemon
		self.daemon_ = Daemon(self)
		self.daemon_.start()

	def __del__(self):
		self.socket_.close()

	def id(self):
		return self.address_.__hash__()

	def successor(self):
		return self.finger_[0]

	def find_successor(self, id):
		node = self.find_predecessor(id)
		return node.successor()

	def find_predecessor(self, id):
		node = self
		# we are alone in the ring
		if node.successor() == node:
			return node
		# we are not alone in the ring
		while not inrange(id, (node.id() + 1) % SIZE, (node.successor().id() + 1) % SIZE):
			node = node.closest_preceding_finger(id)
		return node

	def closest_preceding_finger(self, id):
		for i in range(LOGSIZE-1,-1,-1):
			if inrange(self.finger_[i].id(), self.id() + 1, id):
				return self.finger_[i]
		return self

	def init_finger_table(self, remote):
		self.finger_[0] = remote.find_successor((self.id() + 1) % SIZE)
		self.predecessor_ = self.finger_[0].predecessor()
		self.finger_[0].set_predecessor(self.address_)
		for i in range(1, LOGSIZE):
			if inrange(((self.id() + (1<<i)) % SIZE), self.id(), self.finger_[i-1].id()):
				self.finger_[i] = self.finger_[i-1]
			else:
				self.finger_[i] = remote.find_successor((self.id() + (1<<i)) % SIZE)

	def update_others(self):
		for i in range(0, LOGSIZE):
			previous = self.find_predecessor((self.id() - (1<<(i)) + 1 + SIZE) % SIZE)
			# not nessesary, but we avoid sending a message
			if previous != self:
				previous.update_finger_table(self.address_, i)

	def update_finger_table(self, remote, i):
		if inrange(remote.id(), (self.id() + (1<<i)) % SIZE, self.finger_[i].id() + 1):
			self.finger_[i] = remote
			# do not update if previous is predecesor (optimization)
			if self.predecessor_.address_.__hash__() != remote.address_.__hash__():
				self.predecessor_.update_finger_table(remote.address_, i)

	def get(self, key):
		try:
			return self.data_[key]
		except Exception:
			return None

	def set(self, key, value):
		self.data_[key] = value

	def run(self):
		while 1:
			conn, addr = self.socket_.accept()
			request = conn.recv(1024)
			# telnet connection
			if request[-2:] == "\r\n":
				request = request[:-2]

			print "recv: '%s' <%s>" % (request, addr)
			request = request.split(' ')
			if request[0] == 'get_successor':
				conn.sendall(json.dumps({'ip': self.successor().address_.ip,
										 'port': self.successor().address_.port}))
			if request[0] == 'get_predecessor':
				conn.sendall(json.dumps({'ip': self.predecessor_.address_.ip,
										 'port': self.predecessor_.address_.port}))

			if request[0] == 'find_successor':
				successor = self.find_successor(int(request[1]))
				conn.sendall(json.dumps({'ip': successor.address_.ip,
										 'port': successor.address_.port}))

			if request[0] == 'closest_preceding_finger':
				closest = self.closest_preceding_finger(int(request[1]))
				conn.sendall(json.dumps({'ip': closest.address_.ip,
										 'port': closest.address_.port}))
			if request[0] == 'update_finger_table':
				self.update_finger_table(Remote(Address(request[1], int(request[2]))), int(request[3]))
			if request[0] == 'set_predecessor':
				self.predecessor_ = Remote(Address(request[1], int(request[2])))
			if request[0] == 'get':
				conn.sendall(json.dumps({'response': self.get(request[1])}))
			if request[0] == 'set':
				self.set(request[1], request[2])
			if request[0] == 'print_finger_table':
				res = {}
				for i in range(0, LOGSIZE):
					res[i] = "%s" % self.finger_[i].address_
				conn.sendall(json.dumps(res))

			conn.close()

			if request[0] == 'shutdown':
				self.socket_.close()
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
