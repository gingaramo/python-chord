#!/bin/python
import json
import socket
import threading

def inrange(c, a, b):
	# c in [a,b)
	if a < b:
		return a <= c and c < b
	return a <= c or c < b

SIZE = 8

class Address(object):
	def __init__(self, ip, port):
		self.ip = ip
		self.port = int(port)

	def hash(self):
		return hash(("%s:%s" % (self.ip, self.port))) % (1<<SIZE)

	def __str__(self):
		return "<%s>:%s" % (self.ip, self.port)

class Remote(object):
	def __init__(self, remote_address):
		self.address_ = remote_address

	def start_connection(self):
		self.socket_ = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.socket_.connect((self.address_.ip, self.address_.port))

	def close_connection(self):
		self.socket_.close()
		self.socket_ = None

	def __str__(self):
		return "Remote %s" % self.address_

	def __unicode__(self):
		return "Remote %s" % self.address_

	def id(self):
		return self.address_.hash()

	def successor(self):
		self.start_connection()
		self.socket_.sendall('get_successor')

		response = json.loads(self.socket_.recv(1024))
		print "send: %s <%s> r = '%s'" % ('get_successor', self.address_, response)
		self.close_connection()
		return Remote(Address(response['ip'], response['port']))

	def predecessor(self):
		self.start_connection()
		self.socket_.sendall('get_predecessor')

		response = json.loads(self.socket_.recv(1024))
		print "send: %s <%s> r = '%s'" % ('get_predecessor', self.address_, response)
		self.close_connection()
		return Remote(Address(response['ip'], response['port']))

	def set_predecessor(self, address):
		self.start_connection()
		print "send: %s <%s>" % ('set_predecessor %s %s' % (address.ip, address.port), self.address_)
		self.socket_.sendall('set_predecessor %s %s' % (address.ip, address.port))
		self.close_connection()

	def find_successor(self, id):
		self.start_connection()
		self.socket_.sendall('find_successor %s' % id)
		data = self.socket_.recv(1024)

		response = json.loads(data)
		print "send: %s <%s> r = '%s'" % ('find_successor %s' % id, self.address_, response)
		self.close_connection()
		return Remote(Address(response['ip'], response['port']))

	def closest_preceding_finger(self, id):
		self.start_connection()
		self.socket_.sendall('closest_preceding_finger %s' % id)

		response = json.loads(self.socket_.recv(1024))
		print "send: %s <%s> r = '%s'" % ('closest_preceding_finger %s' % id, self.address_, response)
		self.close_connection()
		return Remote(Address(response['ip'], response['port']))

	def update_finger_table(self, address, i):
		self.start_connection()
		self.socket_.sendall('update_finger_table %s %s %s' % (address.ip, address.port, i))
		print "send: %s <%s>" % ('update_finger_table %s %s %s' % (address.ip, address.port, i), self.address_)
		self.close_connection()


class Peer(threading.Thread):
	def __init__(self, local_address, remote_address = None):
		threading.Thread.__init__(self)
		self.local_ = Local(local_address, remote_address)

	def run(self):
		self.local_.run()

class Local(object):
	def __init__(self, local_address, remote_address = None):
		self.address_ = local_address
		print "self id = %s" % self.id()
		# listen to incomming connections
		self.socket_ = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.socket_.bind((self.address_.ip, int(self.address_.port)))
		self.socket_.listen(100)

		if remote_address:
			remote = Remote(remote_address)
			self.finger_ = range(0, SIZE)
			self.init_finger_table(remote)
			self.update_others()
		else:
			self.finger_ = list()
			for i in range(0, SIZE):
				self.finger_.append(self)
			self.predecessor_ = self

	def __del__(self):
		self.socket_.close()

	def id(self):
		return self.address_.hash()

	def successor(self):
		return self.finger_[0]

	def find_successor(self, id):
		node = self.find_predecessor(id)
		return node.successor()

	def find_predecessor(self, id):
		node = self
		# we are alone
		if node.successor() == node:
			return node
		# we are not alone
		while not inrange(id, (node.id() + 1) % (1<<SIZE), (node.successor().id() + 1) % (1<<SIZE)):
			node = node.closest_preceding_finger(id)
		return node

	def closest_preceding_finger(self, id):
		for i in range(SIZE-1,-1,-1):
			# finger[i].node_id in (n, id)?
			if inrange(self.finger_[i].id(), self.id() + 1, id):
				return self.finger_[i]
		return self

	# join
	def init_finger_table(self, remote):
		self.finger_[0] = remote.find_successor((self.id() + 1) % (1<<SIZE))
		self.predecessor_ = self.finger_[0].predecessor()
		self.finger_[0].set_predecessor(self.address_)
		for i in range(1, SIZE):
			if inrange(((self.id() + (1<<i)) % (1<<SIZE)), self.id(), self.finger_[i-1].id()):
				self.finger_[i] = self.finger_[i-1]
			else:
				self.finger_[i] = remote.find_successor((self.id() + (1<<i)) % (1<<SIZE))

	def update_others(self):
		for i in range(0, SIZE):
			previous = self.find_predecessor((self.id() - (1<<(i)) + 1 + (1<<SIZE)) % (1<<SIZE))
			# not nessesary, but we avoid sending a message
			if previous != self:
				previous.update_finger_table(self.address_, i)

	def update_finger_table(self, remote, i):
		if inrange(remote.id(), (self.id() + (1<<i)) % (1<<SIZE), self.finger_[i].id() + 1):
			self.finger_[i] = remote
			# do not update if previous is predecesor (optimization)
			if self.predecessor_.address_.hash() != remote.address_.hash():
				self.predecessor_.update_finger_table(remote.address_, i)

	def run(self):
		while 1:
			conn, addr = self.socket_.accept()
			request = conn.recv(1024)
			print "recv: '%s' <%s>" % (request, addr)
			request = request.split(' ')
			if request[0] == 'get_successor':
				conn.sendall(json.dumps({'ip': self.successor().address_.ip,
										 'port': self.successor().address_.port}))
			if request[0] == 'get_predecessor':
				print json.dumps({'ip': self.predecessor_.address_.ip,
										 'port': self.predecessor_.address_.port})
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
				pass
			if request[0] == 'set':
				pass
			if request[0] == 'print_finger_table':
				res = {}
				for i in range(0, SIZE):
					res[i] = "%s" % self.finger_[i].address_
				conn.sendall(json.dumps(res))

			conn.close()

			if request[0] == 'shutdown':
				self.socket_.close()
				break

if __name__ == "__main__":
	import sys
	if len(sys.argv) == 2:
		local = Local(Address("127.0.0.1", sys.argv[1]))
	else:
		local = Local(Address("127.0.0.1", sys.argv[1]), Address("127.0.0.1", sys.argv[2]))
	print "self id = %s " % local.id()
	local.run()
