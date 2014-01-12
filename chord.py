#!/bin/python
import os
import json
import socket
import threading
import random
import time
import mutex

from address import Address, inrange
from remote import Remote
from settings import *
from network import *

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

# deamon to run Local's run method
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

	@repeat_and_sleep(STABILIZE_INT)
	@retry_on_socket_error(STABILIZE_RET)
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

	@repeat_and_sleep(FIX_FINGERS_INT)
	def fix_fingers(self):
		# Randomly select an entry in finger_ table and update its value
		i = random.randrange(LOGSIZE - 1) + 1
		self.finger_[i] = self.find_successor(self.id(1<<i))
		# Keep calling us
		return True

	@repeat_and_sleep(DISTRIBUTE_DATA_INT)
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

	@repeat_and_sleep(UPDATE_SUCCESSORS_INT)
	@retry_on_socket_error(UPDATE_SUCCESSORS_RET)
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

	@retry_on_socket_error(FIND_SUCCESSOR_RET)
	def find_successor(self, id):
		# The successor of a key can be us iff
		# - we have a pred(n)
		# - id is in (pred(n), n]
		if self.predecessor() and \
		   inrange(id, self.predecessor().id(1), self.id(1)):
			return self
		node = self.find_predecessor(id)
		return node.successor()

	@retry_on_socket_error(FIND_PREDECESSOR_RET)
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

			request = read_from_socket(conn)
			command = request.split(' ')[0]

			if command == 'get_successor':
				conn.sendall(json.dumps({'ip': self.successor().address_.ip,
										 'port': self.successor().address_.port}) +
							 "\r\n")
			if command == 'get_predecessor':
				if self.predecessor_ != None:
					conn.sendall(json.dumps({'ip': self.predecessor_.address_.ip,
											 'port': self.predecessor_.address_.port})+
							 "\r\n")
				else:
					conn.sendall(json.dumps("")+
							 "\r\n")
			if command == 'find_successor':
				successor = self.find_successor(int(request[1]))
				conn.sendall(json.dumps({'ip': successor.address_.ip,
										 'port': successor.address_.port})+
							 "\r\n")
			if command == 'closest_preceding_finger':
				closest = self.closest_preceding_finger(int(request[1]))
				conn.sendall(json.dumps({'ip': closest.address_.ip,
										 'port': closest.address_.port})+
							 "\r\n")
			if command == 'notify':
				self.notify(Remote(Address(request[1], int(request[2]))))
			if command == 'get_successors':
				conn.sendall(json.dumps(self.get_successors())+
							 "\r\n")

			if command == 'get':
				conn.sendall(self.get(request[1])+
							 "\r\n")
			if command == 'set':
				self.set(request[1], " ".join(request[2:]))

			conn.close()

			if request[0] == 'shutdown':
				self.socket_.close()
				self.shutdown_ = True
				break

if __name__ == "__main__":
	import sys
	if len(sys.argv) == 2:
		local = Local(Address("127.0.0.1", sys.argv[1]))
	else:
		local = Local(Address("127.0.0.1", sys.argv[1]), Address("127.0.0.1", sys.argv[2]))
	local.run()
