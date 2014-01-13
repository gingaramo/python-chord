from chord import Local, Daemon, repeat_and_sleep, inrange
from remote import Remote
from address import Address
import json

# data structure that represents a distributed hash table
class DHT(object):
	def __init__(self, local_address, remote_address = None):
		self.local_ = Local(local_address, remote_address)
		def set_wrap(msg):
			return self.set(msg)
		def get_wrap(msg):
			return self.get(msg)

		self.data_ = {}
		self.shutdown_ = False

		self.local_.register_command("_set", set_wrap)
		self.local_.register_command("_get", get_wrap)

		self.daemons_ = {}
		self.daemons_['distribute_data'] = Daemon(self, 'distribute_data')
		self.daemons_['distribute_data'].start()

		self.local_.start()

	def _get(self, key):
		try:
			return self.data_[key]
		except Exception:
			# not in our range
			suc = self.local_.find_successor(hash(key))
			if self.local_.id() == suc.id():
				return None
			return suc.command("get %s" % key)

	def _set(self, msg):
		self.data_[key] = value

	def get(self, key):
		return self._get(key)

	def set(self, key, value):
		key = msg.split(' ')[0]
		value = msg[len(key)+1:]
		self._set("%s %s" % (key, value))

	@repeat_and_sleep(5)
	def distribute_data(self):
		to_remove = []
		# to prevent from RTE in case data gets updated by other thread
		keys = self.data_.keys()
		for key in keys:
			if self.local_.predecessor() and \
			   not inrange(hash(key), self.local_.predecessor().id(1), self.local_.id(1)):
				try:
					node = self.local_.find_successor(hash(key))
					node.command("set %s %s" % (key, self.data_[key]))
					# print "moved %s into %s" % (key, node.id())
					to_remove.append(key)
					print "migrated"
				except socket.error:
					print "error migrating"
					# we'll migrate it next time
					pass
		# remove all we don't own or failed to move
		for key in to_remove:
			del self.data_[key]
		# Keep calling us
		return True

def create_dht(lport):
	laddress = map(lambda port: Address('127.0.0.1', port), lport)
	r = [DHT(laddress[0])]
	for address in laddress[1:]:
		r.append(DHT(address, laddress[0]))
	return r


if __name__ == "__main__":
	import sys
	if len(sys.argv) == 2:
		dht = DHT(Address("127.0.0.1", sys.argv[1]))
	else:
		dht = DHT(Address("127.0.0.1", sys.argv[1]), Address("127.0.0.1", sys.argv[2]))
	
