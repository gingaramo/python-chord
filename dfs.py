# data structure that represents a distributed hash table
class DFS(object):
	def __init__(self, local_address, remote_address = None):
		self.local_ = Local(local_address, remote_address)
				

	def get(self, key, remote = None):
		if not remote or not remote.ping():
			remote = self.local_.find_successor(hash(key))
		return remote.get(key), remote

	def set(self, key, value, remote = None):
		if not remote and not remote.ping():
			remote = self.local_.find_successor(hash(key))
		remote.set(key, value)


if __name__ == "__main__":
	import sys
	if len(sys.argv) == 2:
		dfs = Local(Address("127.0.0.1", sys.argv[1]))
	else:
		dfs = Local(Address("127.0.0.1", sys.argv[1]), Address("127.0.0.1", sys.argv[2]))
	
