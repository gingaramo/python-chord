# read : file, offset, size -> status b64_data
# write : file, offset, size, buf
# truncate_size : file new_size
# attr : file -> dict

BLOCK_SIZE = 4096

# data structure that represents a distributed file system
class DFS(object):
	def __init__(self, local_address, remote_address = None):
		self.local_ = Local(local_address, remote_address)
		def read_wrap(msg):
			return self._read(msg)
		def write_wrap(msg):
			return self._write(msg)
		def attr_wrap(msg):
			return self._attr(msg)

		self.data_  = {}
		self.attr_  = {}

		self.shutdown_ = False

		self.local_.register_command("read", read_wrap)
		self.local_.register_command("write", write_wrap)
		self.local_.register_command("attr", write_wrap)

		self.local_.start()

    # helper function to eliminate duplicated code
    def get_offsets(self, offset, size):
        block_offset = offset / BLOCK_SIZE
        start = offset % BLOCK_SIZE
        end = min(start + size, BLOCK_SIZE)
        return (block_offset, start, end)

	def we_serve(self, file_name):
		pass
	def locate_remote(self, file_name):
		pass

	def _read(self, msg):

	def _write(self, msg):

	def _attr(self, msg):

	def read(self, file_name, offset, size):
		attr = self.attr(file_name)

		if offset > attr.size:
			return ""

		block_offset, start, end = self.get_offsets(offset, size)
		block_id = "%s:%s" % (file_name, block_offset)

		if not we_serve(block_id):
			return "-1"

		result = self.data_[block_id][start:end]
	def write(self, file, offset, size, buf):
		pass

	def attr(self, file):
		pass

if __name__ == "__main__":
	import sys
	if len(sys.argv) == 2:
		dfs = Local(Address("127.0.0.1", sys.argv[1]))
	else:
		dfs = Local(Address("127.0.0.1", sys.argv[1]), Address("127.0.0.1", sys.argv[2]))
	
