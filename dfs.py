# read : file, offset, size -> status b64_data
# write : file, offset, size, buf
# truncate_size : file new_size
# attr : file -> dict
import settings

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
		self.local_.register_command("attr", attr_wrap)

		self.local_.start()

    # helper function to eliminate duplicated code
    def get_offsets(self, offset, size):
        block_offset = offset / BLOCK_SIZE
        start = offset % BLOCK_SIZE
        end = min(start + size, BLOCK_SIZE)
        return (block_offset, start, end)

    def get_id(self, file_name, offset):
		block_offset, start, end = self.get_offsets(offset, 0)
    	return "%s:%s" % (file_name, block_offset)

	def get_hash(self, file_name, offset):
		return hash(self.get_id(file_name, offset)) % settings.SIZE

	def get_remote(self, file_name, offset):
		hs = self.get_hash(file_name, offset)
		suc = self.local_.find_successor(hs)
		return suc

	def _read(self, request):
		# request  = {'file_name':'my_file.txt', 'offset':<#NUMBER#>, 'size': <#NUMBER#>}
		# response = {'status':'failed'} |
		#			 {'status':'failed','code':<#CODE ERROR#>} |
		#			 {'status':'redirect'}
		# 			 {'status':'ok','data':<#DATA READ AS B64#>}
		try:
			data = json.loads(request)
			if not self.local_.is_ours(self.get_hash(data['file_name'], data['offset'])):
				return json.dumps({'status':'redirect'})
			# otherwise continue
			result = self.read(data['file_name'], data['offset'], data['size'])
			if type result == type -1:
				return json.dumps({'status':'failed', 'code': result})
			result = base64.b64encode(result)
			return json.dumpds({'status':'ok','data':total_read})

		except Exception:
			return json.dumps({'status':'failed'})

	def _write(self, request):
		# request  = {'file_name':'my_file.txt', 'offset':<#NUMBER#>, 'data':<#B64 ENCODED DATA#>}
		# response = {'status':'failed'} |
		#			 {'status':'failed','code':<#CODE ERROR#>} |
		#			 {'status':'redirect'}
		# 			 {'status':'ok','bytes':<#BYTES WROTE#>}
		try:
			data = json.loads(request)
			if not self.local_.is_ours(self.get_hash(data['file_name'], data['offset'])):
				return json.dumps({'status':'redirect'})
			result = self.write(data['file_name'], base64.b64decode(data['data']), offset)
			if result < 0:
				return json.dumps({'status':'failed','code':result})
			else:
				return json.dumps({'status':'ok','bytes':result})
		except Exception:
			return json.dumps({'status':'failed'})

	def _attr(self, request):
		# request  = {'file_name':'my_file.txt'[,'size':<#NEW VALUE#>|,'mode':<#NEW MODE#>]}
		# response = {'status':'failed'} |
		#			 {'status':'failed','code':<#CODE ERROR#>} |
		#			 {'status':'redirect'}
		try:
			data = json.loads(request)
			if not self.local_.is_ours(self.get_hash(data['file_name'], 0)):
				return json.dumps({'status':'redirect'})

		except Exception:
			return json.dumps({'status':'failed'})

	def trunc_(self, request):

	def read(self, path, size, offset):
		attr = self.attr(file_name)

		if offset > attr.size:
			return ""

		block_offset, start, end = self.get_offsets(offset, size)
		block_id = self.get_id(file_name, offset)


		result = self.data_[block_id][start:end]

	def write(self, path, buf, offset):
		pass

	def attr(self, path):
		pass

if __name__ == "__main__":
	import sys
	if len(sys.argv) == 2:
		dfs = Local(Address("127.0.0.1", sys.argv[1]))
	else:
		dfs = Local(Address("127.0.0.1", sys.argv[1]), Address("127.0.0.1", sys.argv[2]))
	
