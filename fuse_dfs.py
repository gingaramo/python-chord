#!/bin/python

#
# usage: 
#  ./python create_chord.py $N_CHORDS
#  - take one of the pids (one of the list from the first line) and replace
#  - assign that number to PORT
#  ./python fuse_dfs.py <mountpoint>
# unmount with fusermount -u <mountpoint>
#

# Brief Summary
# =============
# This is a FS implemented on top of python-chord library. There are many things
# to experiment with (caching, adaptative load balancing, etc).
#
# I have not ran the POSIX test yet, but I did verify that the MD5 sum of the copy
# of a 30 MB file was the same.
#
# The structure is pretty simple, every item stored has the following fields:
# {'type':('directory', 'file'),
#  'data': ___
# }
#
# - In the case of directories, 'data' contains a dictionary with the keyword \
# 'files' that returns a list of files in the FS.
# - In the case of files, 'data' contains a 'base64_data' field with bytes encoded
# in base64.
#
#

import stat
import errno
import fuse
import socket
import dfs
from time import time
from subprocess import *

import chord
import json 
import base64

fuse.fuse_python_api = (0, 2)

# port from an chord node listening on <127.0.0.1:PORT>
PORT = 19308
BLOCK_SIZE = 4096


# default stat, not very useful
class MyStat(fuse.Stat):
    def __init__(self):
        self.st_mode = stat.S_IFDIR | 0755
        self.st_dev = 0
        self.st_ino = 0
        self.st_nlink = 1
        self.st_uid = 1000 # my uid
        self.st_gid = 1000 # my gid
        self.st_size = 4096
        self.st_atime = 0
        self.st_mtime = 0
        self.st_ctime = 0


# logging function
def log(info):
    f = open("/tmp/dfs.log", "a+")
    f.write(info + "\n")
    f.close()

# decorator to log every system call on our fs (strace equiv)
def logtofile(func):
    def inner(self, *args, **kwargs):
        f = open("/tmp/dfs.log", "a+")
        f.write("Function %s called with parameters %s %s\n" % (func.__name__,
                args, kwargs))
        f.close()
        return func(self, *args, **kwargs)
    return inner


class FUSEDFS(fuse.Fuse):
    def __init__(self, local, *args, **kw):
        fuse.Fuse.__init__(self, *args, **kw)
        self.local_ = local

    # helper function to eliminate duplicated code
    def get_offsets(self, offset, size):
        block_offset = offset / BLOCK_SIZE
        start = offset % BLOCK_SIZE
        end = min(start + size, BLOCK_SIZE)
        return (block_offset, start, end)

    # This is the guy responsible of making our FS visible to linux
    @logtofile
    def getattr(self, path):
        st = MyStat()

        # TODO: I need to add this metadata to the :0 block :/
        st.st_atime = int(time())
        st.st_mtime = st.st_atime
        st.st_ctime = st.st_atime

        # if we are asking for root, just get /, else, remove / and ask for block :0
        if path == '/':
            obj = get('/')
        else:
            obj = get("%s:0" % path[1:])

        # if nothing was returned, it can be 2 things, the file doesn't exist or
        # we need to bootstrap
        if obj == None:
            if path == '/':
                log("Creating empty root folder")
                obj = {'type':'directory', 'data':{'files':[]}}
                put(path, obj)
            else:
                log("File ' %s' doesn't exist" % path)
                return -errno.ENOENT

        if obj['type'] == 'file':
            # if it's a file, set the file flag and get size
            st.st_mode = stat.S_IFREG | 0666
            # we assume there's nothing bigger than 4G here. we do a binary search
            # to find the las block. This could be improved a lot with different
            # algorithms or ideas.
            left = 0
            right = (1<<32)/BLOCK_SIZE
            while left + 1 < right:
                mid = (left + right) / 2
                offsets = self.get_offsets(mid * BLOCK_SIZE, 1)
                key = "%s:%s" %(path[1:], offsets[0])
                block = get(key)
                if block != None:
                    left = mid
                else:
                    right = mid
            # the total size is the sum of previous blocks plus the data stored
            # at block 'left'
            key = "%s:%s" %(path[1:], left)
            block = get(key)
            size = left * BLOCK_SIZE + len(base64.b64decode(block['data']['b64_data']))

            st.st_size = size
        return st

    @logtofile
    def readdir(self, path, offset):
        files = [ "..", "." ]
        # right now we only support for '/', but this is general enough to support
        # folders in case we decide to implement mkdir
        directory = get(path)
        if directory != None and directory['type'] == 'directory':
            files.extend(directory['data']['files'])

        for r in files:
            yield fuse.Direntry(str(r))

    @logtofile
    def mknod(self, path, mode, dev):
        root = get('/')
        key = path[1:]
        # check if it exist, we shouldn't create an already existing file
        if key in root['data']['files']:
            return - 42
        # we are going to add it then!
        root['data']['files'].extend([key])
        put('/', root)

        # we only set the initial block
        key = "%s:0" % key
        obj = {'type': 'file',
               'data': { 'b64_data': base64.b64encode("") }
              }
        put(key, obj)

        # logging
        log("New node created '%s'" % key)

        return 0

    @logtofile
    def unlink(self, path):
        # not possible to remove files yet
        return -42

    @logtofile
    def read(self, path, size, offset):
        # we get rid of '/'
        path = path[1:]

        # first we make sure it exsist
        key = "%s:0" % path
        obj = get(key)
        if obj == None:
            return - errno.ENOENT

        # otherwise it exist, and we need to calculate the key for the current block
        block_offset, start, end = self.get_offsets(offset, size)
        key = "%s:%s" % (path, block_offset)

        # get the file block
        obj = get(key)
        # if it doesn't exist, just return 0, because it means we are at the end of
        # the file
        if obj == None:
            return 0

        # we read
        data = base64.b64decode(obj['data']['b64_data'])[start:end]

        return data

    @logtofile
    def write(self, path, buf, offset):
        # we get rid of '/'
        path = path[1:]

        # first we make sure it exsist
        key = "%s:0" % path
        obj = get(key)
        if obj == None:
            return - errno.ENOENT

        # otherwise it exist, and we need to calculate the key for the current block
        block_offset, start, end = self.get_offsets(offset, len(buf))

        key = "%s:%s" % (path, block_offset)

        # get the file block
        obj = get(key)

        # if it doesn't exist, just return create it
        if obj == None:
            obj = {'type':'file',
                   'data':{'b64_data': None}
            }

            # fill up with 0x00's before
            data = ("\00" * start) + buf[:end-start]
            obj['data']['b64_data'] = base64.b64encode(data)

        else:
            data = base64.b64decode(obj['data']['b64_data'])
            # this is the new data
            data = data[:start] + buf[:end-start] + data[end:]
            obj['data']['b64_data'] = base64.b64encode(data)


        # save into the DHT
        put(key, obj)
        return int(end-start)

    @logtofile
    def release(self, path, flags):
        return 0

    @logtofile
    def open(self, path, flags):
        return 0

    @logtofile
    def truncate(self, path, size):
        # we get rid of '/'
        path = path[1:]

        # first we make sure it exsist
        key = "%s:0" % path
        obj = get(key)
        if obj == None:
            return - errno.ENOENT

        # otherwise it exist, and we need to calculate the key for the current block
        block_offset, start, end = self.get_offsets(size, 0)
        key = "%s:%s" % (path, block_offset)

        # get the file block
        obj = get(key)

        # if it doesn't exist, just return 0
        if obj == None:
            return 0

        # if it does exist, truncate it
        data = base64.b64decode(obj['data']['b64_data'])
        obj['data']['b64_data'] = base64.b64encode(data[:end])


        put(key, obj)
        log("File %s truncated to %s" % (key, end))
        return 0

    @logtofile
    def utime(self, path, times):
        return 0

    @logtofile
    def mkdir(self, path, mode):
        return 0

    @logtofile
    def rmdir(self, path):
        return 0

    @logtofile
    def rename(self, pathfrom, pathto):
        return 0

    @logtofile
    def fsync(self, path, isfsyncfile):
        return 0

def main():
    usage="""
        FUSEDFS: A filesystem implemented on top of a DHT.
    """ + fuse.Fuse.fusage

    if len(sys.argv) == 2:
        local = Local(Address("127.0.0.1", sys.argv[1]))
    else:
        local = Local(Address("127.0.0.1", sys.argv[1]), Address("127.0.0.1", sys.argv[2]))

    local.start()
    server = FUSEDFS(local, version="%prog " + fuse.__version__,
                     usage=usage, dash_s_do='setsingle')
    server.parse(errex=1)
    server.main()

if __name__ == '__main__':
    main()