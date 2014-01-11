python-chord
============

Python **Chord** implementation, [paper available here](http://pdos.csail.mit.edu/papers/chord:sigcomm01/chord_sigcomm.pdf).

Currently supports concurrent addition of peers into the network and node failures/leave. Will implement better tests soon.

Some key lookup consistency test implemented in test.py, and nodes distribute keys accordingly to node joins. It presents high availability by allowing anyone to set
an arbitrary key on any node, that later gets sent to its rightful owner. Add
replication and lots of stuff and you have a highly available, eventually consistent
DHT and probably could implement partition tolerance.

The behaviour can be greatly modified, but I still don't have a easy way to do it
that's not modifying the code.

### How to test?
- `$>python test.py` to check consistency. Tests can fail due to the fact that the network is not stable yet (on consistency check), or that the new value was overriden by old value (on fusser test).
- `$>python create_chord.py $N_CHORD_NODES` to run a DHT that lets you ask questions to random members.

# Distributed File System

A distributed file sistem implemented on top of python-chord can be found in dfs.py.

### How to test?
- `$>python create_chord.py $N_CHORD_NODES`, followed by `$>python dfs.py 
$MOUNT_POINT`. Read description on dfs.py to know how to operate.

### What's next?

- Add replication!
- Adaptative load balance, based on [this paper](http://members.unine.ch/pascal.felber/publications/ICCCN-06.pdf).

**DISCLAIMER**
Pet project for fun to learn about DHT's, not intended to be used in real life.

Other projects:
 - SOON: C++ implementation of Raft concencus protocol.