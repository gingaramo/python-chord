python-chord
============

Python **Chord** implementation, [paper available here](http://pdos.csail.mit.edu/papers/chord:sigcomm01/chord_sigcomm.pdf).

Its base class is Local, located in chord.py, that provides the overlay network and
a lookup operation. Other commands such as 'get' or 'put' are provided by the top
layers using Chord by registering those commands. This provides enough flexibility
to add replication, re-distribution of keys/load, custom protocols, etc. in a simple
and easy way by means of 'agents' (will be implemented soon).

Currently supports concurrent addition of peers into the network and can handle node
failures / leave. Key lookup consistency test implemented in test.py, currently nodes 
distribute keys accordingly to node joins.

The behaviour can be greatly modified by setting the appropriate values on settings.py.

### How to test?
- `$>python test.py` to check consistency. Tests can fail due to the fact that the network is not stable yet (on consistency check), or that the new value was overriden by old value (on fusser test).
- `$>python create_chord.py $N_CHORD_NODES` to run a DHT that lets you ask questions to random members.

## Distributed File System

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