python-chord
============

Python implementation of [this paper](http://pdos.csail.mit.edu/papers/chord:sigcomm01/chord_sigcomm.pdf).

Its base class is `Local`, located in chord.py. It provides the overlay network and
a lookup operation. Other commands such as 'get' or 'put' are provided by the top
layers using Chord by registering those commands (take a look at dht.py). This provides
enough flexibility to add replication, re-distribution of keys/load, custom protocols, 
etc.

Currently supports concurrent addition of peers into the network and can handle node
failures / leave. Key lookup consistency test implemented in test.py.

The behaviour of the network can be greatly modified by setting the appropriate values 
on `settings.py`.

### How to test?
- `$>python test.py` to check consistency. Tests can fail due to the fact that the network is not stable yet, should work by increasing the rate of updates.
- `$>python create_chord.py $N_CHORD_NODES` to run a DHT that lets you ask questions to random members.

## Distributed Hash Table
A distributed hash table implementation on top of Chord is available in `dht.py`. It 
uses the overlay network provided by Chord's algorithms and adds two more commands to
the network, the commands `set` and `get`.

After registering those commands with the appropriate callbacks we have a fairly 
simple DHT implementation that also balances loads according to node joins.

### To be implemented:
- Replication to handle node failures/departures without losing information.

## Distributed File System
For this case we implemented a file system ... (to be continued)

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