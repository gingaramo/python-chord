python-chord
============

Python **Chord** implementation, [paper available here](http://pdos.csail.mit.edu/papers/chord:sigcomm01/chord_sigcomm.pdf).

Currently supports concurrent addition of peers into the network.

Some key lookup consistency test implemented in test.py, and nodes distribute keys accordingly to node joins.

### What's next?

- Keep list of the r successors of a node to support node failure
- Implement some source of replication
- Build a decentralized and replicated user-space file system

**DISCLAIMER**
Pet project for fun to learn about DHT's, not intended to be used in real life.

Other projects:
 - Comming soon