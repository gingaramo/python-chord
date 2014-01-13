# CONFIGURATION FILE

# log size of the ring
LOGSIZE = 8
SIZE = 1<<LOGSIZE

# successors list size (to continue operating on node failures)
N_SUCCESSORS = 4

# INT = interval in seconds
# RET = retry limit

# Stabilize
STABILIZE_INT = 1
STABILIZE_RET = 4

# Fix Fingers
FIX_FINGERS_INT = 4

# Update Successors
UPDATE_SUCCESSORS_INT = 1
UPDATE_SUCCESSORS_RET = 6

# Find Successors
FIND_SUCCESSOR_RET = 3
FIND_PREDECESSOR_RET = 3
