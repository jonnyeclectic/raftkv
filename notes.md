1. Throttle the leader when it's too far ahead 
Raft doesn't have flow controls to cope with this. Might need to send an additional message to throttle the leader so Replica can catch up and commit.
2. Server doesn't know how far along it was when restarting
3. PreVote

