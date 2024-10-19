通过测试大概发现该测试无法通过的原因
~~1. 并发问题~~   
~~2. 活锁？~~
~~3. 并发问题~~
raft性能不佳
    - persist()
    - Snapshot()
    - InstallSnapshot()
    - lab3B频繁snapshot

不开启-race 25%
开启-race 90%
