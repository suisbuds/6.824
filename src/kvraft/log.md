通过测试大概发现该测试无法通过的原因：客户端参数?
1. ~~数据竞态~~
2. 活锁？
3. 性能不佳，达不到测试要求的速度


Test: restarts, snapshots, many clients (3B) ...
2024/10/15 20:03:12 kv-RaftStateSize = 8017, kv.applyIndex = 9054
2024/10/15 20:03:12 kv-RaftStateSize = 8061, kv.applyIndex = 9054
2024/10/15 20:03:12 kv-RaftStateSize = 8105, kv.applyIndex = 9054
2024/10/15 20:03:12 kv-RaftStateSize = 8045, kv.applyIndex = 9056
2024/10/15 20:03:12 kv-RaftStateSize = 8149, kv.applyIndex = 9054
2024/10/15 20:03:12 kv-RaftStateSize = 8089, kv.applyIndex = 9056
2024/10/15 20:03:12 kv-RaftStateSize = 8193, kv.applyIndex = 9054
2024/10/15 20:03:12 kv-RaftStateSize = 8133, kv.applyIndex = 9056
2024/10/15 20:03:12 kv-RaftStateSize = 8237, kv.applyIndex = 9054
2024/10/15 20:03:12 kv-RaftStateSize = 8221, kv.applyIndex = 9056
2024/10/15 20:03:12 kv-RaftStateSize = 8325, kv.applyIndex = 9054
2024/10/15 20:03:12 kv-RaftStateSize = 8265, kv.applyIndex = 9056
2024/10/15 20:03:12 kv-RaftStateSize = 8369, kv.applyIndex = 9054
2024/10/15 20:03:12 kv-RaftStateSize = 8457, kv.applyIndex = 9054
2024/10/15 20:03:12 kv-RaftStateSize = 8353, kv.applyIndex = 9056
--- FAIL: TestSnapshotRecoverManyClients3B (12.91s)
    test_test.go:360: logs were not trimmed (8457 > 8*1000)
FAIL
exit status 1
FAIL    6.824/kvraft    12.925s