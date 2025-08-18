# TODOs (beyond inline comments)

- [X] (minor) set the rocksdb setting that helps with prefix scans
- [ ] (minor) make newtype safety stuff for db keys
- [X] (minor) proper command setup (cli args, etc)
- [X] (minor) ci
- [ ] (minor) rocksdb settings tuning
- [ ] (major) server concurrency (either normally or via partitioning)
- [ ] (major) server/storage partitioning?
- [ ] (major) move storage to an io thread pool
- [ ] (feat) implement lease expiry
- [X] (feat) implement poll in server
- [ ] (minor) add polling timeout and num_items options
- [ ] (major) fix race condition: concurrent polls can hand out the same messages
- [ ] (feat) implement remove
- [ ] (feat) implement extend
