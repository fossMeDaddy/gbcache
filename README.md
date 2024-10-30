# GBCache [WIP]

well, you can just call it Giga-Byte Cache because the actual expansion is too dirty, even for me.

## Basic Idea

In-memory key-value pair LRU cache on top of structured data stored on local disk.
> A giant fucking complicated hashmap

progress:
- [x] lru, in-memory read/writes
- [x] sync to disk
- [x] get, set, delete commands
- [ ] increment command
- [ ] TTL (more general, scheduled command runs, doesn't necessarily have to be a delete)
- [ ] multi-threading (god save me, except there is no god, it's all lost & hopeless)
- [ ] WAL file write & recovery for those of you with nastier kinks for your databases
- [ ] backups, cuz yes.

## Comments on Zig
- error handling, i think all the languages should adopt errors as values already
- explicitness, love it, no allocator? no heap!
- comptime, *chef's kiss*
- tests, it's actually convenient, just define a test block wherever you want!
- standard library, fucking A

I haven't found anything negative about Zig, yet.
