# GBCache [WIP]

Giga-Byte Cache.

## Basic Idea

In-memory key-value pair LRU cache on top of structured data stored on local disk.
> A giant fucking complicated hashmap

progress:
- [x] lru, in-memory read/writes
- [x] sync to disk
- [x] get, set, delete commands
- [x] increment command
- [x] server: make the damn thing
- [x] server: opt-in end-end AES256 encryption
- [x] TTL (more general, scheduled command runs, doesn't necessarily have to be a delete)
- [ ] rewrite it in Rust (ah shit, here we go again...)
- [ ] multi-threading (god save me, except there is no god, it's all lost & hopeless)
- [ ] WAL file & recovery (so you can get as kinky with your server as you want)
- [ ] pub/sub notifications for requested changes
- [ ] backups, cuz yes.

## Comments on Zig
- error handling, i think all the languages should adopt errors as values already
- explicitness, love it, no allocator? no heap!
- comptime, *chef's kiss*
- tests, it's actually convenient, just define a test block wherever you want!
- standard library, fucking A

I haven't found anything negative about Zig, yet.

> "then why are you rewriting it in Rust?"

I currently am finding out what REAL benefits rust provides by using it in another project,
if the benefits outweigh the cost of a rewrite and subsequent maintenance is easier in rust overall,
then I'll go through with it.
