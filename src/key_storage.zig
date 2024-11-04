const std = @import("std");
const lib = @import("lib.zig");
const lru_cache = @import("lru.zig");
const fs_tracker = @import("fst.zig");

var gpa = std.heap.GeneralPurposeAllocator(.{}){};

pub const Key = struct {
    key_hash: u64,
    value_offset: u64,
    value_size: u64,

    pub fn is_zero(self: Key) bool {
        return self.value_size == 0 and self.value_offset == 0 and self.key_hash == 0;
    }
};

pub const Errors = error{
    KeyNotFound,
};
pub const StorageManager = struct {
    capacity: u64 = 100_000,
    max_bucket_size: u16 = 10,
    absolute_path: []const u8,
    bin_filename: []const u8 = "buckets.bin",
    fst: *fs_tracker.FreeSpaceTracker,

    _file: ?std.fs.File = null,
    _bucket_offset: u64 = 0,
    _mem_alloc: std.mem.Allocator = gpa.allocator(),
    _key_storage_path: ?[]const u8 = null,
    _lru: ?*lru_cache.LRUCache([]const u8, Key) = null,

    pub fn init(self: *StorageManager) !void {
        if (self._file) |file| {
            file.close();
            self._file = null;
        }

        const key_storage_path = try std.fs.path.join(self._mem_alloc, &[_][]const u8{ self.absolute_path, self.bin_filename });
        const result = try lib.fs.openOrCreateFileRW(key_storage_path);
        const lru = try self._mem_alloc.create(lru_cache.LRUCache([]const u8, Key));
        lru.* = lru_cache.LRUCache([]const u8, Key){
            .allocator = self._mem_alloc,
            .capacity = 10_000,
        };

        try lru.init();

        self._file = result.file;
        self._key_storage_path = key_storage_path;
        self._lru = lru;

        if (result.created) {
            const zero_key = Key{ .key_hash = 0, .value_offset = 0, .value_size = 0 };
            for (0..self.capacity * self.max_bucket_size) |_| {
                const b = std.mem.toBytes(zero_key);
                _ = try result.file.write(&b);
            }
            const total_b_written = try result.file.getPos();

            try result.file.sync();
            std.debug.assert(total_b_written == @sizeOf(Key) * self.max_bucket_size * self.capacity);
        }

        self._bucket_offset = @sizeOf(Key) * self.max_bucket_size;
    }

    pub fn deinit(self: *StorageManager) void {
        if (self._file) |file| {
            file.close();
            self._file = file;
        }
    }

    /// dynamically allocates a bucket buffer, caller is responsible for freeing the bucket buffer.
    fn _get_bucket_disk(self: *StorageManager, key_hash: u64) !struct { buf: []u8, offset: u64 } {
        const file = self._file.?;

        const index = key_hash % self.capacity;
        const offset = index * self._bucket_offset;
        try file.seekTo(offset);

        const bucket_buf = try self._mem_alloc.alloc(u8, self._bucket_offset);
        const b_read = try file.read(bucket_buf);
        std.debug.assert(b_read == bucket_buf.len);

        return .{ .buf = bucket_buf, .offset = offset };
    }

    fn _get_key_disk(self: *StorageManager, key_hash: u64) !?Key {
        const bucket = try self._get_bucket_disk(key_hash);
        defer self._mem_alloc.free(bucket.buf);

        for (0..self.max_bucket_size) |bucketI| {
            const start = bucketI * @sizeOf(Key);
            const key_buf = bucket.buf[start .. start + @sizeOf(Key)];
            const key = lib.ptrs.bufToType(Key, key_buf);

            if (key.key_hash == key_hash) {
                return key;
            }
        }

        return null;
    }

    /// asserts either `key.hash == key_hash` OR `key.is_zero()`
    /// assume this to be deletion operation of key at key_hash in case `key.is_zero()`
    fn _set_key_disk(self: *StorageManager, key_hash: u64, key: Key) !void {
        lib.assert(key_hash == key.key_hash or key.is_zero());

        const file = self._file.?;

        const bucket = try self._get_bucket_disk(key_hash);
        defer self._mem_alloc.free(bucket.buf);

        var write_key_offset: u64 = undefined;
        var _write_key_offset_set = false;
        for (0..self.max_bucket_size) |bucketI| {
            const start = bucketI * @sizeOf(Key);
            const key_buf = bucket.buf[start .. start + @sizeOf(Key)];
            const k = lib.ptrs.bufToType(Key, key_buf);

            if (key_hash == k.key_hash) {
                write_key_offset = bucket.offset + start;
                _write_key_offset_set = true;
                break;
            }

            if (!_write_key_offset_set and k.is_zero()) {
                write_key_offset = bucket.offset + start;
                _write_key_offset_set = true;
            }
        }

        if (!_write_key_offset_set) {
            std.debug.panic("we're so fucked, write key offset was not set, probably means bucket filled up (offset: {any})", .{bucket.offset});

            // TODO: emergency resize, and do some clever tricks to avoid downtime
        }

        try file.seekTo(write_key_offset);

        const key_buf = std.mem.toBytes(key);
        const b_write = try file.write(&key_buf);
        std.debug.assert(b_write == key_buf.len);

        try file.sync();
    }

    /// NOTE: I BEG YOU please ensure `key_hash = lru.gen_key_hash(key_str)`
    fn _get_key(self: *StorageManager, key_str: []const u8, key_hash: u64) !?Key {
        var lru = self._lru.?;

        if (lru.get(key_str)) |key_lru| {
            return key_lru;
        }

        var key: ?Key = null;
        const _key = try self._get_key_disk(key_hash);
        if (_key) |k| {
            try lru.set(key_str, k);
            key = k;
        }

        return key;
    }

    pub fn get(self: *StorageManager, key_str: []const u8) !?Key {
        const lru = self._lru.?;

        const key_hash = lru.gen_key_hash(key_str);
        const key = self._get_key(key_str, key_hash);

        return key;
    }

    pub fn set(self: *StorageManager, key_str: []const u8, value_metadata: lib.types.ValueMetadata) !void {
        var lru = self._lru.?;

        const key_hash = lru.gen_key_hash(key_str);
        const key = Key{
            .key_hash = key_hash,
            .value_offset = value_metadata.value_offset,
            .value_size = value_metadata.value_size,
        };

        // NOTE: please do not remove this line AS previous value from lru.set might not reflect the truth in DISK!
        const prev_k = try self._get_key(key_str, key_hash);
        try lru.set(key_str, key);
        if (prev_k) |k| {
            try self.fst.log_free_space(.{ .value_offset = k.value_offset, .value_size = k.value_size });
        }

        // TODO: spawn a thread to keep the disk updated
        // (batch updates would work, NOPE, RWLOCK on file almost-immediate syncing will, i mean should...)
        try self._set_key_disk(key_hash, key);
    }

    pub fn remove(self: *StorageManager, key_str: []const u8) !void {
        var lru = self._lru.?;

        const key_hash = lru.gen_key_hash(key_str);

        try self._set_key_disk(key_hash, Key{ .key_hash = 0, .value_size = 0, .value_offset = 0 });
        const r_key = lru.remove(key_str);

        if (r_key) |k| {
            try self.fst.log_free_space(.{ .value_offset = k.value_offset, .value_size = k.value_size });
        }
    }

    // pub fn set(self: KeyStorageManager, key: Key) !void {}
    // pub fn get(self: KeyStorageManager, key: Key) !void {}
};
