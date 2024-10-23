const std = @import("std");
const lib = @import("lib.zig");

var gpa = std.heap.GeneralPurposeAllocator(.{}){};

pub const Key = struct {
    key_hash: u64,
    value_offset: u64,
    value_size: u64,

    pub fn isZero(self: Key) bool {
        return self.value_size == 0 and self.value_offset == 0 and self.key_hash == 0;
    }
};

pub const Errors = error{
    KeyNotFound,
};
pub const StorageManager = struct {
    capacity: u64 = 1000,
    max_bucket_size: u16 = 15,
    absolute_path: []const u8,
    bin_filename: []const u8 = "buckets.bin",

    _file: ?std.fs.File = null,
    _bucket_offset: u64 = 0,
    _mem_alloc: std.mem.Allocator = gpa.allocator(),
    _key_storage_path: ?[]const u8 = null,

    pub fn init(self: *StorageManager) !void {
        if (self._file) |file| {
            file.close();
            self._file = null;
        }

        const key_storage_path = try std.fs.path.join(self._mem_alloc, &[_][]const u8{ self.absolute_path, self.bin_filename });
        const result = try lib.fs.openOrCreateFileRW(key_storage_path);
        self._file = result.file;
        self._key_storage_path = key_storage_path;

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

    fn get_bucket(self: *StorageManager, key_hash: u64) !struct { buf: []u8, offset: u64 } {
        const file = self._file.?;

        const index = key_hash % self.capacity;
        const offset = index * self._bucket_offset;
        try file.seekTo(offset);

        const bucket_buf = try self._mem_alloc.alloc(u8, self._bucket_offset);
        const b_read = try file.read(bucket_buf);
        std.debug.assert(b_read == bucket_buf.len);

        return .{ .buf = bucket_buf, .offset = offset };
    }

    pub fn get(self: *StorageManager, key_hash: u64) !Key {
        const bucket = try self.get_bucket(key_hash);
        defer self._mem_alloc.free(bucket.buf);

        for (0..self.max_bucket_size) |bucketI| {
            const start = bucketI * @sizeOf(Key);
            const key_buf = bucket.buf[start .. start + @sizeOf(Key)];
            const key = lib.ptrs.bufToStruct(Key, &key_buf);

            if (key.key_hash == key_hash) {
                std.debug.print("matched hash key_buf ({}): {any}\n", .{ bucket.offset, key_buf });
                std.debug.print("matched hash key: {any}\n", .{key});
                return key.*;
            }
        }

        return Key{ .key_hash = 0, .value_offset = 0, .value_size = 0 };
    }

    pub fn set(self: *StorageManager, key: Key) !void {
        const file = self._file.?;

        const bucket = try self.get_bucket(key.key_hash);
        defer self._mem_alloc.free(bucket.buf);

        var write_key_offset: u64 = undefined;
        var _write_key_offset_set = false;
        for (0..self.max_bucket_size) |bucketI| {
            const start = bucketI * @sizeOf(Key);
            const key_buf = bucket.buf[start .. start + @sizeOf(Key)];
            const k = lib.ptrs.bufToStruct(Key, &key_buf);

            if (k.key_hash == key.key_hash) {
                write_key_offset = bucket.offset + start;
                _write_key_offset_set = true;
                break;
            }

            if (!_write_key_offset_set and k.isZero()) {
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
        std.debug.print("key_buf: {any}\n", .{key_buf});
        const b_write = try file.write(&key_buf);
        std.debug.assert(b_write == key_buf.len);

        try file.sync();
    }

    pub fn deinit(self: *StorageManager) void {
        if (self._file) |file| {
            file.close();
            self._file = file;
        }
    }

    // pub fn set(self: KeyStorageManager, key: Key) !void {}
    // pub fn get(self: KeyStorageManager, key: Key) !void {}
};
