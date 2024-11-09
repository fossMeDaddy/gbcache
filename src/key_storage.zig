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

pub const SetKeyAction = struct {
    key: Key,
    key_hash: u64,
};

const KeyStorageMetadata = struct { size_factor: f32 };

pub const Errors = error{
    KeyNotFound,
};
pub const StorageManager = struct {
    init_capacity: u64 = 100_000,
    max_bucket_size: u8 = 10,
    absolute_path: []const u8,
    bin_filename: []const u8 = "buckets.bin",
    metadata_bin_filename: []const u8 = "buckets.metadata.bin",
    resize_factor: f32 = 0.75,
    fst: *fs_tracker.FreeSpaceTracker,

    _mem_alloc: std.mem.Allocator = gpa.allocator(),
    _lru: ?*lru_cache.LRUCache([]const u8, Key) = null,

    _file: ?std.fs.File = null,
    _metadata_file: ?std.fs.File = null,

    _key_storage_path: []u8 = undefined,
    _bucket_offset: u64 = undefined,

    _metadata: KeyStorageMetadata = undefined,
    _resizing_in_progress: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    _resize_actions_buffer: ?*std.ArrayList(SetKeyAction) = null,

    _resize_mutex: std.Thread.Mutex = std.Thread.Mutex{},

    pub fn init(self: *StorageManager) !void {
        if (self._file) |file| {
            file.close();
            self._file = null;
        }

        self._bucket_offset = @sizeOf(Key) * self.max_bucket_size;

        var entries = std.ArrayList([]const u8).init(self._mem_alloc);
        defer entries.deinit();
        const dir = try std.fs.openDirAbsolute(self.absolute_path, std.fs.Dir.OpenDirOptions{
            .iterate = true,
        });
        var dir_iter = dir.iterate();
        while (try dir_iter.next()) |entry| {
            if (entry.kind == .file) {
                if (std.mem.endsWith(u8, entry.name, self.bin_filename)) {
                    const index = std.sort.lowerBound([]const u8, entry.name, entries.items, .{}, lib.sort.string_less_than_fn);
                    try entries.insert(index, entry.name);
                }
            }
        }

        var latest_filename: []const u8 = undefined;
        const latest_entry = entries.popOrNull();
        if (latest_entry) |entry| {
            latest_filename = entry;
        } else {
            const filename = try self._get_latest_bin_filename();
            latest_filename = filename.filename[0..filename.len];
        }

        const key_storage_path = try std.fs.path.join(self._mem_alloc, &[_][]const u8{ self.absolute_path, latest_filename });
        const key_storage_metadata_path = try std.fs.path.join(self._mem_alloc, &[_][]const u8{ self.absolute_path, self.metadata_bin_filename });

        const result = try lib.fs.openOrCreateFileRW(key_storage_path);
        const metadata_result = try lib.fs.openOrCreateFileRW(key_storage_metadata_path);

        const lru = try self._mem_alloc.create(lru_cache.LRUCache([]const u8, Key));
        lru.* = lru_cache.LRUCache([]const u8, Key){
            .allocator = self._mem_alloc,
            .capacity = 10_000,
        };

        const resize_copy_buffer = try self._mem_alloc.create(std.ArrayList(SetKeyAction));
        resize_copy_buffer.* = std.ArrayList(SetKeyAction).init(self._mem_alloc);

        try lru.init();

        self._file = result.file;
        self._metadata_file = metadata_result.file;
        self._lru = lru;
        self._resize_actions_buffer = resize_copy_buffer;
        self._key_storage_path = key_storage_path;

        if (result.created) {
            try _init_buckets_file(result.file, self.init_capacity, self.max_bucket_size);
        }

        if (metadata_result.created) {
            try self._flush_key_storage_metadata();
        } else {
            try self._read_key_storage_metadata();
        }
    }

    pub fn deinit(self: *StorageManager) void {
        if (self._file) |file| {
            file.close();
            self._mem_alloc.destroy(file);
            self._file = null;
        }

        if (self._metadata_file) |metadata_file| {
            metadata_file.close();
            self._mem_alloc.destroy(metadata_file);
            self._metadata_file = null;
        }
    }

    /// lock resize mutex ONLY IF resize in progress
    fn _check_lock_resize_mutex(self: *StorageManager) void {
        if (self._resizing_in_progress.load(.seq_cst)) self._resize_mutex.lock();
    }

    /// unlock resize mutex ONLY IF resize in progress
    fn _check_unlock_resize_mutex(self: *StorageManager) void {
        if (self._resizing_in_progress.load(.seq_cst)) self._resize_mutex.unlock();
    }

    fn _get_latest_bin_filename(self: *StorageManager) !struct { filename: []const u8, len: usize } {
        var filename: [100]u8 = undefined;

        const ts = try std.fmt.allocPrint(self._mem_alloc, "{}", .{std.time.timestamp()});
        defer self._mem_alloc.free(ts);

        const _filename = try std.mem.concat(self._mem_alloc, u8, &[_][]const u8{ ts, "-", self.bin_filename });
        defer self._mem_alloc.free(_filename);

        std.mem.copyForwards(u8, &filename, _filename);

        return .{ .filename = &filename, .len = _filename.len };
    }

    fn _init_buckets_file(file: std.fs.File, capacity: u64, max_bucket_size: u16) !void {
        const zero_key = Key{ .key_hash = 0, .value_offset = 0, .value_size = 0 };
        for (0..capacity * max_bucket_size) |_| {
            const b = std.mem.toBytes(zero_key);
            _ = try file.write(&b);
        }
        const total_b_written = try file.getPos();

        try file.sync();
        std.debug.assert(total_b_written == @sizeOf(Key) * max_bucket_size * capacity);
    }

    fn _read_key_storage_metadata(self: *StorageManager) !void {
        const metadata_file = self._metadata_file.?;

        try metadata_file.seekTo(0);
        var buf: [@sizeOf(KeyStorageMetadata)]u8 = undefined;
        const b_read = try metadata_file.read(&buf);
        lib.assert(b_read == buf.len);

        self._metadata = lib.ptrs.bufToType(KeyStorageMetadata, &buf);
    }

    fn _flush_key_storage_metadata(self: *StorageManager) !void {
        const metadata_file = self._metadata_file.?;

        const b_write = try metadata_file.write(&std.mem.toBytes(self._metadata));
        lib.assert(b_write == @sizeOf(KeyStorageMetadata));

        try metadata_file.sync();
    }

    fn _resize(self: *StorageManager) !void {
        const _new_filename = try self._get_latest_bin_filename();
        const new_bucket_cap = self.init_capacity * 2;

        const new_filename = _new_filename.filename[0.._new_filename.len];

        const new_key_storage_path = try std.fs.path.join(self._mem_alloc, &[_][]const u8{ self.absolute_path, new_filename });
        const new_file = try std.fs.createFileAbsolute(new_key_storage_path, std.fs.File.CreateFlags{
            .read = true,
        });
        try _init_buckets_file(new_file, new_bucket_cap, self.max_bucket_size);

        const file = try std.fs.openFileAbsolute(self._key_storage_path, std.fs.File.OpenFlags{
            .mode = .read_write,
        });
        defer file.close();

        const bucket_step = 100;
        const buf = try self._mem_alloc.alloc(u8, self._bucket_offset * bucket_step);
        defer self._mem_alloc.free(buf);
        const bucket_buf = try self._mem_alloc.alloc(u8, self._bucket_offset);
        defer self._mem_alloc.free(bucket_buf);

        const init_cap_f: f32 = @floatFromInt(self.init_capacity);
        const bucket_step_f: f32 = @floatFromInt(bucket_step);
        const n_bucket_steps: u32 = @intFromFloat(@ceil(init_cap_f / bucket_step_f));
        for (0..@min(self.init_capacity, n_bucket_steps)) |_| {
            const b_read = try file.read(buf);
            lib.assert(b_read % self._bucket_offset == 0);

            const n_buckets = @divFloor(b_read, self._bucket_offset);
            for (0..n_buckets) |b_step| {
                for (0..self.max_bucket_size) |k_step| {
                    const key_start = b_step * self._bucket_offset + k_step * @sizeOf(Key);
                    const key = lib.ptrs.bufToType(Key, buf[key_start .. key_start + @sizeOf(Key)]);
                    if (key.is_zero()) {
                        continue;
                    }

                    const new_bucket_i = new_bucket_cap % key.key_hash;
                    const new_bucket_offset = new_bucket_i * self._bucket_offset;
                    try new_file.seekTo(new_bucket_offset);
                    const bucket_b_read = try new_file.read(bucket_buf);
                    lib.assert(bucket_b_read == bucket_buf.len);

                    const write_key_offset = try _get_write_key_offset(key.key_hash, bucket_buf, new_bucket_offset, self.max_bucket_size);
                    try new_file.seekTo(write_key_offset);
                    const key_b_write = try new_file.write(&std.mem.toBytes(key));
                    lib.assert(key_b_write == @sizeOf(Key));
                    // NOTE: RECHECK this block, probably something is missed
                }
            }
        }

        self._resize_mutex.lock();
        defer self._resize_mutex.unlock();

        const resize_actions_buffer = self._resize_actions_buffer.?;
        const set_key_actions = try resize_actions_buffer.toOwnedSlice();
        for (set_key_actions) |action| {
            const bucket_i = new_bucket_cap % action.key_hash;
            const bucket_offset = bucket_i * self._bucket_offset;

            try new_file.seekTo(bucket_offset);
            const bucket_b_read = try new_file.read(bucket_buf);
            lib.assert(bucket_b_read == bucket_buf.len);

            const write_key_offset = try _get_write_key_offset(action.key_hash, bucket_buf, bucket_offset, self.max_bucket_size);
            try new_file.seekTo(write_key_offset);
            const key_b_write = try new_file.write(&std.mem.toBytes(action.key));
            lib.assert(key_b_write == @sizeOf(Key));
        }

        try new_file.sync();

        self._file = new_file;
        _ = self._resizing_in_progress.swap(false, .seq_cst);
    }

    /// dynamically allocates a bucket buffer, caller is responsible for freeing the bucket buffer.
    fn _get_bucket_disk(self: *StorageManager, key_hash: u64) !struct { buf: []u8, offset: u64 } {
        const file = self._file.?;

        const index = key_hash % self.init_capacity;
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

        for (0..self.max_bucket_size) |keyI| {
            const start = keyI * @sizeOf(Key);
            const key_buf = bucket.buf[start .. start + @sizeOf(Key)];
            const key = lib.ptrs.bufToType(Key, key_buf);

            if (key.key_hash == key_hash) {
                return key;
            }
        }

        return null;
    }

    /// takes in bucket data and `bucket_offset` and returns an offset value where a `Key` can be safely written to
    fn _get_write_key_offset(key_hash: u64, bucket_buf: []u8, bucket_offset: u64, max_bucket_size: u64) !u64 {
        var write_key_offset: u64 = undefined;
        var _write_key_offset_set = false;
        for (0..max_bucket_size) |keyI| {
            const start = keyI * @sizeOf(Key);
            const k = lib.ptrs.bufToType(Key, bucket_buf[start .. start + @sizeOf(Key)]);

            if (key_hash == k.key_hash) {
                write_key_offset = bucket_offset + start;
                _write_key_offset_set = true;
                break;
            }

            if (!_write_key_offset_set and k.is_zero()) {
                write_key_offset = bucket_offset + start;
                _write_key_offset_set = true;
            }
        }

        if (!_write_key_offset_set) {
            std.debug.panic("we're so fucked, write key offset was not set, probably means bucket filled up (offset: {any})", .{bucket_offset});

            // TODO: emergency resize, and do some clever tricks to avoid downtime
        }

        return write_key_offset;
    }

    /// asserts either `key.hash == key_hash` OR `key.is_zero()`
    /// assume this to be deletion operation of key at key_hash in case `key.is_zero()`
    fn _set_key_disk(self: *StorageManager, key_hash: u64, key: Key) !void {
        lib.assert(key_hash == key.key_hash or key.is_zero());

        const bucket = try self._get_bucket_disk(key_hash);
        defer self._mem_alloc.free(bucket.buf);

        const write_key_offset = try _get_write_key_offset(key_hash, bucket.buf, bucket.offset, self.max_bucket_size);

        const size_of_key_f: f32 = @floatFromInt(@sizeOf(Key));
        const max_bucket_size_f: f32 = @floatFromInt(self.max_bucket_size);
        var size_factor: f32 = @floatFromInt(write_key_offset - bucket.offset);
        size_factor = (size_factor / size_of_key_f) / max_bucket_size_f;
        if (size_factor > self._metadata.size_factor) {
            self._metadata.size_factor = size_factor;
            try self._flush_key_storage_metadata();
        }

        self._resize_mutex.lock();
        defer self._resize_mutex.unlock();

        if (self._metadata.size_factor > self.resize_factor and !self._resizing_in_progress.load(.seq_cst)) {
            _ = self._resizing_in_progress.swap(true, .seq_cst);

            const t = try std.Thread.spawn(.{}, _resize, .{self});
            t.detach();
        }

        if (self._resizing_in_progress.load(.seq_cst)) {
            const resize_actions_buffer = self._resize_actions_buffer.?;
            try resize_actions_buffer.append(SetKeyAction{
                .key_hash = key_hash,
                .key = key,
            });
        } else {
            const file = self._file.?;

            try file.seekTo(write_key_offset);
            const key_buf = std.mem.toBytes(key);
            const b_write = try file.write(&key_buf);
            std.debug.assert(b_write == key_buf.len);

            try file.sync();
        }
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
};
