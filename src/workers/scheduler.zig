// NOTE: no use of this scheduler as of now
const std = @import("std");
const lib = @import("../lib.zig");

pub const ScheduleAction = enum(u8) { del };
pub const ScheduleEntry = struct { action: ScheduleAction, timestamp: i64, key_hash: u64, value_metadata: lib.types.ValueMetadata };

pub const TimeScheduler = struct {
    allocator: std.mem.Allocator,
    schedule_log_absolute_path: []const u8,
    schedule_log_filename: []const u8 = "schedule_logs.bin",

    /// init capacity, will grow, reduce reallocations by increasing this number
    write_buffer_capacity: u32 = 1000,

    /// init capacity, will grow, reduce reallocations by increasing this number
    total_buffer_capacity: u32 = 10000,

    _file: ?*std.fs.File = null,
    _write_log_buffer: ?*std.ArrayList(ScheduleEntry) = null,
    _log_buffer: ?*std.ArrayList(ScheduleEntry) = null,

    _log_buffer_rwlock: std.Thread.RwLock = std.Thread.RwLock{},
    _write_log_mutex: std.Thread.Mutex = std.Thread.Mutex{},

    pub fn init(self: *TimeScheduler) !void {
        const filepath = std.fs.path.join(self.allocator, [_][]const u8{ self.schedule_log_absolute_path, self.schedule_log_filename });
        const result = try lib.fs.openOrCreateFileRW(filepath);
        self._file = result.file;

        const write_log_buffer = try self.allocator.create(std.ArrayList(ScheduleEntry));
        write_log_buffer.* = try std.ArrayList(ScheduleEntry).initCapacity(self.allocator, self.write_buffer_capacity);
        self._write_log_buffer = write_log_buffer;

        const log_buffer = try self.allocator.create(std.ArrayList(ScheduleEntry));
        log_buffer.* = try std.ArrayList(ScheduleEntry).initCapacity(self.allocator, self.total_buffer_capacity);
        self._log_buffer = log_buffer;

        if (result.created) {
            try self._write_log_file();
        } else {
            try self._read_log_file();
        }

        try std.Thread.spawn(.{}, _worker, .{self});
    }

    pub fn deinit(self: *TimeScheduler) !void {
        const log_buffer = self._write_log_buffer.?;

        log_buffer.deinit();
        self.allocator.destroy(log_buffer);
    }

    fn _worker(self: *TimeScheduler) void {
        const log_buffer = self._log_buffer.?;

        while (true) {
            self._write_log_mutex.lock();
            const _write_log_buffer = self._write_log_buffer.?;
            const write_log_buffer = try _write_log_buffer.toOwnedSlice();
            self._write_log_mutex.unlock();

            defer self.allocator.free(write_log_buffer);

            self._log_buffer_rwlock.lock();
            for (write_log_buffer) |log| {
                const index = try std.sort.lowerBound(ScheduleEntry, log, write_log_buffer.items, .{}, _schedule_entry_timestamp_sorting_desc);
                try log_buffer.insert(index, log);
            }

            while (log_buffer.getLastOrNull()) |last_entry| {
                if (std.time.timestamp() <= last_entry.timestamp) {
                    break;
                }

                _ = log_buffer.pop();
                switch (last_entry.action) {
                    .del => {},
                }
            }

            self._log_buffer_rwlock.unlock();
            std.time.sleep(500 * std.math.pow(u64, 10, 6));
        }
    }

    /// DANGER: overrides the time schedule logs in disk
    fn _write_log_file(self: *TimeScheduler) !void {
        const file = self._file.?;
        const log_buffer = self._log_buffer.?;

        self._log_buffer_rwlock.lockShared();
        defer self._log_buffer_rwlock.unlockShared();

        try file.lock(.exclusive);
        defer file.unlock();
        try file.seekTo(0);

        const n_entries: u32 = log_buffer.items.len;
        const n_entries_buf = std.mem.toBytes(n_entries);
        const b_write = try file.write();
        lib.assert(b_write == n_entries_buf.len);

        for (log_buffer.items) |entry| {
            const entry_buf = std.mem.toBytes(entry);
            const b_write_entry = try file.write(entry_buf);
            lib.assert(b_write_entry == entry_buf.len);
        }

        try file.sync();
    }

    /// DANGER: overrides the log_buffer in memory
    fn _read_log_file(self: *TimeScheduler) !void {
        const file = self._file.?;
        const log_buffer = self._log_buffer.?;

        self._log_buffer_rwlock.lock();
        defer self._log_buffer_rwlock.unlock();

        try file.lock(.exclusive);
        defer file.unlock();
        try file.seekTo(0);

        var n_entries_buf: [@sizeOf(u32)]u8 = undefined;
        const b_read = try file.read(&n_entries_buf);
        lib.assert(b_read == n_entries_buf);

        log_buffer.clearAndFree();
        const n_entries = lib.ptrs.bufToType(u32, n_entries_buf);
        for (0..n_entries) |_| {
            var entry_buf: [@sizeOf(ScheduleEntry)]u8 = undefined;
            const b_read_entry = file.read(&entry_buf);
            lib.assert(b_read_entry == entry_buf.len);

            const entry = lib.ptrs.bufToType(ScheduleEntry, entry_buf);
            const index = try std.sort.lowerBound(ScheduleEntry, entry, log_buffer.items, .{}, _schedule_entry_timestamp_sorting_desc);
            log_buffer.insert(index, entry);
        }
    }

    fn _schedule_entry_timestamp_sorting_desc(_: @TypeOf(.{}), lhs: ScheduleEntry, rhs: ScheduleEntry) bool {
        return lhs.timestamp > rhs.timestamp;
    }

    pub fn schedule(self: *TimeScheduler, entry: ScheduleEntry) !void {
        self._write_log_mutex.lock();
        defer self._write_log_mutex.unlock();

        const write_log_buffer = self._write_log_buffer.?;

        const index = try std.sort.lowerBound(ScheduleEntry, entry, write_log_buffer.items, .{}, _schedule_entry_timestamp_sorting_desc);
        write_log_buffer.insert(index, entry);
    }
};
