const std = @import("std");
const lib = @import("lib.zig");
const lru_cache = @import("lru.zig");
const fs_tracker = @import("fst.zig");

pub const Page = [1024 * 16]u8;

var gpa = std.heap.GeneralPurposeAllocator(.{}){};

pub const StorageManager = struct {
    absolute_path: []const u8,
    bin_filename: []const u8 = "data.bin",
    fst: *fs_tracker.FreeSpaceTracker,

    _file: ?std.fs.File = null,
    _mem_alloc: std.mem.Allocator = gpa.allocator(),
    _val_storage_path: ?[]const u8 = null,

    _lru: ?*lru_cache.LRUCache(u64, Page) = null,

    pub fn init(self: *StorageManager) !void {
        if (self._file) |file| {
            file.close();
            self._file = null;
        }

        const val_storage_path = try std.fs.path.join(self._mem_alloc, &[_][]const u8{ self.absolute_path, self.bin_filename });
        const result = try lib.fs.openOrCreateFileRW(val_storage_path);
        const lru = try self._mem_alloc.create(lru_cache.LRUCache(u64, Page));
        lru.* = lru_cache.LRUCache(u64, Page){
            .allocator = self._mem_alloc,
            .capacity = 10_000,
        };
        try lru.init();

        self._file = result.file;
        self._val_storage_path = val_storage_path;
        self._lru = lru;

        if (!result.created) {}
    }

    pub fn deinit(self: *StorageManager) void {
        if (self._file) |f| {
            f.close();
        }

        self._mem_alloc.free(self._val_storage_path);
    }

    const _GetPageStat = struct { page_offset: u64, cur: u64 };
    fn _get_page_stat(_: StorageManager, val_offset: u64) _GetPageStat {
        var offset: u64 = undefined;
        if (offset >= @sizeOf(Page)) {
            offset = @sizeOf(Page) - val_offset % @sizeOf(Page);
        } else {
            offset = @sizeOf(Page) - val_offset;
        }

        const size_f: f64 = @floatFromInt(val_offset);
        const n_pages: u64 = @intFromFloat(std.math.floor(size_f / @sizeOf(Page)));
        const page_offset = n_pages * @sizeOf(Page);

        return _GetPageStat{ .page_offset = page_offset, .cur = @sizeOf(Page) - offset };
    }

    fn _read_page_disk(self: *StorageManager, page_offset: u64) !struct { buf: Page, bytes_read: u64 } {
        const file = self._file.?;

        try file.seekTo(page_offset);
        var page_buf: Page = undefined;
        const b_read = try file.read(&page_buf);

        return .{ .buf = page_buf, .bytes_read = @intCast(b_read) };
    }

    fn _write_buf(_: StorageManager, page_buf: *Page, buf: []const u8, offset: u64) !void {
        if (offset >= page_buf.len) {
            return error.CursorPositionBiggerThanBuffer;
        }
        if (buf.len > page_buf.len) {
            return error.WriteBufferTooLarge;
        }

        for (buf, 0..buf.len) |b, i| {
            page_buf[offset + i] = b;
        }
    }

    fn _write_page_disk(self: *StorageManager, page_offset: u64, page_ptr: Page) !void {
        const file = self._file.?;

        try file.seekTo(page_offset);
        const b_write = try file.write(&page_ptr); // mem copy
        std.debug.assert(b_write == page_ptr.len);

        try file.sync();
    }

    /// unless absolutely necessary, DO NOT CALL THIS FUNCTION DIRECTLY.
    /// use `save_value` instead
    pub fn write_value(self: *StorageManager, val_offset: u64, buf: []const u8) !void {
        var lru = self._lru.?;

        const page_stat = self._get_page_stat(val_offset);

        var page: Page = undefined;
        if (lru.get(page_stat.page_offset)) |p| {
            page = p;
        } else {
            const disk_page = try self._read_page_disk(page_stat.page_offset); // 1 mem copy
            page = disk_page.buf;
        }

        try self._write_buf(&page, buf, page_stat.cur);
        _ = try lru.set(page_stat.page_offset, page); // 2 mem copy

        // TODO: have a bunch of parallel workers to do this (page-level mutex)
        try self._write_page_disk(page_stat.page_offset, page);
    }

    /// finds a free space or writes ahead of the current cursor. returns value_offset the buffer was written at.
    pub fn save_value(self: *StorageManager, buf: []const u8) !u64 {
        var write_offset: u64 = undefined;
        var _append = true;
        const alloc_val = try self.fst.find_allocate(buf.len);
        if (alloc_val) |val_metadata| {
            write_offset = val_metadata.value_offset;
            _append = false;
        } else {
            write_offset = self.fst.get_cursor();
        }

        try self.write_value(write_offset, buf);
        if (_append) try self.fst.increment_cursor(buf.len);

        return write_offset;
    }

    pub fn read_page(self: *StorageManager, page_offset: u64) !Page {
        var lru = self._lru.?;

        const page_ptr_lru = lru.get(page_offset);
        if (page_ptr_lru) |_page_ptr| {
            return _page_ptr;
        }

        const disk_page = try self._read_page_disk(page_offset);
        _ = try lru.set(page_offset, disk_page.buf);
        return disk_page.buf;
    }

    pub fn read_value(self: *StorageManager, val_offset: u64, val_size: u64) ![]const u8 {
        const page_stat = self._get_page_stat(val_offset);
        if (page_stat.cur + val_size > @sizeOf(Page)) {
            return error.ReadingOutOfBoundsPage;
        }

        const page = try self.read_page(page_stat.page_offset);
        return page[page_stat.cur .. page_stat.cur + val_size];
    }
};
