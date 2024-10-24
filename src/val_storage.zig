const std = @import("std");
const lib = @import("lib.zig");
const lru_cache = @import("lru.zig");

pub const Page = [1024 * 16]u8;

var gpa = std.heap.GeneralPurposeAllocator(.{}){};

pub const StorageManager = struct {
    absolute_path: []const u8,
    bin_filename: []const u8 = "data.bin",

    _file: ?std.fs.File = null,
    _size: u64 = 0,
    _mem_alloc: std.mem.Allocator = gpa.allocator(),
    _val_storage_path: ?[]const u8 = null,

    _lru: ?lru_cache.LRUCache(u64, Page) = null,

    pub fn init(self: *StorageManager) !void {
        if (self._file) |file| {
            file.close();
            self._file = null;
        }

        const val_storage_path = try std.fs.path.join(self._mem_alloc, &[_][]const u8{ self.absolute_path, self.bin_filename });
        const result = try lib.fs.openOrCreateFileRW(val_storage_path);
        const lru = lru_cache.LRUCache(u64, Page){};

        try lru.init();

        self._file = result.file;
        self._val_storage_path = val_storage_path;
        self._lru = lru;

        if (!result.created) {
            self._size = try result.file.stat();
        }
    }

    pub fn deinit(self: *StorageManager) void {
        if (self._file) |f| {
            f.close();
        }

        self._mem_alloc.free(self._val_storage_path);
    }

    const _GetPageStat = struct { page_offset: u64, cur: u64 };
    fn _get_page_stat(val_offset: u64) _GetPageStat {
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

    fn _write_buf(page_buf: *Page, buf: []const u8, offset: u64) !void {
        if (offset >= page_buf.len) {
            return error.CursorPositionBiggerThanBuffer;
        }
        if (buf.len > page_buf) {
            return error.WriteBufferTooLarge;
        }

        for (buf, 0..buf.len) |b, i| {
            page_buf[offset + i] = b;
        }
    }

    fn _write_page_disk(self: *StorageManager, page_offset: u64, page_ptr: Page) !void {
        const file = self._file.?;

        try file.seekTo(page_offset);
        const b_write = try file.write(page_ptr.*); // mem copy
        std.debug.assert(b_write == page_ptr.len);

        try file.sync();
    }

    pub fn write_value(self: *StorageManager, val_offset: u64, buf: []const u8) !void {
        const lru = self._lru.?;

        const page_stat = self._get_page_stat(val_offset);

        var page_ptr: *Page = undefined;
        if (lru.get(page_stat.page_offset)) |ptr| {
            page_ptr = ptr;
        } else {
            const page = try self._read_page_disk(page_stat.page_offset); // 1 mem copy
            page_ptr = &page;
        }

        try self._write_buf(page_ptr, buf, page_stat.cur);
        try lru.set(page_stat.page_offset, page_ptr.*); // 2 mem copy

        // TODO: have a bunch of parallel workers to do this (page-level mutex)
        try self._write_page_disk(page_stat.page_offset, page_ptr.*);
    }

    pub fn append_write_value(self: *StorageManager, buf: []const u8) !u64 {
        try self.write_value(self._size, buf);
        const val_offset = self._size;
        self._size += buf.len;

        return val_offset;
    }

    pub fn read_page(self: *StorageManager, page_offset: u64) !Page {
        const lru = self._lru.?;

        const page_ptr_lru = lru.get(page_offset);
        if (page_ptr_lru) |_page_ptr| {
            return _page_ptr;
        }

        const disk_page = try self._read_page_disk(page_offset);
        const page_ptr = try lru.set(page_offset, disk_page.buf);
        return page_ptr.*;
    }

    pub fn read_value(self: *StorageManager, val_offset: u64, val_size: u64) ![]const u8 {
        const page_stat = self._get_page_stat(val_offset);
        const page = try self.read_page(page_stat.page_offset);

        return page[page_stat.cur .. page_stat.cur + val_size];
    }
};
