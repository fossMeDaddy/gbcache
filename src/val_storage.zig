const std = @import("std");
const lib = @import("lib.zig");

pub const Page = [1024 * 16]u8;
pub const MemPages = std.AutoHashMap(u64, Page);

var gpa = std.heap.GeneralPurposeAllocator(.{}){};

pub const StorageManager = struct {
    absolute_path: []const u8,
    bin_filename: []const u8 = "data.bin",

    _file: ?std.fs.File = null,
    _mem_alloc: std.mem.Allocator = gpa.allocator(),
    _val_storage_path: ?[]const u8 = null,

    _mem_pages: ?MemPages = null,
    _lru: ?LRUCache = null,

    pub fn init(self: *StorageManager) !void {
        if (self._file) |file| {
            file.close();
            self._file = null;
        }

        const val_storage_path = try std.fs.path.join(self._mem_alloc, &[_][]const u8{ self.absolute_path, self.bin_filename });
        const result = try lib.fs.openOrCreateFileRW(val_storage_path);
        self._file = result.file;
        self._val_storage_path = val_storage_path;

        if (result.created) {}
    }

    pub fn deinit(self: *StorageManager) void {
        if (self._file) |f| {
            f.close();
        }

        self._mem_alloc.free(self._val_storage_path);
    }

    const ReadPage = struct { buf: Page, bytes_read: u32, buf_offset: u32 };
    fn _read_page(self: *StorageManager, value_offset: u64) !ReadPage {
        const file = self._file.?;

        const stat = try file.stat();
        if (value_offset > stat.size) {
            return error.ValueOffsetGreaterThanFileSize;
        }

        const value_offset_f: f64 = @floatFromInt(value_offset);
        const n_pages: u64 = @intFromFloat(std.math.floor(value_offset_f / @sizeOf(Page)));
        const page_offset = @sizeOf(Page) * n_pages;
        try file.seekTo(page_offset);

        var page_buf: [@sizeOf(Page)]u8 = undefined;
        const b_read: u32 = @intCast(try file.read(&page_buf));

        const page_buf_offset: u32 = @intCast(if (n_pages > 0) value_offset % @sizeOf(Page) else value_offset);

        return .{ .buf = page_buf, .bytes_read = b_read, .buf_offset = page_buf_offset };
    }

    const WritePage = struct { b_write: u64, value_offset: u64 };
    fn _write_page(self: *StorageManager, buf: []const u8) !WritePage {
        std.debug.assert(buf.len <= @sizeOf(Page));

        const file = self._file.?;
        try file.seekFromEnd(0);
        const stat = try file.stat();

        const _a = @sizeOf(Page) - stat.size % @sizeOf(Page);
        const _b = @sizeOf(Page) - stat.size;
        const page_free_space = if (stat.size >= @sizeOf(Page)) _a else _b;

        var b_write: u64 = 0;
        if (buf.len > page_free_space) {
            const rand_padding = try self._mem_alloc.alloc(u8, page_free_space);
            defer self._mem_alloc.free(rand_padding);
            b_write += try file.write(rand_padding);

            std.debug.assert((stat.size + b_write) % @sizeOf(Page) == 0);
            // TODO: log free space
        }
        const w_offset = stat.size + b_write;

        b_write += try file.write(buf);
        std.debug.assert(b_write == buf.len);

        try file.sync();
        return .{ .b_write = @intCast(b_write), .value_offset = w_offset };
    }

    pub fn write_page(self: *StorageManager, buf: []const u8) !WritePage {
        // get page from memory, write to it
        // handoff disk writing to thread
        // return
    }
};
