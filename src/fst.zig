const std = @import("std");
const lib = @import("lib.zig");

pub const FreeSpaceTracker = struct {
    absolute_path: []const u8,
    bin_filename: []const u8 = "fst.bin",
    allocator: std.mem.Allocator,

    /// Mem used: spots_cap * @sizeOf(ValueMetadata)
    spots_cap: u64 = 10_000,

    _cur: u64 = 0,
    _file: ?std.fs.File = null,
    _spots: ?*std.ArrayList(lib.types.ValueMetadata) = null,

    pub fn init(self: *FreeSpaceTracker) !void {
        const result = try lib.fs.openOrCreateFileRW(try std.fs.path.join(self.allocator, &[_][]const u8{ self.absolute_path, self.bin_filename }));
        self._file = result.file;

        const spots = try self.allocator.create(std.ArrayList(lib.types.ValueMetadata));
        spots.* = std.ArrayList(lib.types.ValueMetadata).init(self.allocator);
        self._spots = spots;

        if (result.created) {
            try self._write_cur_disk();
            try self._write_spots_array_disk();
        } else {
            try self._read_cur_disk();
            try self._read_spots_disk();
        }
    }

    pub fn deinit(self: *FreeSpaceTracker) void {
        if (self._spots) |spots| {
            spots.deinit();
            self.allocator.destroy(spots);
        }
        if (self._file) |file| {
            file.close();
        }
    }

    fn _null_val_metadata_bytes() []const u8 {
        const _null_val_metadata = lib.types.ValueMetadata{ .value_offset = 0, .value_size = 0 };
        const b = std.mem.toBytes(_null_val_metadata);

        return &b;
    }

    fn _val_metadata_size_sorting(_: @TypeOf(.{}), lhs: lib.types.ValueMetadata, rhs: lib.types.ValueMetadata) bool {
        return lhs.value_size < rhs.value_size;
    }

    fn _val_metadata_offset_sorting(lhs: lib.types.ValueMetadata, rhs: lib.types.ValueMetadata) bool {
        return lhs.value_offset < rhs.value_offset;
    }

    fn _write_cur_disk(self: *FreeSpaceTracker) !void {
        const file = self._file.?;
        try file.seekTo(0);

        const cur_buf = std.mem.toBytes(self._cur);
        const b_write = try file.write(&cur_buf);
        lib.assert(b_write == @sizeOf(@TypeOf(self._cur)));

        try file.sync();
    }

    fn _read_cur_disk(self: *FreeSpaceTracker) !void {
        const file = self._file.?;
        try file.seekTo(0);

        var cur_buf: [@sizeOf(@TypeOf(self._cur))]u8 = undefined;
        const b_read = try file.read(&cur_buf);
        lib.assert(b_read == cur_buf.len);

        self._cur = lib.ptrs.bufToType(u64, &cur_buf);
    }

    fn _read_spots_disk(self: *FreeSpaceTracker) !void {
        // NOTE: read spots in reverse sorting order of their sizes, as they were written in DESC order
        // even if capacity is reached, wont be losing much free space!
        var spots = self._spots.?;
        const file = self._file.?;

        try file.seekTo(@sizeOf(@TypeOf(self._cur)));

        const stat = try file.stat();
        const real_spots_buf_size = stat.size - @sizeOf(@TypeOf(self._cur));
        lib.assert(real_spots_buf_size % lib.types.ValueMetadataSize == 0);

        const spots_buf = try self.allocator.alloc(u8, @min(self.spots_cap, real_spots_buf_size));
        defer self.allocator.free(spots_buf);

        const b_read = try file.readAll(spots_buf);
        lib.assert(b_read == spots_buf.len);

        const n_val_metadata = @divFloor(spots_buf.len, lib.types.ValueMetadataSize);

        var n = n_val_metadata;
        while (n > 0) {
            const step = n * lib.types.ValueMetadataSize;

            const val_metadata_buf = spots_buf[step - lib.types.ValueMetadataSize .. step];
            const val_metadata = lib.ptrs.bufToType(lib.types.ValueMetadata, val_metadata_buf);

            try spots.append(val_metadata);
            n -= 1;
        }

        const is_sorted = std.sort.isSorted(lib.types.ValueMetadata, spots.items, .{}, _val_metadata_size_sorting);
        if (!is_sorted) {
            std.sort.block(lib.types.ValueMetadata, spots.items, .{}, _val_metadata_size_sorting);
            _ = try self._write_spots_array_disk();
        }
    }

    // TODO: make it efficient!
    fn _write_spots_array_disk(self: *FreeSpaceTracker) !void {
        const spots = self._spots.?;
        const file = self._file.?;

        try file.seekTo(@sizeOf(@TypeOf(self._cur)));

        var rev_iter = std.mem.reverseIterator(spots.items);
        while (rev_iter.next()) |item| {
            const spot_b = std.mem.toBytes(item);
            _ = try file.write(&spot_b);
        }
        // _ = try file.write(_null_val_metadata_bytes());

        try file.sync();
    }

    fn _get_insert_index_asc_order(self: *FreeSpaceTracker, target_val_metadata: lib.types.ValueMetadata) usize {
        const spots = self._spots.?;

        return std.sort.lowerBound(lib.types.ValueMetadata, target_val_metadata, spots.items, .{}, _val_metadata_size_sorting);
    }

    /// returns index of the free space item
    fn _get_free_space(self: *FreeSpaceTracker, min_bytes: u64) ?usize {
        const spots = self._spots.?;

        const index = self._get_insert_index_asc_order(lib.types.ValueMetadata{ .value_offset = 0, .value_size = min_bytes });
        if (index < spots.items.len) {
            return index;
        }

        return null;
    }

    pub fn get_cursor(self: *FreeSpaceTracker) u64 {
        return self._cur;
    }

    pub fn increment_cursor(self: *FreeSpaceTracker, by: u64) !void {
        self._cur += by;

        // TODO: fs IO operations in separate thread
        _ = try self._write_cur_disk();
    }

    /// if val_metadata.value_size is received, this blows up.
    pub fn log_free_space(self: *FreeSpaceTracker, val_metadata: lib.types.ValueMetadata) !void {
        lib.assert(val_metadata.value_size > 0);
        var spots = self._spots.?;

        const insert_i = self._get_insert_index_asc_order(val_metadata);
        try spots.insert(insert_i, val_metadata);

        // TODO: call this in a separate thread
        try self._write_spots_array_disk();
    }

    /// finds a free space, allocates the free space and logs the remaining space back
    pub fn find_allocate(self: *FreeSpaceTracker, min_bytes: u64) !?lib.types.ValueMetadata {
        if (min_bytes == 0) {
            return error.InvalidAllocationRequested;
        }

        const spots = self._spots.?;

        const free_i = self._get_free_space(min_bytes);
        if (free_i) |i| {
            var val_metadata = spots.orderedRemove(i);
            const alloc_space = lib.types.ValueMetadata{ .value_offset = val_metadata.value_offset, .value_size = min_bytes };

            val_metadata.value_offset += min_bytes;
            val_metadata.value_size -= min_bytes;

            if (val_metadata.value_size > 0) {
                try self.log_free_space(val_metadata);
            }

            return alloc_space;
        }

        return null;
    }
};

test "FST" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const test_alloc = gpa.allocator();

    const tracker_filename = "fst_test.bin";
    const abs_path = try std.fs.realpathAlloc(test_alloc, "./tmp/data");

    const tracker_abs_filepath = try std.fs.path.join(test_alloc, &[_][]const u8{ abs_path, tracker_filename });
    std.fs.deleteFileAbsolute(tracker_abs_filepath) catch {};

    var fst = FreeSpaceTracker{ ._cur = 0, .absolute_path = abs_path, .bin_filename = tracker_filename, .allocator = test_alloc };
    try fst.init();

    try fst.log_free_space(lib.types.ValueMetadata{ .value_offset = 0, .value_size = 100 });
    try fst.log_free_space(lib.types.ValueMetadata{ .value_offset = 199, .value_size = 20 });
    try fst.log_free_space(lib.types.ValueMetadata{ .value_offset = 1000, .value_size = 80 });

    try std.testing.expectEqual(100, fst._spots.?.items[2].value_size);

    const alloc_size1 = 21;
    const val_alloc = try fst.find_allocate(alloc_size1);
    std.debug.print("ALLOCATION SUCCESS: {any}\n", .{val_alloc.?});

    try std.testing.expectEqual(1000, val_alloc.?.value_offset);
    try std.testing.expectEqual(alloc_size1, val_alloc.?.value_size);

    const spot_i = fst._get_free_space(80 - alloc_size1);
    const val_metadata = fst._spots.?.items[spot_i.?];
    try std.testing.expectEqual(1000 + alloc_size1, val_metadata.value_offset);
    try std.testing.expectEqual(80 - alloc_size1, val_metadata.value_size);

    try fst.increment_cursor(10);
    try fst.increment_cursor(10);
    try fst.increment_cursor(10);
    try std.testing.expectEqual(30, fst._cur);
    try fst._read_cur_disk();
    try std.testing.expectEqual(30, fst._cur);

    const prev_cur = fst._cur;
    const prev_spots_arr = try fst._spots.?.toOwnedSlice();
    defer test_alloc.free(prev_spots_arr);

    fst.deinit();
    try fst.init();

    try std.testing.expectEqual(prev_cur, fst._cur);

    for (fst._spots.?.items, prev_spots_arr) |item, prev_item| {
        try std.testing.expectEqual(prev_item.value_offset, item.value_offset);
        try std.testing.expectEqual(prev_item.value_size, item.value_size);
    }
}
