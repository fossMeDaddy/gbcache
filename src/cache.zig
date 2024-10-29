const std = @import("std");

const modks = @import("./key_storage.zig");
const modvs = @import("./val_storage.zig");
const modfst = @import("./fst.zig");

var key_manager: ?*modks.StorageManager = null;
var val_manager: ?*modvs.StorageManager = null;
var fs_tracker: ?*modfst.FreeSpaceTracker = null;

var gpa = std.heap.GeneralPurposeAllocator(.{}){};
const allocator = gpa.allocator();

pub fn init(capacity: u64, absolute_path: []const u8) !void {
    const fst = try allocator.create(modfst.FreeSpaceTracker);
    fst.* = modfst.FreeSpaceTracker{ .absolute_path = absolute_path, .allocator = allocator };
    try fst.init();

    const km = try allocator.create(modks.StorageManager);
    km.* = modks.StorageManager{
        .capacity = capacity,
        .absolute_path = absolute_path,
        .fst = fst,
    };
    try km.init();

    const vm = try allocator.create(modvs.StorageManager);
    vm.* = modvs.StorageManager{
        .absolute_path = absolute_path,
        .fst = fst,
    };
    try vm.init();

    key_manager = km;
    val_manager = vm;
    fs_tracker = fst;

    std.debug.print("\nFREE SPACE TRACKER STATUS\n", .{});
    std.debug.print("{any}\n{any}\n\n", .{ fst.get_cursor(), fst._spots.?.items });
}

pub fn deinit() void {
    const km = key_manager.?;
    const vm = val_manager.?;
    const fst = fs_tracker.?;

    km.deinit();
    vm.deinit();
    fst.deinit();

    allocator.destroy(km);
    allocator.destroy(vm);
    allocator.destroy(fst);

    // TODO: call deinit on key & val managers also
}

pub fn get(key_str: []const u8) !?[]const u8 {
    var km = key_manager.?;
    var vm = val_manager.?;

    const key = try km.get(key_str);
    if (key) |k| {
        const val_buf = try vm.read_value(k.value_offset, k.value_size);
        return val_buf;
    } else {
        return null;
    }
}

pub fn set(key_str: []const u8, val_buf: []const u8) !void {
    var km = key_manager.?;
    var vm = val_manager.?;

    _ = try km.get(key_str);
    const value_offset = try vm.save_value(val_buf);
    try km.set(key_str, .{
        .value_offset = value_offset,
        .value_size = val_buf.len,
    });
}
