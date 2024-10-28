const std = @import("std");

const key_storage = @import("./key_storage.zig");
const val_storage = @import("./val_storage.zig");

var key_manager: ?*key_storage.StorageManager = null;
var val_manager: ?*val_storage.StorageManager = null;

var gpa = std.heap.GeneralPurposeAllocator(.{}){};
const allocator = gpa.allocator();

pub fn init(capacity: u64, absolute_path: []const u8) !void {
    const km = try allocator.create(key_storage.StorageManager);
    km.* = key_storage.StorageManager{
        .capacity = capacity,
        .absolute_path = absolute_path,
    };

    const vm = try allocator.create(val_storage.StorageManager);
    vm.* = val_storage.StorageManager{
        .absolute_path = absolute_path,
    };

    try km.init();
    try vm.init();

    key_manager = km;
    val_manager = vm;
}

pub fn deinit() void {
    allocator.destroy(key_manager);
    allocator.destroy(val_manager);

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
    std.debug.print("SET : {*}, {*}\n", .{ &km, &vm });

    const value_offset = try vm.save_value(val_buf);
    try km.set(key_str, .{
        .value_offset = value_offset,
        .value_size = val_buf.len,
    });
}
