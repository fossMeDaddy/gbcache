const std = @import("std");

const key_storage = @import("./key_storage.zig");
const val_storage = @import("./val_storage.zig");

var key_manager: ?key_storage.StorageManager = null;
var val_manager: ?val_storage.StorageManager = null;

// const gpa = std.heap.GeneralPurposeAllocator(.{}){};

pub fn init(capacity: u64, absolute_path: []const u8) !void {
    const km = key_storage.StorageManager{
        .capacity = capacity,
        .absolute_path = absolute_path,
    };

    const vm = val_storage.StorageManager{
        .absolute_path = absolute_path,
    };

    try km.init();
    try vm.init();

    key_manager = km;
    val_manager = vm;
}

pub fn get(key_str: []const u8) !?[]u8 {
    const km = key_manager.?;
    const vm = val_manager.?;

    const key = km.get(key_str);
    if (key) |k| {
        const val_buf = try vm.read_value(k.value_offset, k.value_size);
        return val_buf;
    } else {
        return null;
    }
}

pub fn set(key_str: []const u8, val_buf: []const u8) !void {
    const km = key_manager.?;
    const vm = val_manager.?;

    const value_offset = try vm.append_write_value(val_buf);
    try km.set(key_str, key_storage.Key{
        .key_hash = km.gen_key_hash(key_str),
        .value_offset = value_offset,
        .value_size = val_buf.len,
    });
}
