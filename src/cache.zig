const std = @import("std");
const modks = @import("key_storage.zig");
const modvs = @import("val_storage.zig");
const modfst = @import("fst.zig");
const modts = @import("time_scheduler.zig");
const lib = @import("lib.zig");

const ActionType = enum(u8) { del };
const TimeSchedulerAction = modts.ScheduleAction(ActionType);
const Ctx = struct {
    km: *modks.StorageManager,
};
const TimeScheduler = modts.TimeScheduler(ActionType, _scheduler_callback, Ctx);

var key_manager: ?*modks.StorageManager = null;
var val_manager: ?*modvs.StorageManager = null;
var fs_tracker: ?*modfst.FreeSpaceTracker = null;
var time_scheduler: ?*TimeScheduler = null;

var gpa = std.heap.GeneralPurposeAllocator(.{}){};
const allocator = gpa.allocator();

pub fn init(capacity: u64, absolute_path: []const u8) !void {
    const fst = try allocator.create(modfst.FreeSpaceTracker);
    fst.* = modfst.FreeSpaceTracker{ .absolute_path = absolute_path, .allocator = allocator };
    try fst.init();

    const km = try allocator.create(modks.StorageManager);
    km.* = modks.StorageManager{
        .init_capacity = capacity,
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

    const ts = try allocator.create(TimeScheduler);
    ts.* = TimeScheduler{
        .allocator = allocator,
        .absolute_path = absolute_path,
        .ctx = Ctx{
            .km = km,
        },
    };
    try ts.init();

    key_manager = km;
    val_manager = vm;
    fs_tracker = fst;
    time_scheduler = ts;
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

    const value_offset = try vm.save_value(val_buf);
    try km.set(key_str, .{
        .value_offset = value_offset,
        .value_size = val_buf.len,
    });
}

pub fn remove(key_str: []const u8) !void {
    var km = key_manager.?;
    try km.remove(key_str);
}

pub fn set_expires_at(key_str: []const u8, timestamp_s: i64) !void {
    var ts = time_scheduler.?;

    try ts.schedule(TimeSchedulerAction{
        .action_type = .del,
        .timestamp_s = timestamp_s,
        .dynamic_buf = key_str,
    });
}

/// the value stored MUST BE of the size of either, 64, 32, 16 or 8 bits.
/// the value stored MUST BE unsigned.
/// returns the new value as a u64.
pub fn increment(key_str: []const u8, inc: u64) !u64 {
    const km = key_manager.?;
    const vm = val_manager.?;

    const _key = try km.get(key_str);
    if (_key == null) {
        return error.KeyNotFound;
    }
    const key = _key.?;

    var val_buf = try vm.read_value(key.value_offset, key.value_size);

    var num: u64 = undefined;
    var _max_int_wrap: u64 = undefined;
    switch (val_buf.len) {
        1 => {
            num = @intCast(lib.ptrs.bufToType(u8, @constCast(val_buf)));
            _max_int_wrap = std.math.maxInt(u8);
        },
        2 => {
            num = @intCast(lib.ptrs.bufToType(u16, @constCast(val_buf)));
            _max_int_wrap = std.math.maxInt(u16);
        },
        4 => {
            num = @intCast(lib.ptrs.bufToType(u32, @constCast(val_buf)));
            _max_int_wrap = std.math.maxInt(u32);
        },
        8 => {
            num = @intCast(lib.ptrs.bufToType(u64, @constCast(val_buf)));
            _max_int_wrap = std.math.maxInt(u64);
        },
        else => {
            return error.InvalidValueNaN;
        },
    }
    num += inc;
    num = num % _max_int_wrap;

    switch (val_buf.len) {
        1 => {
            const n: u8 = @intCast(num);
            val_buf = &std.mem.toBytes(n);
        },
        2 => {
            const n: u16 = @intCast(num);
            val_buf = &std.mem.toBytes(n);
        },
        4 => {
            const n: u32 = @intCast(num);
            val_buf = &std.mem.toBytes(n);
        },
        8 => {
            const n: u64 = @intCast(num);
            val_buf = &std.mem.toBytes(n);
        },
        else => unreachable,
    }
    lib.assert(val_buf.len == key.value_size);

    try vm.write_value(key.value_offset, val_buf);
    return num;
}

fn _scheduler_callback(ctx: Ctx, action: TimeSchedulerAction) void {
    switch (action.action_type) {
        .del => {
            ctx.km.remove(action.dynamic_buf) catch {};
        },
    }
}
