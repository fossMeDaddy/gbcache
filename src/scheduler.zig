const std = @import("std");
const lib = @import("lib.zig");

/// `ActionType` is preferrably a u8, string, yk things that could come from an enum
pub fn ScheduleAction(comptime ActionType: type) type {
    return struct {
        timestamp_s: i64,
        dynamic_buf: []u8,
        action_type: ActionType,
    };
}

/// `ActionType` is preferrably a u8, string, yk things that could come from an enum
/// `func` is called as: `func(func_ctx, action)` where action is of type `ScheduleAction(ActionType)`
pub fn TimeScheduler(comptime ActionType: type, comptime func: anytype, comptime func_ctx: anytype) type {
    const Action = ScheduleAction(ActionType);
    const ActionArrayList = std.ArrayList(Action);

    const log_file_sep: u8 = 0x1a;
    const log_file_eof: u8 = 0x1f;

    return struct {
        bin_filename: []const u8 = "scheduler.bin",
        absolute_path: []const u8,
        allocator: std.mem.Allocator,

        _file: std.fs.File = undefined,
        _actions: ActionArrayList = undefined,
        _worker_thread: std.Thread = undefined,
        _scheduler_storage_path: []u8 = undefined,
        _schedule_buffer: ActionArrayList = undefined,
        _schedule_buf_mutex: std.Thread.Mutex = std.Thread.Mutex{},

        const Self = @This();

        pub fn init(self: *Self) !void {
            self._scheduler_storage_path = try std.fs.path.join(self.allocator, &[_][]const u8{ self.absolute_path, self.bin_filename });
            const result = try lib.fs.openOrCreateFileRW(self._scheduler_storage_path);

            self._file = result.file;
            self._actions = ActionArrayList.init(self.allocator);
            self._schedule_buffer = ActionArrayList.init(self.allocator);

            try self._read_actions_disk();
            self._worker_thread = try std.Thread.spawn(.{}, _worker, .{self});
        }

        pub fn deinit(self: *Self) void {
            self._worker_thread.join();
            self.allocator.free(self._scheduler_storage_path);

            self._actions.deinit();
            self._schedule_buffer.deinit();

            self._file.close();
        }

        /// schedule into the future from here.
        /// `dynamic_buf` is probably the output of std.mem.toBytes of your custom struct.
        pub fn schedule(self: *Self, action: Action) !void {
            self._schedule_buf_mutex.lock();
            defer self._schedule_buf_mutex.unlock();

            try self._schedule_buffer.append(action);
        }

        fn _action_ts_desc_sorting_fn(_: @TypeOf(.{}), lhs: Action, rhs: Action) bool {
            return lhs.timestamp_s > rhs.timestamp_s;
        }

        fn _save_actions_disk(self: *Self) !void {
            try self._file.seekTo(0);

            for (self._actions.items) |action| {
                _ = try self._file.write(&std.mem.toBytes(action));
                _ = try self._file.write(&[_]u8{log_file_sep});
            }

            _ = try self._file.write(&[_]u8{log_file_eof});
            try self._file.sync();
        }

        fn _read_actions_disk(self: *Self) !void {
            try self._file.seekTo(0);
            const reader = self._file.reader();

            const _actions_buf = try reader.readUntilDelimiterOrEofAlloc(self.allocator, log_file_eof, 50 * 1024 * 1024);
            if (_actions_buf == null) return;
            const actions_buf = _actions_buf.?;

            var iter = std.mem.splitScalar(u8, actions_buf, log_file_sep);
            while (iter.next()) |action_buf| {
                if (action_buf.len == 0) continue;

                const action = lib.ptrs.bufToType(Action, @constCast(action_buf));
                try self._actions.append(action);
            }
        }

        fn _worker(self: *Self) !void {
            while (true) {
                defer std.time.sleep(1 * std.math.pow(u64, 10, 9));

                self._schedule_buf_mutex.lock();
                const schedule_buffer = try self._schedule_buffer.toOwnedSlice();
                self._schedule_buf_mutex.unlock();

                for (schedule_buffer) |action| {
                    const insert_i = std.sort.lowerBound(Action, action, schedule_buffer, .{}, _action_ts_desc_sorting_fn);
                    try self._actions.insert(insert_i, action);
                }

                const _last_action = self._actions.getLastOrNull();
                if (_last_action == null) continue;
                if (_last_action.?.timestamp_s > std.time.timestamp()) continue;

                var last_action = _last_action.?;
                while (last_action.timestamp_s < std.time.timestamp()) {
                    _ = @call(.auto, func, .{ func_ctx, last_action });
                    _ = self._actions.pop();

                    const _la = self._actions.getLastOrNull();
                    if (_la == null) break;

                    last_action = _la.?;
                }

                try self._save_actions_disk();
            }
        }
    };
}

const TestAction = enum(u8) { get, set };

fn test_func(_: @TypeOf(.{}), action: ScheduleAction(TestAction)) void {
    std.debug.print("test_func running: {any}\n", .{action});
}

test "scheduler" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    const Action = ScheduleAction(TestAction);

    const abs_path = try std.fs.cwd().realpathAlloc(allocator, "./tmp/data");
    defer allocator.free(abs_path);

    var scheduler = TimeScheduler(TestAction, test_func, .{}){
        .allocator = allocator,
        .absolute_path = abs_path,
    };
    try scheduler.init();
    defer scheduler.deinit();

    try scheduler.schedule(Action{
        .timestamp_s = std.time.timestamp() + 10,
        .action_type = .set,
        .dynamic_buf = &[_]u8{},
    });
    try scheduler.schedule(Action{
        .timestamp_s = std.time.timestamp() + 2,
        .action_type = .get,
        .dynamic_buf = &[_]u8{},
    });
}
