const std = @import("std");
const root = @import("./root.zig");
const lib = @import("./lib.zig");
const constants = @import("./constants.zig");

var gpa = std.heap.GeneralPurposeAllocator(.{}){};
var allocator = gpa.allocator();

const net = std.net;

pub const DataTokenSplit: u8 = 0x1e;
pub const DataSplit: u8 = 0x1f;
pub const DataEnd: u8 = 0x1d;
/// even if the data is encrypted, this character is received as is.
pub const StreamEnd: u8 = 0x04;

const Cmd = enum(u8) {
    ping,
    get,
    set,
    del,
    inc,
    exp,
};

fn string_to_cmd_enum(str: []const u8) ?Cmd {
    if (std.mem.eql(u8, str, "PING")) {
        return Cmd.ping;
    } else if (std.mem.eql(u8, str, "GET")) {
        return Cmd.get;
    } else if (std.mem.eql(u8, str, "SET")) {
        return Cmd.set;
    } else if (std.mem.eql(u8, str, "DEL")) {
        return Cmd.del;
    } else if (std.mem.eql(u8, str, "INC")) {
        return Cmd.inc;
    } else if (std.mem.eql(u8, str, "EXP")) {
        return Cmd.exp;
    } else {
        return null;
    }
}

const Command = struct {
    cmd: Cmd,
    key: ?[]const u8 = null,
    value: ?[]const u8 = null,
};

const _StreamData = struct { commands: []Command, stream_data: []u8 };
///`.stream_data` and `.commands` must be freed by the caller!
fn read_stream_into_command(mem_alloc: std.mem.Allocator, stream: net.Stream) !_StreamData {
    const reader = stream.reader();

    var commands = std.ArrayList(Command).init(mem_alloc);

    const _stream_data = try reader.readUntilDelimiterAlloc(mem_alloc, StreamEnd, 512 * 1024);

    var stream_data: []u8 = undefined;
    if (lib.env.get_key()) |key| {
        stream_data = try lib.crypto.decrypt_buffer(mem_alloc, _stream_data, key);
        mem_alloc.free(_stream_data);
    } else {
        stream_data = _stream_data;
    }

    var iter = std.mem.splitScalar(u8, stream_data, DataSplit);
    while (iter.next()) |command_buf| {
        var command_buf_iter = std.mem.splitScalar(u8, command_buf, DataTokenSplit);

        const _cmd_buf = command_buf_iter.first();
        const cmd = string_to_cmd_enum(_cmd_buf) orelse return error.InvalidCommand;

        const key_buf = command_buf_iter.next();
        const val_buf = command_buf_iter.rest();

        try commands.append(Command{
            .cmd = cmd,
            .key = key_buf,
            .value = val_buf,
        });
    }

    const commands_owned = try commands.toOwnedSlice();
    return .{ .commands = commands_owned, .stream_data = stream_data };
}

fn stream_write_buffer(mem_alloc: std.mem.Allocator, stream: net.Stream, buf: []const u8) !void {
    if (lib.env.get_key()) |key| {
        const stream_buf = try lib.crypto.encrypt_buffer(mem_alloc, buf, key);
        defer mem_alloc.free(stream_buf);

        _ = try stream.write(stream_buf);
    } else {
        _ = try stream.write(buf);
    }
}

pub fn main() !void {
    try lib.env.load_dotenv(allocator);
    try lib.env.validate();

    const abs_path = try std.fs.realpathAlloc(allocator, "./tmp/data");
    try root.cache.init(100_000, abs_path);

    const address = try net.Address.parseIp4("0.0.0.0", 8080);
    var server = try address.listen(.{});

    std.debug.print("SERVER READY: port -> 8080\n", .{});
    while (true) {
        const c = try server.accept();
        const stream_data = try read_stream_into_command(allocator, c.stream);
        defer allocator.free(stream_data.stream_data);
        defer allocator.free(stream_data.commands);

        for (stream_data.commands) |command| {
            switch (command.cmd) {
                .ping => {
                    _ = try stream_write_buffer(allocator, c.stream, "PONG");
                },

                .get => {
                    if (command.key == null) {
                        c.stream.close();
                        break;
                    }
                    const key = command.key.?;

                    const value = try root.cache.get(key);
                    if (value) |val_buf| {
                        _ = try stream_write_buffer(allocator, c.stream, val_buf);
                    }
                },

                .set => {
                    if (command.key == null or command.value == null) {
                        c.stream.close();
                        break;
                    }
                    const key = command.key.?;
                    const val = command.value.?;

                    try root.cache.set(key, val);
                },

                .inc => {
                    if (command.key == null) {
                        c.stream.close();
                        break;
                    }
                    const key = command.key.?;

                    var inc_by: u64 = undefined;
                    if (command.value) |v| {
                        const _inc_by_ptr: *u64 = @ptrCast(@constCast(&v));
                        inc_by = _inc_by_ptr.*;
                    } else {
                        inc_by = 1;
                    }

                    const result = try root.cache.increment(key, inc_by);
                    _ = try stream_write_buffer(allocator, c.stream, &std.mem.toBytes(result));
                },

                .del => {
                    if (command.key == null) {
                        c.stream.close();
                        break;
                    }
                    const key = command.key.?;

                    try root.cache.remove(key);
                },

                .exp => {
                    if (command.key == null) {
                        c.stream.close();
                        break;
                    }
                    const key = command.key.?;

                    var expires_at: i64 = undefined;
                    if (command.value) |value| {
                        if (value.len != @sizeOf(i64)) {
                            c.stream.close();
                            break;
                        }

                        expires_at = lib.ptrs.bufToType(i64, @constCast(value));
                    } else {
                        c.stream.close();
                        break;
                    }

                    try root.cache.set_expires_at(key, expires_at);
                },
            }
            _ = try stream_write_buffer(allocator, c.stream, &[_]u8{DataSplit});
        }

        _ = try stream_write_buffer(allocator, c.stream, &[_]u8{DataEnd});
    }
}

test "main" {
    // const allocator = std.testing.allocator;

    const cap = 10_000;
    const abs_path = try std.fs.realpathAlloc(allocator, "./tmp/data");
    defer allocator.free(abs_path);

    try root.cache.init(cap, abs_path);

    try root.cache.set("HALO", "abc---1");
    try root.cache.set("HALO2", "abc##2");

    const v1 = try root.cache.get("HALO");
    const v2 = try root.cache.get("HALO2");

    try root.cache.set("HALO", "abcde$");
    const v1_new = try root.cache.get("HALO");

    try std.testing.expectEqualStrings("abc---1", v1.?);
    try std.testing.expectEqualStrings("abc##2", v2.?);
    try std.testing.expectEqualStrings("abcde$", v1_new.?);

    try root.cache.remove("HALO2");
    const v2_new = try root.cache.get("HALO2");
    try std.testing.expectEqual(null, v2_new);

    const n1: u8 = 69;
    const n1_buf = std.mem.toBytes(n1);
    try root.cache.set("counter1", &n1_buf);
    _ = try root.cache.increment("counter1", 1);
    _ = try root.cache.increment("counter1", 1);
    _ = try root.cache.increment("counter1", 1);
    _ = try root.cache.increment("counter1", 1);
    const n1_new = try root.cache.increment("counter1", 1);
    try std.testing.expectEqual(74, n1_new);
}
