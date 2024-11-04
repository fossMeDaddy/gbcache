const std = @import("std");
const root = @import("./root.zig");

var gpa = std.heap.GeneralPurposeAllocator(.{}){};
var allocator = gpa.allocator();

const net = std.net;

pub const ResponseSplit: []const u8 = "\r\n";
pub const ResponseEnd: []const u8 = ResponseSplit ++ ResponseSplit;

const Cmd = enum(u8) {
    ping,
    get,
    set,
    del,
    inc,
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
    }

    return null;
}

const Command = struct {
    cmd: Cmd,
    key: ?[]const u8 = null,
    value: ?[]const u8 = null,
};

// TODO: use this when need to keep the connections open!
//
/// read the stream until '\r\n\r\n'
// fn read_until_eor(mem_alloc: std.mem.Allocator, stream: net.Stream) ![]const u8 {
//     var stream_data = try std.ArrayList(u8).initCapacity(mem_alloc, 1024);
//
//     while (true) {
//         var buf: [1024]u8 = undefined;
//         const b_read = stream.read(&buf);
//         if (b_read == 0) {
//             break;
//         }
//
//         const iter = std.mem.splitSequence(u8, buf[0..b_read], ResponseEnd);
//         stream_data.appendSlice(iter.first());
//     }
//
//     return stream_data;
// }

const _StreamData = struct { commands: []Command, stream_data: []u8 };

/// NOTE: `.stream_data` and `.commands` has to be freed by the caller!
fn read_stream_into_command(mem_alloc: std.mem.Allocator, stream: net.Stream) !_StreamData {
    const reader = stream.reader();

    var commands = std.ArrayList(Command).init(mem_alloc);
    const stream_data = try reader.readAllAlloc(mem_alloc, 512 * 1024);

    var iter = std.mem.splitSequence(u8, stream_data, ResponseSplit);
    while (iter.next()) |command_buf| {
        var command_buf_iter = std.mem.splitScalar(u8, command_buf, '\n');

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

    const owned_commands = try commands.toOwnedSlice();
    const _stream_data = _StreamData{ .commands = owned_commands, .stream_data = stream_data };
    return _stream_data;
}

pub fn main() !void {
    const abs_path = try std.fs.realpathAlloc(allocator, "./tmp/data");
    try root.cache.init(100_000, abs_path);

    const address = try net.Address.parseIp4("0.0.0.0", 8080);
    var server = try address.listen(.{});

    while (true) {
        std.debug.print("SERVER READY: port -> 8080\n", .{});
        const c = try server.accept();
        const stream_data = try read_stream_into_command(allocator, c.stream);
        defer allocator.free(stream_data.stream_data);
        defer allocator.free(stream_data.commands);

        for (stream_data.commands) |command| {
            switch (command.cmd) {
                .ping => {
                    _ = try c.stream.write("PONG");
                },

                .get => {
                    if (command.key == null) {
                        c.stream.close();
                        break;
                    }
                    const key = command.key.?;

                    const value = try root.cache.get(key);
                    if (value) |val_buf| {
                        _ = try c.stream.write(val_buf);
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
                    _ = try c.stream.write(&std.mem.toBytes(result));
                },

                .del => {
                    if (command.key == null) {
                        c.stream.close();
                        break;
                    }
                    const key = command.key.?;

                    try root.cache.remove(key);
                },
            }
            _ = try c.stream.write(ResponseSplit);
        }

        // NOTE: response ends with '\r\n\r\n'
        _ = try c.stream.write(ResponseSplit);

        // TODO: when connection is persistent, remove this
        c.stream.close();
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
