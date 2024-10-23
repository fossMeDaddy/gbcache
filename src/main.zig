const std = @import("std");
const root = @import("./root.zig");

var gpa = std.heap.GeneralPurposeAllocator(.{}){};
var allocator = gpa.allocator();

pub fn main() !void {
    const cap = 10_000;
    const abs_path = try std.fs.realpathAlloc(allocator, "./tmp/data");
    defer allocator.free(abs_path);

    try root.cache.init(cap, abs_path);

    try root.cache.set("hello", "abc---1");
    const val = try root.cache.get("hello");

    std.debug.print("\nfinal result: {any}\n", .{val});
}
