const std = @import("std");
const root = @import("./root.zig");

var gpa = std.heap.GeneralPurposeAllocator(.{}){};
var allocator = gpa.allocator();

pub fn main() !void {
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
}
