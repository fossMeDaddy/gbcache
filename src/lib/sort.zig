const std = @import("std");

pub fn string_less_than_fn(_: @TypeOf(.{}), lhs: []const u8, rhs: []const u8) bool {
    return std.mem.lessThan(u8, lhs, rhs);
}
