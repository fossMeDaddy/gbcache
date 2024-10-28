pub const fs = @import("lib/fs.zig");
pub const ptrs = @import("lib/ptrs.zig");
pub const types = @import("lib/types.zig");
pub const sort = @import("lib/sort.zig");

pub fn assert(expr: bool) void {
    if (!expr) {
        unreachable;
    }
}
