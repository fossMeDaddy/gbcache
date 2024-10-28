pub fn bufToType(comptime T: type, buf: []u8) T {
    const s: *T = @ptrCast(@alignCast(buf.ptr));
    return s.*;
}
