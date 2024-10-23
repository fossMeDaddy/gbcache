pub fn bufToStruct(comptime T: type, bufPtr: *const []u8) *T {
    const s: *T = @ptrCast(@alignCast(bufPtr.ptr));
    return s;
}
