pub fn assert(expr: bool) void {
    if (!expr) {
        unreachable;
    }
}
