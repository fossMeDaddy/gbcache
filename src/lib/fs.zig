const std = @import("std");

pub fn openOrCreateFileRW(absolute_path: []const u8) !struct { file: std.fs.File, created: bool } {
    var file: ?std.fs.File = null;

    file = std.fs.openFileAbsolute(absolute_path, std.fs.File.OpenFlags{
        .mode = .read_write,
    }) catch |err| {
        if (err == error.FileNotFound) {
            file = try std.fs.createFileAbsolute(absolute_path, std.fs.File.CreateFlags{
                .read = true,
            });

            return .{ .file = file.?, .created = true };
        } else {
            return err;
        }
    };

    if (file) |f| {
        return .{ .file = f, .created = false };
    } else {
        return error.SomethingWentWrong;
    }
}
