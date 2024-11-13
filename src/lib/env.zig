const std = @import("std");
const fs = @import("./fs.zig");
const constants = @import("../constants.zig");

const aes_256_gcm = std.crypto.aead.aes_gcm.Aes256Gcm;

pub var EnvMap: *std.process.EnvMap = undefined;

pub fn load_dotenv(allocator: std.mem.Allocator) !void {
    const env_map = try allocator.create(std.process.EnvMap);
    env_map.* = std.process.EnvMap.init(allocator);
    EnvMap = env_map;

    const file = std.fs.cwd().openFile(".env", std.fs.File.OpenFlags{
        .mode = .read_write,
    }) catch {
        return;
    };

    const contents = try file.readToEndAlloc(allocator, 10 * 1024);
    defer allocator.free(contents);

    var lines = std.mem.splitScalar(u8, contents, '\n');
    while (lines.next()) |line| {
        if (std.mem.indexOfScalar(u8, line, '=')) |_| {
            var iter = std.mem.splitScalar(u8, line, '=');

            const _env_var = iter.first();
            const env_var = try allocator.alloc(u8, _env_var.len);
            @memcpy(env_var, _env_var);

            const _env_var_val = iter.rest();
            const env_var_val = try allocator.alloc(u8, _env_var_val.len);
            @memcpy(env_var_val, _env_var_val);

            if (env_map.get(env_var) == null) {
                try env_map.putMove(env_var, env_var_val);
            }
        }
    }
}

pub fn validate() !void {
    // doing nothing
}
