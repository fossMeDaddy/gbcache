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

            const env_var = iter.first();
            const env_var_val = iter.rest();
            if (env_map.get(env_var) == null) {
                try env_map.putMove(@constCast(env_var), @constCast(env_var_val));
            }
        }
    }
}

pub fn validate() !void {
    const enc_key = EnvMap.get(constants.ENV_ENC_KEY_KEY);
    if (enc_key) |key| {
        if (key.len != aes_256_gcm.key_length) {
            return error.EnvValidationFailed;
        }
    }
}

pub fn get_key() ?[aes_256_gcm.key_length]u8 {
    var key: [aes_256_gcm.key_length]u8 = undefined;

    if (EnvMap.get(constants.ENV_ENC_KEY_KEY)) |_key| {
        @memcpy(&key, _key);
    } else {
        return null;
    }

    return key;
}
