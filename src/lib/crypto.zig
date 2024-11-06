const std = @import("std");
const env = @import("env.zig");
const constants = @import("../constants.zig");

const aes_256_gcm = std.crypto.aead.aes_gcm.Aes256Gcm;

pub const KEY_LENGTH = aes_256_gcm.key_length;

const NONCE = [aes_256_gcm.nonce_length]u8{ 8, 0, 0, 0, 0, 0, 0, 0, 0, 8, 5, 5 };

/// caller must guarantee that encryption key with key name from `constants` exists in env.
/// caller must free the returned memory.
pub fn decrypt_buffer(mem_alloc: std.mem.Allocator, ciphertext: []const u8, key: [aes_256_gcm.key_length]u8) ![]u8 {
    const plaintext_len = ciphertext.len - aes_256_gcm.tag_length;
    if (plaintext_len <= 0) {
        return error.InvalidCipherText;
    }

    const plaintext = try mem_alloc.alloc(u8, plaintext_len);
    var ciphertext_tag: [aes_256_gcm.tag_length]u8 = undefined;
    @memcpy(&ciphertext_tag, ciphertext[plaintext_len..]);

    try aes_256_gcm.decrypt(plaintext, ciphertext[0..plaintext_len], ciphertext_tag, &[_]u8{}, NONCE, key);

    return plaintext;
}

pub fn encrypt_buffer(mem_alloc: std.mem.Allocator, message: []const u8, key: [aes_256_gcm.key_length]u8) ![]u8 {
    const ciphertext = try mem_alloc.alloc(u8, message.len);
    defer mem_alloc.free(ciphertext);

    var ciphertext_tag: [aes_256_gcm.tag_length]u8 = undefined;
    aes_256_gcm.encrypt(ciphertext[0..message.len], &ciphertext_tag, message, &[_]u8{}, NONCE, key);

    const ciphertext_concat = try std.mem.concat(mem_alloc, u8, &[_][]u8{ ciphertext, &ciphertext_tag });

    return ciphertext_concat;
}

test "encryption, decryption test" {
    const testing = std.testing;
    const test_alloc = testing.allocator;

    const key: [KEY_LENGTH]u8 = undefined;

    const msg = try test_alloc.alloc(u8, 10);
    defer test_alloc.free(msg);

    @memcpy(msg, "!@#$%^&*()");

    const enc_buf = try encrypt_buffer(test_alloc, msg, key);
    defer test_alloc.free(enc_buf);

    const dec_buf = try decrypt_buffer(test_alloc, enc_buf, key);
    defer test_alloc.free(dec_buf);

    try testing.expectEqualStrings(msg, dec_buf);
}
