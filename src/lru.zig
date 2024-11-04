const std = @import("std");

pub fn LRUNode(comptime K: type, comptime V: type) type {
    return struct {
        const Self = @This();

        /// map key for lookup
        m_key: K,
        /// map value against the 'm_key' pointer
        m_val_ptr: *V,

        next: ?*Self,
        prev: ?*Self,
    };
}

pub fn LRUCacheMapContext(comptime K: type) type {
    return switch (K) {
        []const u8 => std.hash_map.StringContext,
        else => std.hash_map.AutoContext(K),
    };
}

fn LRUCacheMapType(comptime K: type, comptime V: type) type {
    return std.HashMap(K, V, LRUCacheMapContext(K), std.hash_map.default_max_load_percentage);
}

pub fn LRUCache(comptime K: type, comptime V: type) type {
    return struct {
        head: ?*LRUNode(K, V) = null,
        tail: ?*LRUNode(K, V) = null,
        allocator: std.mem.Allocator,
        capacity: u64,

        _map: ?*LRUCacheMapType(K, V) = null,
        _size: u64 = 0,
        _map_ctx: LRUCacheMapContext(K) = LRUCacheMapContext(K){},

        const Self = @This();

        pub fn init(self: *Self) !void {
            const m = try self.allocator.create(LRUCacheMapType(K, V));
            m.* = LRUCacheMapType(K, V).init(self.allocator);
            self._map = m;

            if (self.capacity < 1) {
                return error.ZeroCapacity;
            }
        }

        pub fn eql_keys(self: Self, k1: K, k2: K) bool {
            return self._map_ctx.eql(k1, k2);
        }

        pub fn gen_key_hash(self: Self, k: K) u64 {
            return self._map_ctx.hash(k);
        }

        fn _mem_cpy_key(self: *Self, key: K) std.mem.Allocator.Error!K {
            switch (K) {
                []const u8 => {
                    const key_str: []u8 = @ptrCast(@constCast(key));
                    const key_alloc = try self.allocator.alloc(u8, key_str.len);
                    @memcpy(key_alloc, key_str);

                    return key_alloc;
                },

                else => {
                    return key;
                },
            }
        }

        fn _get_node(self: *Self, m_key: K) ?*LRUNode(K, V) {
            var cur_node = self.head;
            while (cur_node) |node| {
                if (self.eql_keys(node.m_key, m_key)) {
                    break;
                }

                cur_node = node.next;
            }

            return cur_node;
        }

        fn _push_to_top(self: *Self, node: *LRUNode(K, V)) void {
            if (node == self.head) {
                return;
            }

            if (node.prev) |prev| {
                prev.next = node.next;

                if (self.tail == node) {
                    self.tail = prev;
                }
            }
            if (node.next) |next| {
                next.prev = node.prev;
            }

            node.prev = null;
            node.next = self.head;
            if (self.head) |head| {
                head.prev = node;
            } else {
                self.tail = node;
            }
            self.head = node;
        }

        fn _remove_node(self: *Self, node: *LRUNode(K, V)) void {
            defer self.allocator.destroy(node);

            if (node.next) |node_next| {
                node_next.prev = node.prev;
            }
            if (node.prev) |node_prev| {
                node_prev.next = node.next;
            }

            if (node == self.head) {
                self.head = node.next;
            }
            if (node == self.tail) {
                self.tail = node.prev;
            }
        }

        /// sets the m_key to m_value in map. returns a value if there was a value at m_key previously.
        pub fn set(self: *Self, m_key: K, m_value: V) !void {
            var m = self._map.?;

            if (self._size >= self.capacity) {
                const tail = self.tail.?;
                defer self._remove_node(tail);

                const removed = m.remove(tail.m_key);
                std.debug.assert(removed);

                self._size -= 1;
            }

            const m_key_cpy = try self._mem_cpy_key(m_key);

            const entry = try m.getOrPut(m_key_cpy);
            const new_val_ptr = entry.value_ptr;
            entry.value_ptr.* = m_value;

            var node_ptr: *LRUNode(K, V) = undefined;
            if (self._get_node(m_key_cpy)) |node| {
                node_ptr = node;
                node_ptr.m_val_ptr = new_val_ptr;
            } else {
                node_ptr = try self.allocator.create(LRUNode(K, V));
                node_ptr.* = LRUNode(K, V){
                    .m_key = m_key_cpy,
                    .m_val_ptr = new_val_ptr,
                    .next = null,
                    .prev = null,
                };

                self._size += 1;
            }

            self._push_to_top(node_ptr);
        }

        pub fn get(self: *Self, m_key: K) ?V {
            const m = self._map.?;

            const node = self._get_node(m_key);
            if (node) |n| {
                self._push_to_top(n);
            }

            return m.get(m_key);
        }

        pub fn remove(self: *Self, m_key: K) ?V {
            const m = self._map.?;

            var prev_v: ?V = null;
            if (m.fetchRemove(m_key)) |prev_kv| {
                prev_v = prev_kv.value;
            } else {
                return null;
            }

            const _node = self._get_node(m_key);
            if (_node == null) {
                return null;
            }
            const node = _node.?;
            self._remove_node(node);

            self._size -= 1;
            return prev_v;
        }
    };
}

fn _test_print_lru(lru_cache: *LRUCache(u64, []const u8)) void {
    const head = lru_cache.head;

    std.debug.print("ll (size: {any}, head: {any}, tail: {any}):\n", .{ lru_cache._size, lru_cache.head, lru_cache.tail });
    var cur_node = head;
    while (cur_node) |node| {
        std.debug.print("{any}: {s}\n", .{ node.m_key, node.m_val_ptr.* });
        cur_node = node.next;
    }
    std.debug.print("\n\n", .{});
}

fn _test_print_map_stat(m: *std.AutoHashMap(u64, []const u8)) void {
    std.debug.print("len: {any}, unmanaged_size: {any}\n", .{ m.count(), m.unmanaged.size });
}

test "LRU" {
    const test_alloc = std.testing.allocator;
    // var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    // const test_alloc = gpa.allocator();

    var lru = LRUCache(u64, []const u8){
        .capacity = 5,
        .allocator = test_alloc,
    };
    try lru.init();
    // const m = lru._map.?;

    _ = try lru.set(1, "one");
    _ = try lru.set(2, "two");
    _ = try lru.set(3, "three");
    _ = try lru.set(4, "four");
    _ = try lru.set(5, "five");
    _ = try lru.set(6, "six");

    _ = try lru.set(2, "TWO!");

    const v3 = lru.get(3);
    const v2 = lru.get(2);
    const v1 = lru.get(1);
    const v2_r = lru.remove(2);

    try std.testing.expectEqualStrings(v2.?, "TWO!");
    try std.testing.expectEqual(v1, null);
    try std.testing.expectEqualStrings(v3.?, "three");
    try std.testing.expectEqual("TWO!", v2_r.?);

    var lru_str = LRUCache([]const u8, u64){
        .capacity = 3,
        .allocator = test_alloc,
    };
    try lru_str.init();

    _ = try lru_str.set("1", 1);
    _ = try lru_str.set("2", 2);
    _ = try lru_str.set("3", 3);

    const s1 = lru_str.get("1");
    try std.testing.expectEqual(1, s1.?);

    const s3 = lru_str.get("3");
    try std.testing.expectEqual(3, s3.?);
}
