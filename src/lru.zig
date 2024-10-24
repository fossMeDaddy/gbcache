const std = @import("std");

pub fn LRUNode(comptime K: type, comptime V: type) type {
    const Node = struct {
        /// map key for lookup
        m_key: K,
        /// map value against the 'm_key' pointer
        m_val_ptr: *V,

        next: ?*@This(),
        prev: ?*@This(),
    };

    return Node;
}

pub fn LRUCache(comptime K: type, comptime V: type) type {
    const Cache = struct {
        head: ?*LRUNode(K, V) = null,
        tail: ?*LRUNode(K, V) = null,
        allocator: std.mem.Allocator,
        capacity: u64,

        _map: ?*std.AutoHashMap(K, V) = null,
        _size: u64 = 0,
        // _thread_pool: std.Thread.Pool = std.Thread.Pool{},

        pub fn init(self: *@This()) !void {
            const m = try self.allocator.create(std.AutoHashMap(K, V));
            m.* = std.AutoHashMap(K, V).init(self.allocator);
            self._map = m;

            if (self.capacity < 1) {
                return error.ZeroCapacity;
            }

            // try self._thread_pool.init(.{
            //     .allocator = self.allocator,
            //     .n_jobs = self.concurrency,
            // });
        }

        fn _get_node(self: *@This(), m_key: K) ?*LRUNode(K, V) {
            var cur_node = self.head;
            while (cur_node) |node| {
                if (node.m_key == m_key) {
                    break;
                }

                cur_node = node.next;
            }

            return cur_node;
        }

        fn _push_to_top(self: *@This(), node: *LRUNode(K, V)) void {
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

        pub fn set(self: *@This(), m_key: K, m_value: V) !V {
            var m = self._map.?;

            if (self._size >= self.capacity) {
                const tail = self.tail.?;
                defer self.allocator.destroy(tail);

                const removed = m.remove(tail.m_key);
                std.debug.assert(removed);

                if (tail == self.head) {
                    self.head = null;
                }
                if (tail.prev) |prev| {
                    prev.next = null;
                }
                self.tail = tail.prev;

                self._size -= 1;
            }

            const prev_kv = try m.fetchPut(m_key, m_value);
            const val_ptr = m.getPtr(m_key).?;

            var node_ptr: *LRUNode(K, V) = undefined;
            if (prev_kv) |kv| {
                if (self._get_node(kv.key)) |node| {
                    node_ptr = node;
                    node_ptr.m_val_ptr = val_ptr;
                } else {
                    std.debug.print("existing KV: {any}\n", .{kv});
                    std.debug.print("this KV was NOT FOUND with _get_node function!!! this should not happen, CRASHING THE PROGRAM...\n", .{});
                    unreachable;
                }
            } else {
                node_ptr = try self.allocator.create(LRUNode(K, V));
                node_ptr.* = LRUNode(K, V){
                    .m_key = m_key,
                    .m_val_ptr = val_ptr,
                    .next = null,
                    .prev = null,
                };

                self._size += 1;
            }

            self._push_to_top(node_ptr);

            return val_ptr.*;
        }

        pub fn get(self: *@This(), m_key: K) ?V {
            const m = self._map.?;

            const node = self._get_node(m_key);
            if (node) |n| {
                self._push_to_top(n);
            }

            return m.get(m_key);
        }
    };

    return Cache;
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

test "lru cache" {
    const test_alloc = std.testing.allocator;

    var lru = LRUCache(u64, []const u8){
        .capacity = 5,
        .allocator = test_alloc,
    };
    try lru.init();
    const m = lru._map.?;

    _test_print_lru(&lru);

    _ = try lru.set(1, "one");
    _test_print_map_stat(m);
    _test_print_lru(&lru);

    _ = try lru.set(2, "two");
    _test_print_map_stat(m);
    _test_print_lru(&lru);

    _ = try lru.set(3, "three");
    _test_print_map_stat(m);
    _test_print_lru(&lru);

    _ = try lru.set(4, "four");
    _test_print_map_stat(m);
    _test_print_lru(&lru);

    _ = try lru.set(5, "five");
    _test_print_map_stat(m);
    _test_print_lru(&lru);

    _ = try lru.set(6, "six");
    _test_print_map_stat(m);
    _test_print_lru(&lru);

    _ = try lru.set(2, "TWO!");
    _test_print_map_stat(m);
    _test_print_lru(&lru);

    const v3 = lru.get(3);
    std.debug.print("v3: {s}\n", .{v3.?.*});

    const v2 = lru.get(2);
    std.debug.print("v2: {s}\n", .{v2.?.*});

    const v1 = lru.get(1);
    std.debug.print("v1: {any}\n", .{v1});

    try std.testing.expectEqualStrings(v2.?.*, "TWO!");
    try std.testing.expectEqual(v1, null);
    try std.testing.expectEqual(v3.?.*, "three");
}
