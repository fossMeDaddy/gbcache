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
        head: ?*LRUNode(K, V),
        tail: ?*LRUNode(K, V),
        allocator: std.mem.Allocator,
        capacity: u64,
        concurrency: u64 = 1,

        _map: ?std.AutoHashMap(K, V),
        _size: u64 = 0,
        _thread_pool: std.Thread.Pool = std.Thread.Pool{},

        pub fn init(self: *@This()) void {
            self._map = std.AutoHashMap(K, V).init(self.allocator);
            if (self.capacity < 1) {
                return error.ZeroCapacity;
            }

            try self._thread_pool.init(.{
                .allocator = self.allocator,
                .n_jobs = self.concurrency,
            });
        }

        pub fn set(self: *@This(), m_key: K, m_value: V) !void {
            const m = self._map.?;

            if (self._size >= self.capacity) {
                const tail = self.tail.?;
                defer self.allocator.destroy(tail);

                const removed = m.remove(tail.page_offset);
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

            const existing_page = m.get(m_key);
            if (existing_page) {
                return error.KeyAlreadyExists;
            }

            const prev_kv = try m.fetchPut(m_key, m_value);
            const val_ptr = try m.getPtr(m_key).?;

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
            // if (self.head) |head| {
            //     head.prev = node_ptr;
            // } else {
            //     self.tail = node_ptr;
            // }
            // node_ptr.next = self.head;
            // self.head = node_ptr;
        }

        pub fn get(self: *@This(), m_key: K) ?*V {
            const m = self._map.?;

            const node = self._get_node(m_key);
            if (node) |n| {
                self._push_to_top(n);
            }

            return m.getPtr(m_key);
        }

        fn _get_node(self: *@This(), m_key: K) ?*LRUNode(K, V) {
            const cur_node = self.head;
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
            }
            self.head = node;
        }
    };

    return Cache;
}

test "lru cache" {
    const test_alloc = std.testing.allocator;

    const lru = LRUCache(u64, []const u8){
        .capacity = 5,
        .allocator = test_alloc,
    };

    lru.set(1, "one");
    lru.set(2, "two");
    lru.set(3, "three");
    lru.set(4, "four");
    lru.set(5, "five");

    lru.get(3);
    lru.get(1);
}
