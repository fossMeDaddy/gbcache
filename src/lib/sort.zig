const std = @import("std");

const BinarySearchReturnType = struct { index: usize, found: bool };

/// if element exists, gets the index
/// if element doesn't exist, returns index where the element should be inserted.
/// cmp function:
/// 1. `> 0` if lhs > rhs
/// 2. `< 0` if lhs > rhs
/// 3. `0` if lhs == rhs
pub fn binary_search(comptime V: type, search_element: V, arr: []V, cmp_fn: fn (lhs: V, rhs: V) i8) BinarySearchReturnType {
    const n = arr.len;

    var i: usize = 0;
    var j: usize = n;

    while (i < j) {
        const h = (i + j) >> 1;
        if (cmp_fn(search_element, arr[h]) > 0) {
            i = h + 1;
        } else {
            j = h;
        }
    }

    return .{ .index = i, .found = i < n and cmp_fn(arr[i], search_element) == 0 };
}

fn intCmp(lhs: i32, rhs: i32) i8 {
    if (lhs < rhs) return -1;
    if (lhs > rhs) return 1;
    return 0;
}

test "binary search" {
    var arr1 = [_]i32{ 12, 99, 99, 99, 193 };

    // Test for an element not in the array but should be inserted at the end
    const result1 = binary_search(i32, 199, &arr1, intCmp);
    std.debug.print("result1: {}\n", .{result1});
    try std.testing.expect(result1.found == false and result1.index == 5);

    // Test for an element present in the array (first occurrence of 99)
    const result2 = binary_search(i32, 99, &arr1, intCmp);
    std.debug.print("result2: {}\n", .{result2});
    try std.testing.expect(result2.found == true and result2.index == 1);

    // Test for an element present in the array (last element)
    const result3 = binary_search(i32, 193, &arr1, intCmp);
    std.debug.print("result3: {}\n", .{result3});
    try std.testing.expect(result3.found == true and result3.index == 4);

    // Test for an element smaller than any in the array (insert at start)
    const result4 = binary_search(i32, 5, &arr1, intCmp);
    std.debug.print("result4: {}\n", .{result4});
    try std.testing.expect(result4.found == false and result4.index == 0);

    // Test for an element in the middle of the array
    const result5 = binary_search(i32, 12, &arr1, intCmp);
    std.debug.print("result5: {}\n", .{result5});
    try std.testing.expect(result5.found == true and result5.index == 0);

    // Test with an empty array
    var arr_empty = [_]i32{};
    const result6 = binary_search(i32, 12, &arr_empty, intCmp);
    std.debug.print("result6: {}\n", .{result6});
    try std.testing.expect(result6.found == false and result6.index == 0);

    // Test with a single-element array where element is found
    var arr_single_match = [_]i32{12};
    const result7 = binary_search(i32, 12, &arr_single_match, intCmp);
    std.debug.print("result7: {}\n", .{result7});
    try std.testing.expect(result7.found == true and result7.index == 0);

    // Test with a single-element array where element is not found
    var arr_single_no_match = [_]i32{15};
    const result8 = binary_search(i32, 12, &arr_single_no_match, intCmp);
    std.debug.print("result8: {}\n", .{result8});
    try std.testing.expect(result8.found == false and result8.index == 0);
}
