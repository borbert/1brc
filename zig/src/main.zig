const std = @import("std");

pub fn main() void {
    const allocator = std.heap.page_allocator;
    const file_path = "../data/hello.txt";
    var file = std.fs.cwd().openFile(file_path, .{}) catch |err| {
        std.debug.print("Failed to open file '{s}': {s}\n", .{ file_path, @errorName(err) });
        return;
    };
    defer file.close();
    const size = file.getEndPos() catch |err| {
        std.debug.print("Failed to get file size: {s}\n", .{@errorName(err)});
        return;
    };
    const buffer = allocator.alloc(u8, size) catch |err| {
        std.debug.print("Allocation failed: {s}\n", .{@errorName(err)});
        return;
    };
    defer allocator.free(buffer);
    _ = file.readAll(buffer) catch |err| {
        std.debug.print("Failed to read file: {s}\n", .{@errorName(err)});
        return;
    };
    std.debug.print("{s}\n", .{buffer});
}
