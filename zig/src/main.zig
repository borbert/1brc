const std = @import("std");

const BufSize = 4 * 1024 * 1024; // 4 MB buffer

pub fn main() !void {
    var timer = std.time.Timer.start() catch unreachable;

    const file_path = "../data/measurements_1b.txt";
    var file = try std.fs.cwd().openFile(file_path, .{});
    defer file.close();

    var buf: [BufSize]u8 = undefined;
    var carry: [4096]u8 = undefined; // Enough for the longest line
    var carry_len: usize = 0;

    var lines_read: usize = 0;

    while (true) {
        const n = try file.read(&buf);
        if (n == 0 and carry_len == 0) break;

        var start: usize = 0;
        for (buf[0..n], 0..) |b, i| {
            if (b == '\n') {
                // Copy carry + this chunk up to i for a complete line
                if (carry_len > 0) {
                    _ = carry[carry_len..];
                    try processLine(carry[0..carry_len]);
                    carry_len = 0;
                } else {
                    try processLine(buf[start..i]);
                }
                lines_read += 1;
                start = i + 1;
            }
        }

        // Copy partial line to carry
        if (start < n) {
            const l = n - start;
            if (l > carry.len) return error.LineTooLong;
            std.mem.copyForwards(u8, carry[0..l], buf[start..n]);
            carry_len = l;
        } else {
            carry_len = 0;
        }
    }
    if (carry_len > 0) {
        try processLine(carry[0..carry_len]);
        lines_read += 1;
    }

    const elapsed = timer.read();
    const elapsed_ms = elapsed / 1_000_000; // nanoseconds → ms
    const minutes = elapsed_ms / 60_000;
    const seconds = (elapsed_ms % 60_000) / 1_000;
    const millis = elapsed_ms % 1_000;

    std.debug.print("Elapsed time: {d} min {d} sec {d} ms (total: {d} ms)\n", .{ minutes, seconds, millis, elapsed_ms });
}

fn processLine(line: []const u8) !void {
    // Just a stub; real work comes later.
    _ = line;
}
