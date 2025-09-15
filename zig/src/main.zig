const std = @import("std");
const BufSize = 4 * 1024 * 1024; // 4MB buffer

const CityStats = struct {
    min: f64,
    max: f64,
    avg: f64,
    count: u64,
};

pub fn main() !void {
    var timer = std.time.Timer.start() catch unreachable;
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Use unmanaged map to avoid overhead
    var cityTemps = std.StringHashMapUnmanaged(CityStats){};
    defer {
        // Free all keys before deinit
        var it = cityTemps.iterator();
        while (it.next()) |entry| {
            allocator.free(entry.key_ptr.*);
        }
        cityTemps.deinit(allocator);
    }

    const file_path = "../data/measurements_1b.txt";
    var file = try std.fs.cwd().openFile(file_path, .{});
    defer file.close();

    var buf: [BufSize]u8 = undefined;
    var carry: [4096]u8 = undefined;
    var carry_len: usize = 0;
    var lines_read: usize = 0;
    var bad_lines: usize = 0;

    while (true) {
        const n = try file.read(&buf);
        if (n == 0 and carry_len == 0) break;

        var start: usize = 0;
        for (buf[0..n], 0..) |b, i| {
            if (b == '\n') {
                if (carry_len > 0) {
                    processLine(carry[0..carry_len], &cityTemps, allocator) catch {
                        bad_lines += 1;
                    };
                    carry_len = 0;
                } else {
                    processLine(buf[start..i], &cityTemps, allocator) catch {
                        bad_lines += 1;
                    };
                }
                lines_read += 1;
                start = i + 1;

                // Progress indicator
                if (lines_read % 100_000_000 == 0) {
                    std.debug.print("Processed {d}M lines\n", .{lines_read / 1_000_000});
                }
            }
        }

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
        processLine(carry[0..carry_len], &cityTemps, allocator) catch {
            bad_lines += 1;
        };
        lines_read += 1;
    }

    const elapsed = timer.read();
    const elapsed_ms = elapsed / 1_000_000;
    const minutes = elapsed_ms / 60_000;
    const seconds = (elapsed_ms % 60_000) / 1_000;
    const millis = elapsed_ms % 1_000;

    std.debug.print("Lines read: {d}\n", .{lines_read});
    std.debug.print("Bad lines: {d}\n", .{bad_lines});
    std.debug.print("Unique cities: {d}\n", .{cityTemps.count()});
    std.debug.print("Elapsed time: {d} min {d} sec {d} ms\n", .{ minutes, seconds, millis });
}

fn processLine(line: []const u8, cityTemps: *std.StringHashMapUnmanaged(CityStats), allocator: std.mem.Allocator) !void {
    var it = std.mem.splitScalar(u8, line, ';');
    const city = it.next() orelse return;
    const temp_str = it.next() orelse return;

    if (city.len == 0 or temp_str.len == 0) return;

    const trimmed_temp = std.mem.trim(u8, temp_str, " \t\r\n");
    if (trimmed_temp.len == 0) return;

    const parsed_temp = try std.fmt.parseFloat(f64, trimmed_temp);

    const entry = try cityTemps.getOrPut(allocator, city);
    if (!entry.found_existing) {
        // Only duplicate the key when we actually need to store it
        entry.key_ptr.* = try allocator.dupe(u8, city);
        entry.value_ptr.* = CityStats{
            .min = parsed_temp,
            .max = parsed_temp,
            .avg = parsed_temp,
            .count = 1,
        };
    } else {
        var stats = entry.value_ptr;
        stats.min = @min(stats.min, parsed_temp);
        stats.max = @max(stats.max, parsed_temp);
        stats.count += 1;
        const count: f64 = @floatFromInt(stats.count);
        stats.avg += (parsed_temp - stats.avg) / count;
    }
}
