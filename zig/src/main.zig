const std = @import("std");
// const BufSize = 4 * 1024 * 1024; // 4MB buffer

const CityStats = struct {
    min: f64,
    max: f64,
    sum: f64,
    count: u64,

    fn avg(self: CityStats) f64 {
        return self.sum / @as(f64, @floatFromInt(self.count));
    }
};

pub fn main() !void {
    var timer = std.time.Timer.start() catch unreachable;
    const allocator = std.heap.page_allocator;

    // String interning to avoid duplicate city names
    var string_pool = std.StringHashMapUnmanaged([]const u8){};
    defer {
        var pool_it = string_pool.iterator();
        while (pool_it.next()) |entry| {
            allocator.free(entry.value_ptr.*);
        }
        string_pool.deinit(allocator);
    }

    // Use unmanaged map to avoid overhead
    var cityTemps = std.StringHashMapUnmanaged(CityStats){};
    defer cityTemps.deinit(allocator);

    const file_path = "../data/measurements_1b.txt";
    var file = try std.fs.cwd().openFile(file_path, .{});
    defer file.close();

    //get file stats for the map
    const file_stats = try file.stat();
    // allocate memory for the entire file
    const mapped_memory = try std.posix.mmap(null, file_stats.size, std.posix.PROT.READ, .{ .TYPE = .PRIVATE }, file.handle, 0);
    defer std.posix.munmap(mapped_memory);

    var lines_read: usize = 0;
    var bad_lines: usize = 0;
    var start: usize = 0;

    for (mapped_memory, 0..) |b, i| {
        if (b == '\n') {
            if (i > start) {
                processLine(mapped_memory[start..i], &cityTemps, &string_pool, allocator) catch {
                    bad_lines += 1;
                };
            }
            lines_read += 1;
            start = i + 1;
        }
    }

    if (start < mapped_memory.len) {
        processLine(mapped_memory[start..], &cityTemps, &string_pool, allocator) catch {
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

fn processLine(line: []const u8, cityTemps: *std.StringHashMapUnmanaged(CityStats), string_pool: *std.StringHashMapUnmanaged([]const u8), allocator: std.mem.Allocator) !void {
    // Fast semicolon search - avoid iterator overhead
    const semicolon_pos = std.mem.indexOfScalar(u8, line, ';') orelse return;
    if (semicolon_pos == 0 or semicolon_pos >= line.len - 1) return;

    const city = line[0..semicolon_pos];
    const temp_str = line[semicolon_pos + 1 ..];

    // Skip trimming if not needed (most lines won't need it)
    const clean_temp = if (temp_str[temp_str.len - 1] == '\r')
        temp_str[0 .. temp_str.len - 1]
    else
        temp_str;

    const parsed_temp = std.fmt.parseFloat(f64, clean_temp) catch return;

    // Get interned city string (avoids duplicate allocations)
    const interned_city = blk: {
        const pool_entry = try string_pool.getOrPut(allocator, city);
        if (!pool_entry.found_existing) {
            pool_entry.value_ptr.* = try allocator.dupe(u8, city);
        }
        break :blk pool_entry.value_ptr.*;
    };

    const entry = try cityTemps.getOrPut(allocator, interned_city);
    if (!entry.found_existing) {
        entry.value_ptr.* = CityStats{
            .min = parsed_temp,
            .max = parsed_temp,
            .sum = parsed_temp, // Use sum instead of calculating running average
            .count = 1,
        };
    } else {
        var stats = entry.value_ptr;
        stats.min = @min(stats.min, parsed_temp);
        stats.max = @max(stats.max, parsed_temp);
        stats.sum += parsed_temp; // Much faster than running average
        stats.count += 1;
    }
}
