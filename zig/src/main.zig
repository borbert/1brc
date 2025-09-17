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

const WorkerResult = struct {
    cityTemps: std.StringHashMapUnmanaged(CityStats) = .{},
    string_pool: std.StringHashMapUnmanaged([]const u8) = .{},
    lines_processed: usize = 0,
};

pub fn main() !void {
    var timer = std.time.Timer.start() catch unreachable;
    const allocator = std.heap.page_allocator;

    const file_path = "../data/measurements_1b.txt";
    var file = try std.fs.cwd().openFile(file_path, .{});
    defer file.close();

    const file_stats = try file.stat();
    const mapped_memory = try std.posix.mmap(null, file_stats.size, std.posix.PROT.READ, .{ .TYPE = .PRIVATE }, file.handle, 0);
    defer std.posix.munmap(mapped_memory);

    // Use CPU count for threads (cap at 8 for reasonable memory usage)
    const cpu_count = try std.Thread.getCpuCount();
    const thread_count = @min(cpu_count, 8);

    var threads = try allocator.alloc(std.Thread, thread_count);
    defer allocator.free(threads);

    var results = try allocator.alloc(WorkerResult, thread_count);
    defer {
        for (results) |*result| {
            // Clean up each worker's maps
            var pool_it = result.string_pool.iterator();
            while (pool_it.next()) |entry| {
                allocator.free(entry.value_ptr.*);
            }
            result.string_pool.deinit(allocator);
            result.cityTemps.deinit(allocator);
        }
        allocator.free(results);
    }

    // Split work into chunks, ensuring we don't split lines
    const chunk_size = mapped_memory.len / thread_count;
    for (0..thread_count) |i| {
        const start = i * chunk_size;
        var end = if (i == thread_count - 1) mapped_memory.len else (i + 1) * chunk_size;

        // Adjust end to complete line boundary (except for last chunk)
        if (i < thread_count - 1) {
            while (end < mapped_memory.len and mapped_memory[end] != '\n') {
                end += 1;
            }
            if (end < mapped_memory.len) end += 1; // Include the newline
        }

        threads[i] = try std.Thread.spawn(.{}, workerThread, .{ mapped_memory[start..end], &results[i], allocator });
    }

    // Wait for all threads
    for (threads) |thread| {
        thread.join();
    }

    // Merge all results into final map
    var final_temps = std.StringHashMapUnmanaged(CityStats){};
    defer final_temps.deinit(allocator);

    var total_lines: usize = 0;
    for (results) |*result| {
        total_lines += result.lines_processed;

        var it = result.cityTemps.iterator();
        while (it.next()) |entry| {
            const final_entry = try final_temps.getOrPut(allocator, entry.key_ptr.*);
            if (!final_entry.found_existing) {
                final_entry.value_ptr.* = entry.value_ptr.*;
            } else {
                // Merge stats
                var stats = final_entry.value_ptr;
                stats.min = @min(stats.min, entry.value_ptr.min);
                stats.max = @max(stats.max, entry.value_ptr.max);
                stats.sum += entry.value_ptr.sum;
                stats.count += entry.value_ptr.count;
            }
        }
    }

    const elapsed = timer.read();
    const elapsed_ms = elapsed / 1_000_000;
    const seconds = elapsed_ms / 1_000;
    const millis = elapsed_ms % 1_000;

    std.debug.print("Lines read: {d}\n", .{total_lines});
    std.debug.print("Threads used: {d}\n", .{thread_count});
    std.debug.print("Unique cities: {d}\n", .{final_temps.count()});
    std.debug.print("Elapsed time: {d} sec {d} ms\n", .{ seconds, millis });
}

// fn processLine(line: []const u8, cityTemps: *std.StringHashMapUnmanaged(CityStats), string_pool: *std.StringHashMapUnmanaged([]const u8), allocator: std.mem.Allocator) !void {
//     // Fast semicolon search - avoid iterator overhead
//     const semicolon_pos = std.mem.indexOfScalar(u8, line, ';') orelse return;
//     if (semicolon_pos == 0 or semicolon_pos >= line.len - 1) return;

//     const city = line[0..semicolon_pos];
//     const temp_str = line[semicolon_pos + 1 ..];

//     // Skip trimming if not needed (most lines won't need it)
//     const clean_temp = if (temp_str[temp_str.len - 1] == '\r')
//         temp_str[0 .. temp_str.len - 1]
//     else
//         temp_str;

//     // const parsed_temp = std.fmt.parseFloat(f64, clean_temp) catch return;
//     const parsed_temp = fastParseFloat(clean_temp) catch return;

//     // Get interned city string (avoids duplicate allocations)
//     const interned_city = blk: {
//         const pool_entry = try string_pool.getOrPut(allocator, city);
//         if (!pool_entry.found_existing) {
//             pool_entry.value_ptr.* = try allocator.dupe(u8, city);
//         }
//         break :blk pool_entry.value_ptr.*;
//     };

//     const entry = try cityTemps.getOrPut(allocator, interned_city);
//     if (!entry.found_existing) {
//         entry.value_ptr.* = CityStats{
//             .min = parsed_temp,
//             .max = parsed_temp,
//             .sum = parsed_temp, // Use sum instead of calculating running average
//             .count = 1,
//         };
//     } else {
//         var stats = entry.value_ptr;
//         stats.min = @min(stats.min, parsed_temp);
//         stats.max = @max(stats.max, parsed_temp);
//         stats.sum += parsed_temp; // Much faster than running average
//         stats.count += 1;
//     }
// }
fn processLine(line: []const u8, cityTemps: *std.StringHashMapUnmanaged(CityStats), string_pool: *std.StringHashMapUnmanaged([]const u8), allocator: std.mem.Allocator) !void {
    // Fast semicolon search - avoid iterator overhead
    const semicolon_pos = std.mem.indexOfScalar(u8, line, ';') orelse return;
    if (semicolon_pos == 0 or semicolon_pos >= line.len - 1) return;

    const city = line[0..semicolon_pos];
    const temp_str = line[semicolon_pos + 1 ..];

    // Skip trimming if not needed (most lines won't need it)
    const clean_temp = if (temp_str.len > 0 and temp_str[temp_str.len - 1] == '\r')
        temp_str[0 .. temp_str.len - 1]
    else
        temp_str;

    const parsed_temp = fastParseFloat(clean_temp) catch return;

    // Try to find existing city first (most common case after initial phase)
    if (cityTemps.getPtr(city)) |stats| {
        // Hot path - city already exists
        stats.min = @min(stats.min, parsed_temp);
        stats.max = @max(stats.max, parsed_temp);
        stats.sum += parsed_temp;
        stats.count += 1;
    } else {
        // Cold path - new city, need to intern string
        const pool_entry = try string_pool.getOrPut(allocator, city);
        if (!pool_entry.found_existing) {
            pool_entry.value_ptr.* = try allocator.dupe(u8, city);
        }
        const interned_city = pool_entry.value_ptr.*;

        try cityTemps.put(allocator, interned_city, CityStats{
            .min = parsed_temp,
            .max = parsed_temp,
            .sum = parsed_temp,
            .count = 1,
        });
    }
}

fn workerThread(chunk: []const u8, result: *WorkerResult, allocator: std.mem.Allocator) void {
    var start: usize = 0;

    for (chunk, 0..) |b, i| {
        if (b == '\n') {
            if (i > start) {
                processLine(chunk[start..i], &result.cityTemps, &result.string_pool, allocator) catch {};
                result.lines_processed += 1;
            }
            start = i + 1;
        }
    }

    if (start < chunk.len) {
        processLine(chunk[start..], &result.cityTemps, &result.string_pool, allocator) catch {};
        result.lines_processed += 1;
    }
}

// custom parse float function
fn fastParseFloat(str: []const u8) !f64 {
    if (str.len == 0) return error.InvalidCharacter;

    var result: f64 = 0;
    var decimal_places: f64 = 0;
    var negative = false;
    var i: usize = 0;

    // Handle negative sign
    if (str[0] == '-') {
        negative = true;
        i = 1;
    }

    // Parse integer part
    while (i < str.len and str[i] != '.') {
        const digit = str[i] - '0';
        if (digit > 9) return error.InvalidCharacter;
        result = result * 10 + @as(f64, @floatFromInt(digit));
        i += 1;
    }

    // Parse decimal part if present
    if (i < str.len and str[i] == '.') {
        i += 1;
        while (i < str.len) {
            const digit = str[i] - '0';
            if (digit > 9) return error.InvalidCharacter;
            decimal_places += 1;
            result = result * 10 + @as(f64, @floatFromInt(digit));
            i += 1;
        }

        // Divide by appropriate power of 10
        var divisor: f64 = 1;
        var places = decimal_places;
        while (places > 0) {
            divisor *= 10;
            places -= 1;
        }
        result /= divisor;
    }

    return if (negative) -result else result;
}
