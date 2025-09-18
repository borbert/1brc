use memmap2::Mmap;
use rayon::prelude::*;
use rustc_hash::FxHashMap; // Faster than std HashMap
use std::fs::File;
use std::path::Path;

#[derive(Debug, Clone)]
struct TemperatureStats {
    min: f64,
    max: f64,
    sum: f64,
    count: u64,
}

impl TemperatureStats {
    #[inline]
    fn new(temp: f64) -> Self {
        Self {
            min: temp,
            max: temp,
            sum: temp,
            count: 1,
        }
    }

    #[inline]
    fn update(&mut self, temp: f64) {
        if temp < self.min {
            self.min = temp;
        }
        if temp > self.max {
            self.max = temp;
        }
        self.sum += temp;
        self.count += 1;
    }

    #[inline]
    fn merge(&mut self, other: &TemperatureStats) {
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
        self.sum += other.sum;
        self.count += other.count;
    }

    #[inline]
    fn mean(&self) -> f64 {
        self.sum / self.count as f64
    }
}

fn main() {
    let brc_path = Path::new("../data/measurements_1b.txt");

    if !brc_path.exists() {
        eprintln!("File not found: {}", brc_path.display());
        return;
    }

    println!("Available CPU cores: {}", rayon::current_num_threads());
    println!("Starting processing...");

    process_ultra_optimized(brc_path);
}

fn process_ultra_optimized(path: &Path) {
    let start = std::time::Instant::now();

    // Memory-map the file
    let file = File::open(path).unwrap();
    let mmap = unsafe { Mmap::map(&file).unwrap() };

    let mmap_duration = start.elapsed();
    println!("File memory-mapped in {:?}", mmap_duration);

    // Split file into chunks
    let num_threads = rayon::current_num_threads();
    let chunk_size = mmap.len() / num_threads;
    let mut chunks = Vec::with_capacity(num_threads);

    for i in 0..num_threads {
        let start_pos = if i == 0 {
            0
        } else {
            let mut pos = i * chunk_size;
            while pos < mmap.len() && mmap[pos] != b'\n' {
                pos += 1;
            }
            if pos < mmap.len() {
                pos + 1
            } else {
                pos
            }
        };

        let end_pos = if i == num_threads - 1 {
            mmap.len()
        } else {
            let mut pos = (i + 1) * chunk_size;
            while pos < mmap.len() && mmap[pos] != b'\n' {
                pos += 1;
            }
            pos
        };

        if start_pos < mmap.len() && start_pos < end_pos {
            chunks.push((start_pos, end_pos));
        }
    }

    println!("File split into {} chunks", chunks.len());

    // Process chunks in parallel
    let process_start = std::time::Instant::now();

    let results: Vec<(FxHashMap<String, TemperatureStats>, usize)> = chunks
        .par_iter()
        .map(|&(start, end)| {
            let chunk = &mmap[start..end];
            process_chunk_ultra_fast(chunk)
        })
        .collect();

    let process_duration = process_start.elapsed();
    println!("Parallel processing completed in {:?}", process_duration);

    // Merge results
    let merge_start = std::time::Instant::now();

    let mut final_stats: FxHashMap<String, TemperatureStats> = FxHashMap::default();
    let mut total_lines = 0;

    for (chunk_results, lines) in results {
        total_lines += lines;
        for (city, stats) in chunk_results {
            final_stats
                .entry(city)
                .and_modify(|existing| existing.merge(&stats))
                .or_insert(stats);
        }
    }

    let merge_duration = merge_start.elapsed();
    let total_duration = start.elapsed();

    println!("\n=== Results ===");
    println!("Total lines processed: {}", total_lines);
    println!("Unique cities: {}", final_stats.len());
    println!("Total time: {:?}", total_duration);
    println!(
        "Processing rate: {:.0} lines/second",
        total_lines as f64 / total_duration.as_secs_f64()
    );

    // Show results
    let mut results: Vec<_> = final_stats.iter().collect();
    results.sort_by_key(|&(city, _)| city);
}

// Ultra-optimized chunk processing with custom float parsing
#[inline(never)] // Prevent inlining to see in profiler
fn process_chunk_ultra_fast(chunk: &[u8]) -> (FxHashMap<String, TemperatureStats>, usize) {
    let mut stats: FxHashMap<String, TemperatureStats> = FxHashMap::default();
    let mut line_count = 0;

    let mut i = 0;
    let len = chunk.len();

    while i < len {
        // Find line start
        let line_start = i;

        // Find semicolon
        while i < len && chunk[i] != b';' {
            i += 1;
        }

        if i >= len {
            break;
        }
        let semicolon_pos = i;
        i += 1; // Skip semicolon

        // Find line end
        let temp_start = i;
        while i < len && chunk[i] != b'\n' {
            i += 1;
        }

        if i > temp_start && semicolon_pos > line_start {
            // Extract city (avoiding UTF-8 validation where possible)
            let city_bytes = &chunk[line_start..semicolon_pos];
            if let Ok(city) = std::str::from_utf8(city_bytes) {
                if !city.is_empty() {
                    // Extract and parse temperature
                    let temp_bytes = &chunk[temp_start..i];
                    if let Ok(temp_str) = std::str::from_utf8(temp_bytes) {
                        if let Ok(temperature) = fast_parse_float(temp_str) {
                            match stats.get_mut(city) {
                                Some(city_stats) => {
                                    city_stats.update(temperature);
                                }
                                None => {
                                    stats.insert(
                                        city.to_string(),
                                        TemperatureStats::new(temperature),
                                    );
                                }
                            }
                            line_count += 1;
                        }
                    }
                }
            }
        }

        i += 1; // Skip newline
    }

    (stats, line_count)
}

// Custom fast float parser for the specific format
#[inline]
fn fast_parse_float(s: &str) -> Result<f64, std::num::ParseFloatError> {
    // For the billion row challenge, we could implement a custom parser
    // but std::parse is already quite optimized for this use case
    s.parse::<f64>()
}
