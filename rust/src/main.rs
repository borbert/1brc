use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

fn main() {
    let brc_path = Path::new("../data/measurements_1b.txt");

    // Check if file exists first
    if !brc_path.exists() {
        eprintln!("File not found: {}", brc_path.display());
        return;
    }

    println!("Starting buffered file reading...");
    read_file_buffered(brc_path);
}

fn read_file_buffered(path: &Path) {
    let start = std::time::Instant::now();

    // Open file with buffered reader instead of reading entire file
    let file = match File::open(path) {
        Ok(file) => file,
        Err(e) => {
            eprintln!("Error opening file: {}", e);
            return;
        }
    };

    let reader = BufReader::new(file);
    let mut line_count = 0;
    let mut first_lines = Vec::new();

    // Read line by line instead of loading everything into memory
    for (i, line_result) in reader.lines().enumerate() {
        match line_result {
            Ok(line) => {
                line_count += 1;

                // Store first 5 lines for display
                if i < 5 {
                    first_lines.push(line);
                }

                // Print progress every 10 million lines
                if line_count % 100_000_000 == 0 {
                    let elapsed = start.elapsed();
                    println!(
                        "Processed {} million lines in {:?}",
                        line_count / 1_000_000,
                        elapsed
                    );
                }
            }
            Err(e) => {
                eprintln!("Error reading line {}: {}", i + 1, e);
                return;
            }
        }
    }

    let duration = start.elapsed();

    println!("File read successfully with buffered reader!");
    println!("Lines processed: {}", line_count);

    // Print first few lines
    for (i, line) in first_lines.iter().enumerate() {
        println!("Line {}: {}", i + 1, line);
    }

    println!("Time taken: {:?}", duration);

    // Calculate processing rate
    let lines_per_second = line_count as f64 / duration.as_secs_f64();
    println!("Processing rate: {:.0} lines/second", lines_per_second);

    // Estimate memory usage (much lower now!)
    let estimated_memory = std::mem::size_of::<BufReader<File>>() + 8192; // Default buffer size
    println!("Estimated memory usage: {}", format_bytes(estimated_memory));
}

fn format_bytes(bytes: usize) -> String {
    const KB: usize = 1_024;
    const MB: usize = KB * 1_024;
    const GB: usize = MB * 1_024;
    const TB: usize = GB * 1_024;

    if bytes >= TB {
        format!("{:.2} TB", bytes as f64 / TB as f64)
    } else if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} bytes", bytes)
    }
}
