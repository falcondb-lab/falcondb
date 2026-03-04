use std::os::windows::fs::OpenOptionsExt;
use std::io::Write;
use std::time::Instant;
use std::fs::OpenOptions;

fn bench(path: &str, n: usize) {
    let f = OpenOptions::new()
        .create(true).write(true).truncate(true)
        .custom_flags(0x80000000) // FILE_FLAG_WRITE_THROUGH
        .open(path).unwrap();
    let mut f = std::io::BufWriter::with_capacity(0, f); // no buffering
    let data = vec![0x42u8; 200];
    // warmup
    for _ in 0..100 { f.get_mut().write_all(&data).unwrap(); }
    
    let start = Instant::now();
    for _ in 0..n { f.get_mut().write_all(&data).unwrap(); }
    let elapsed = start.elapsed();
    
    let avg_us = elapsed.as_secs_f64() * 1_000_000.0 / n as f64;
    let tps = n as f64 / elapsed.as_secs_f64();
    println!("  {}: {} writes in {:.1}ms, avg={:.1}us, tps={:.0}", 
        path, n, elapsed.as_millis(), avg_us, tps);
    std::fs::remove_file(path).ok();
}

fn main() {
    let n = 5000;
    println!("FUA write latency benchmark ({n} x 200 bytes):");
    bench("H:\\fua_test.bin", n);
    bench("C:\\fua_test.bin", n);
}
