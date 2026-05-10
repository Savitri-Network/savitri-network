// Check compound interest formula
fn main() {
    println!("=== Formula Verification ===");
    
    // Manual calculation
    let principal = 1000.0;
    let rate = 0.05;
    let years = 1;
    let compounds_per_year = 12;
    
    // Step by step calculation
    let r = rate / compounds_per_year as f64;
    let n = years * compounds_per_year;
    let result = principal * (1.0 + r).powi(n as i32);
    
    println!("Principal: {}", principal);
    println!("Rate: {}", rate);
    println!("Years: {}", years);
    println!("Compounds per year: {}", compounds_per_year);
    println!("r = rate / compounds_per_year = {} / {} = {}", rate, compounds_per_year, r);
    println!("n = years * compounds_per_year = {} * {} = {}", years, compounds_per_year, n);
    println!("(1.0 + r) = {}", 1.0 + r);
    println!("(1.0 + r).powi({}) = {}", n, (1.0 + r).powi(n as i32));
    println!("Final result: {} * {} = {}", principal, (1.0 + r).powi(n as i32), result);
    
    // Expected calculation
    let expected = 1000.0 * (1.0 + 0.05/12.0).powi(12);
    println!("Expected: 1000 * (1 + 0.05/12)^12 = {}", expected);
    
    println!("Difference: {}", (result - expected).abs());
    
    // Check if parameters are swapped
    println!("\n=== Check if parameters are swapped ===");
    let swapped_result = 1000.0 * (1.0 + 1.0/12.0).powi(12 * 0.05 as i32);
    println!("If years and rate were swapped: {}", swapped_result);
    
    let alt_swapped = 1000.0 * (1.0 + 12.0/1.0).powi(0.05 * 1 as i32);
    println!("If compounds and years were swapped: {}", alt_swapped);
}
