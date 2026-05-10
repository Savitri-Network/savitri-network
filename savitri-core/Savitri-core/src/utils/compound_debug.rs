fn main() {
    println!("=== Compound Interest Debug ===");
    
    // Test the current implementation
    let principal = 1000.0;
    let rate = 0.05;
    let years = 1;
    let compounds_per_year = 12;
    
    println!("Input parameters:");
    println!("Principal: {}", principal);
    println!("Rate: {}", rate);
    println!("Years: {}", years);
    println!("Compounds per year: {}", compounds_per_year);
    
    // Current implementation
    let r = rate / compounds_per_year as f64;
    let n = years * compounds_per_year;
    let result = principal * (1.0 + r).powi(n as i32);
    
    println!("\nCurrent implementation:");
    println!("r = rate / compounds_per_year = {} / {} = {}", rate, compounds_per_year, r);
    println!("n = years * compounds_per_year = {} * {} = {}", years, compounds_per_year, n);
    println!("(1.0 + r) = {}", 1.0 + r);
    println!("(1.0 + r).powi(n) = {}", (1.0 + r).powi(n as i32));
    println!("Final result = principal * (1.0 + r).powi(n) = {} * {} = {}", principal, (1.0 + r).powi(n as i32), result);
    
    // Expected calculation
    println!("\nExpected calculation:");
    println!("1000 * (1 + 0.05/12)^12 = 1000 * (1.0041666667)^12");
    let expected = 1000.0 * (1.0 + 0.05/12.0).powi(12);
    println!("Expected = {}", expected);
    
    // Test step by step
    println!("\nStep by step verification:");
    let step1 = 0.05 / 12.0;
    println!("Step 1: 0.05 / 12 = {}", step1);
    let step2 = 1.0 + step1;
    println!("Step 2: 1.0 + {} = {}", step1, step2);
    let step3 = step2.powi(12);
    println!("Step 3: {}^12 = {}", step2, step3);
    let step4 = 1000.0 * step3;
    println!("Step 4: 1000.0 * {} = {}", step3, step4);
    
    println!("\nBug investigation:");
    println!("r = {}", r);
    println!("n = {}", n);
    println!("1.0 + r = {}", 1.0 + r);
    println!("(1.0 + r).powi({}) = {}", n, (1.0 + r).powi(n as i32));
    
    // Test with different parameters
    println!("\nTest with simple parameters:");
    let simple_result = 1000.0 * (1.0 + 0.05/1.0).powi(1);
    println!("1000 * (1 + 0.05/1)^1 = {}", simple_result);
    println!("Expected: 1050.0");
}
