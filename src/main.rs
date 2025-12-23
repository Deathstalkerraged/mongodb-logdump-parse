use csv::Reader;
use serde_json::Value;
use std::collections::{HashMap, BTreeMap};
use std::error::Error;
use std::fs::File;

#[derive(Debug, Clone)]
struct QueryPattern {
    collection: String,
    operation: String,
    filter_fields: Vec<String>,
    sort_fields: Vec<String>,
    index_used: String,
    plan_summary: String,
    duration_ms: Option<i64>,
    field_values: HashMap<String, String>,
}

impl std::fmt::Display for QueryPattern {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let mut parts = vec![
            format!("Collection: {}", self.collection),
            format!("Operation: {}", self.operation),
        ];
        
        if !self.filter_fields.is_empty() {
            parts.push(format!("Filter fields: [{}]", self.filter_fields.join(", ")));
        }
        
        if !self.sort_fields.is_empty() {
            parts.push(format!("Sort fields: [{}]", self.sort_fields.join(", ")));
        }
        
        if !self.plan_summary.is_empty() && self.plan_summary != "unknown" {
            parts.push(format!("Plan: {}", self.plan_summary));
        }
        
        if !self.index_used.is_empty() && self.index_used != "unknown" {
            parts.push(format!("Index: {}", self.index_used));
        }
        
        write!(f, "{}", parts.join(" | "))
    }
}

fn extract_fields_from_object(obj: &Value) -> Vec<String> {
    let mut fields = Vec::new();
    
    match obj {
        Value::Object(map) => {
            for key in map.keys() {
                if !key.starts_with('$') && key != "_id" {
                    fields.push(key.clone());
                }
            }
        }
        _ => {}
    }
    
    fields.sort();
    fields
}

fn extract_field_values_from_object(obj: &Value, prefix: &str) -> HashMap<String, String> {
    extract_field_values_from_object_with_depth(obj, prefix, 0, 3)
}

fn extract_field_values_from_object_with_depth(obj: &Value, prefix: &str, current_depth: usize, max_depth: usize) -> HashMap<String, String> {
    let mut field_values = HashMap::new();
    
    if current_depth >= max_depth {
        return field_values;
    }
    
    match obj {
        Value::Object(map) => {
            for (key, value) in map.iter() {
                if !key.starts_with('$') && key != "_id" {
                    let field_name = if prefix.is_empty() {
                        key.clone()
                    } else {
                        format!("{}.{}", prefix, key)
                    };
                    
                    match value {
                        Value::String(s) => {
                            // Limit string length to avoid huge values
                            if s.len() <= 50 {
                                field_values.insert(field_name, s.clone());
                            } else {
                                field_values.insert(field_name, format!("{}...", &s[..47]));
                            }
                        }
                        Value::Number(n) => {
                            field_values.insert(field_name, n.to_string());
                        }
                        Value::Bool(b) => {
                            field_values.insert(field_name, b.to_string());
                        }
                        Value::Array(arr) => {
                            if arr.len() <= 3 {
                                let arr_str = arr.iter()
                                    .map(|v| match v {
                                        Value::String(s) => s.clone(),
                                        _ => v.to_string(),
                                    })
                                    .collect::<Vec<_>>()
                                    .join(",");
                                field_values.insert(field_name, format!("[{}]", arr_str));
                            } else {
                                field_values.insert(field_name, format!("[{} items]", arr.len()));
                            }
                        }
                        Value::Object(nested_obj) => {
                            // Recursively parse nested objects with limited depth
                            let nested_values = extract_field_values_from_object_with_depth(value, &field_name, current_depth + 1, max_depth);
                            field_values.extend(nested_values);
                        }
                        _ => {}
                    }
                }
            }
        }
        _ => {}
    }
    
    field_values
}

fn parse_query_pattern(json_str: &str) -> Option<QueryPattern> {
    let parsed: Value = serde_json::from_str(json_str).ok()?;
    
    let mut collection = String::new();
    let mut operation = String::new();
    let mut filter_fields = Vec::new();
    let mut sort_fields = Vec::new();
    let mut plan_summary = "unknown".to_string();
    let mut field_values = HashMap::new();
    let mut duration_ms = None;
    
    // Extract namespace (collection)
    if let Some(ns) = parsed.get("attr")?.get("ns")?.as_str() {
        if let Some(dot_pos) = ns.rfind('.') {
            collection = ns[dot_pos + 1..].to_string();
        }
    }
    
    // Extract plan summary
    if let Some(plan) = parsed.get("attr")?.get("planSummary")?.as_str() {
        plan_summary = plan.to_string();
    }
    
    // Extract duration
    if let Some(duration) = parsed.get("attr")?.get("durationMillis")?.as_i64() {
        duration_ms = Some(duration);
    }
    
    // Extract command details
    if let Some(command_obj) = parsed.get("attr")?.get("command") {
        // Determine operation type
        if command_obj.get("find").is_some() {
            operation = "find".to_string();
        } else if command_obj.get("getMore").is_some() {
            operation = "getMore".to_string();
        } else if command_obj.get("listDatabases").is_some() {
            operation = "listDatabases".to_string();
        } else {
            operation = "other".to_string();
        }
        
        // Extract filter fields and values
        if let Some(filter_obj) = command_obj.get("filter") {
            filter_fields = extract_fields_from_object(filter_obj);
            let filter_values = extract_field_values_from_object(filter_obj, "");
            field_values.extend(filter_values);
        }
        
        // Extract sort fields
        if let Some(sort_obj) = command_obj.get("sort") {
            sort_fields = extract_fields_from_object(sort_obj);
        }
    }
    
    // For getMore, try to get originating command
    if operation == "getMore" {
        if let Some(orig_command) = parsed.get("attr")?.get("originatingCommand") {
            if let Some(filter_obj) = orig_command.get("filter") {
                filter_fields = extract_fields_from_object(filter_obj);
                let filter_values = extract_field_values_from_object(filter_obj, "");
                field_values.extend(filter_values);
            }
            if let Some(sort_obj) = orig_command.get("sort") {
                sort_fields = extract_fields_from_object(sort_obj);
            }
        }
    }
    
    if collection.is_empty() && operation.is_empty() {
        return None;
    }
    
    Some(QueryPattern {
        collection,
        operation,
        filter_fields,
        sort_fields,
        index_used: "unknown".to_string(),
        plan_summary,
        duration_ms,
        field_values,
    })
}

fn find_query_patterns_in_braces(csv_path: &str) -> Result<Vec<(QueryPattern, usize)>, Box<dyn Error>> {
    let file = File::open(csv_path)?;
    let mut reader = csv::ReaderBuilder::new()
        .flexible(true)
        .from_reader(file);
    
    let mut pattern_counts: HashMap<String, (QueryPattern, usize)> = HashMap::new();
    
    for result in reader.records() {
        let record = result?;
        for field in record.iter() {
            let field_trimmed = field.trim();
            
            // Find all text within curly braces
            let mut brace_depth = 0;
            let mut start_pos = None;
            
            for (i, ch) in field_trimmed.char_indices() {
                match ch {
                    '{' => {
                        if brace_depth == 0 {
                            start_pos = Some(i);
                        }
                        brace_depth += 1;
                    }
                    '}' => {
                        brace_depth -= 1;
                        if brace_depth == 0 {
                            if let Some(start) = start_pos {
                                let json_content = &field_trimmed[start..=i];
                                
                                if let Some(pattern) = parse_query_pattern(json_content) {
                                    let pattern_key = format!("{}", pattern);
                                    
                                    match pattern_counts.get_mut(&pattern_key) {
                                        Some((_, count)) => *count += 1,
                                        None => {
                                            pattern_counts.insert(pattern_key, (pattern, 1));
                                        }
                                    }
                                }
                            }
                            start_pos = None;
                        }
                    }
                    _ => {}
                }
            }
        }
    }
    
    if pattern_counts.is_empty() {
        return Err("No query patterns found within curly braces".into());
    }
    
    // Sort by count in descending order
    let mut sorted_patterns: Vec<(QueryPattern, usize)> = pattern_counts
        .into_values()
        .collect();
    sorted_patterns.sort_by(|a, b| b.1.cmp(&a.1));
    
    Ok(sorted_patterns)
}

fn analyze_collection_field_patterns(patterns: &[(QueryPattern, usize)]) -> BTreeMap<String, BTreeMap<String, usize>> {
    let mut collection_field_counts: BTreeMap<String, BTreeMap<String, usize>> = BTreeMap::new();
    
    for (pattern, count) in patterns {
        let collection_stats = collection_field_counts
            .entry(pattern.collection.clone())
            .or_insert_with(BTreeMap::new);
        
        // Count filter fields
        for field in &pattern.filter_fields {
            *collection_stats.entry(format!("filter:{}", field)).or_insert(0) += count;
        }
        
        // Count sort fields
        for field in &pattern.sort_fields {
            *collection_stats.entry(format!("sort:{}", field)).or_insert(0) += count;
        }
        
        // Count operation types
        *collection_stats.entry(format!("operation:{}", pattern.operation)).or_insert(0) += count;
        
        // Count plan types (especially COLLSCAN)
        if !pattern.plan_summary.is_empty() && pattern.plan_summary != "unknown" {
            *collection_stats.entry(format!("plan:{}", pattern.plan_summary)).or_insert(0) += count;
        }
    }
    
    collection_field_counts
}

fn analyze_field_value_distributions(patterns: &[(QueryPattern, usize)]) -> BTreeMap<String, BTreeMap<String, usize>> {
    let mut field_value_distributions: BTreeMap<String, BTreeMap<String, usize>> = BTreeMap::new();
    
    // Focus on the slowest queries (those with COLLSCAN or high occurrences)
    for (pattern, count) in patterns.iter() {
        // Only analyze patterns that are problematic (COLLSCAN or high frequency)
        if pattern.plan_summary == "COLLSCAN" || *count > 100 {
            for (field_name, field_value) in &pattern.field_values {
                let field_stats = field_value_distributions
                    .entry(format!("{}:{}", pattern.collection, field_name))
                    .or_insert_with(BTreeMap::new);
                
                *field_stats.entry(field_value.clone()).or_insert(0) += count;
            }
        }
    }
    
    field_value_distributions
}

fn main() -> Result<(), Box<dyn Error>> {
    let csv_file = "/Users/rahulhegde/Downloads/Untitled Discover session (5).csv";
    
    match find_query_patterns_in_braces(csv_file) {
        Ok(patterns) => {
            println!("MongoDB Slow Query Analysis");
            println!("{}", "=".repeat(100));
            
            // Overall patterns
            println!("\n📊 TOP 10 OVERALL QUERY PATTERNS:");
            println!("{}", "-".repeat(80));
            for (i, (pattern, count)) in patterns.iter().take(10).enumerate() {
                println!("{}. {} (appears {} times)", i + 1, pattern, count);
                
                // Suggest optimizations
                if pattern.plan_summary == "COLLSCAN" {
                    println!("   ⚠️  COLLSCAN detected - needs index");
                }
                if pattern.filter_fields.len() > 3 {
                    println!("   ⚠️  Complex filter with {} fields", pattern.filter_fields.len());
                }
            }
            
            // Collection-specific analysis
            let collection_analysis = analyze_collection_field_patterns(&patterns);
            let field_distributions = analyze_field_value_distributions(&patterns);
            
            println!("\n🔍 COLLECTION-SPECIFIC FIELD ANALYSIS:");
            println!("{}", "=".repeat(100));
            
            for (collection, field_stats) in collection_analysis.iter() {
                println!("\n📁 Collection: {}", collection);
                println!("{}", "-".repeat(60));
                
                // Sort fields by usage count
                let mut sorted_fields: Vec<(&String, &usize)> = field_stats.iter().collect();
                sorted_fields.sort_by(|a, b| b.1.cmp(a.1));
                
                // Show top problematic patterns for this collection
                let mut collscan_count = 0;
                let mut most_used_filters = Vec::new();
                let mut most_used_sorts = Vec::new();
                
                for (field_type, count) in sorted_fields.iter().take(10) {
                    if field_type.starts_with("plan:COLLSCAN") {
                        collscan_count = **count;
                    } else if field_type.starts_with("filter:") {
                        most_used_filters.push((field_type.strip_prefix("filter:").unwrap(), **count));
                    } else if field_type.starts_with("sort:") {
                        most_used_sorts.push((field_type.strip_prefix("sort:").unwrap(), **count));
                    }
                    
                    println!("  • {} → {} occurrences", field_type, count);
                }
                
                // Provide specific recommendations
                if collscan_count > 0 {
                    println!("  ⚠️  {} COLLECTION SCANS detected!", collscan_count);
                    
                    if !most_used_filters.is_empty() {
                        let top_filter_fields: Vec<&str> = most_used_filters.iter().take(3).map(|(f, _)| *f).collect();
                        println!("  💡 URGENT: Add index on frequently filtered fields: [{}]", top_filter_fields.join(", "));
                    }
                    
                    if !most_used_sorts.is_empty() {
                        let top_sort_fields: Vec<&str> = most_used_sorts.iter().take(2).map(|(f, _)| *f).collect();
                        println!("  💡 Consider compound index including sort fields: [{}]", top_sort_fields.join(", "));
                    }
                }
                
                // Show suggested compound indexes
                if !most_used_filters.is_empty() && !most_used_sorts.is_empty() {
                    let suggested_compound: Vec<String> = most_used_filters.iter().take(2)
                        .map(|(f, _)| f.to_string())
                        .chain(most_used_sorts.iter().take(1).map(|(f, _)| f.to_string()))
                        .collect();
                    
                    println!("  🎯 Suggested compound index: db.{}.createIndex({{ {} }})", 
                             collection, 
                             suggested_compound.iter().map(|f| format!("{}: 1", f)).collect::<Vec<_>>().join(", "));
                }
            }
            
            // Field value distribution analysis
            if !field_distributions.is_empty() {
                println!("\n📈 FIELD VALUE DISTRIBUTION ANALYSIS (Slowest Queries):");
                println!("{}", "=".repeat(100));
                
                for (field_key, value_counts) in field_distributions.iter() {
                    println!("\n🔢 Field: {}", field_key);
                    println!("{}", "-".repeat(50));
                    
                    // Sort values by frequency
                    let mut sorted_values: Vec<(&String, &usize)> = value_counts.iter().collect();
                    sorted_values.sort_by(|a, b| b.1.cmp(a.1));
                    
                    // Show top 10 most problematic values
                    for (i, (value, count)) in sorted_values.iter().take(10).enumerate() {
                        println!("  {}. '{}' → {} slow queries", i + 1, value, count);
                    }
                    
                    // Provide insights
                    let total_issues = sorted_values.iter().map(|(_, count)| *count).sum::<usize>();
                    let top_3_issues: usize = sorted_values.iter().take(3).map(|(_, count)| *count).sum();
                    let concentration = if total_issues > 0 {
                        (top_3_issues as f64 / total_issues as f64) * 100.0
                    } else {
                        0.0
                    };
                    
                    if concentration > 70.0 {
                        println!("  ⚠️  HIGH CONCENTRATION: Top 3 values cause {:.1}% of slow queries", concentration);
                        println!("  💡 Consider partitioning or specialized indexes for these values");
                    }
                    
                    if sorted_values.len() > 20 {
                        println!("  📊 High cardinality field ({} unique values) - review selectivity", sorted_values.len());
                    }
                }
            }
        }
        Err(e) => {
            eprintln!("Error: {}", e);
        }
    }
    
    Ok(())
}
