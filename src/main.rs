extern crate walkdir;
extern crate zip;

use std::ascii::AsciiExt;
use std::ops::Deref;
use walkdir::WalkDir;

fn main() {
    for entry in WalkDir::new(".")
        .follow_links(true)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_string_lossy()
                .deref()
                .rsplit('.')
                .nth(0)
                .map(|s| s.eq_ignore_ascii_case("zip"))
                .unwrap_or_else(|| false)
        }) {
        println!("{:?}", entry);
    }
}
