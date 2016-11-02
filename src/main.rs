extern crate crossbeam;
extern crate walkdir;
extern crate zip;
extern crate quick_csv;
extern crate num_cpus;

use crossbeam::sync::chase_lev;
use crossbeam::sync::chase_lev::Steal;
use std::ascii::AsciiExt;
use std::borrow::Borrow;
use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
use std::str;
use std::sync::mpsc::{Sender, channel};
use walkdir::WalkDir;

static WANTED_COL: &'static str = "b";
static ZIP_FILE: &'static str = r"t/q.csv";
static FILTER_CHAR: char = '2';

enum Work {
    Quit,
    File(PathBuf),
}

#[derive(Debug)]
struct Row {
    file: PathBuf,
    row: Vec<String>,
}

enum Finding {
    Header(Row),
    MatchedRow(Row),
}

fn proc_zip(path: PathBuf, sender: &Sender<Finding>) -> std::result::Result<(), std::io::Error> {
    let fh = File::open(path.as_path())?;
    let mut zip = zip::ZipArchive::new(BufReader::new(fh))?;

    if let Ok(z_file) = zip.by_name(ZIP_FILE) {
        let mut csv = quick_csv::Csv::from_reader(BufReader::new(z_file)).has_header(true);

        let headers = csv.headers();
        if !headers.is_empty() {
            let want_idx = headers.iter()
                .enumerate()
                .find(|&(_, h)| h.eq_ignore_ascii_case(WANTED_COL))
                .map(|(i, _)| i);

            if let Some(idx) = want_idx {
                let _ = sender.send(Finding::Header(Row {
                    file: path.clone(),
                    row: headers,
                }));

                for row in csv.filter_map(|e| e.ok()) {
                    if let Ok(mut cols) = row.columns() {
                        if let Some(data) = cols.nth(idx) {
                            if data.starts_with(FILTER_CHAR) {
                                if let Ok(cols) = row.columns() {
                                    let _ = sender.send(Finding::MatchedRow(Row {
                                        file: path.clone(),
                                        row: cols.map(|c| c.to_string()).collect(),
                                    }));
                                }
                            }
                        }
                    }
                }
            };
        }
    }

    Ok(())
}

fn main() {
    crossbeam::scope(|scope| {
        let (mut worker, stealer) = chase_lev::deque();
        let (found_tx, found_rx) = channel();

        for _ in 0..num_cpus::get() {
            let stealer = stealer.clone();
            let found_tx = found_tx.clone();

            scope.spawn(move || loop {
                match stealer.steal() {
                    Steal::Empty | Steal::Abort => (),
                    Steal::Data(d) => {
                        match d {
                            Work::Quit => break,
                            Work::File(path) => {
                                let _ = proc_zip(path, &found_tx);
                            }
                        };
                    }
                };
            });
        }

        scope.spawn(move || {
            //            let mut header = None;

            for finding in found_rx {
                match finding {
                    Finding::Header(h) => (),

                    Finding::MatchedRow(r) => println!("Row :: {:?}", r),
                }
            }
        });

        for entry in WalkDir::new(".")
            .follow_links(true)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| {
                let name = e.file_name().to_string_lossy();
                let str: &str = name.borrow();

                if str.len() > 4 {
                    str[str.len() - 4..].eq_ignore_ascii_case(".zip")
                } else {
                    false
                }
            }) {
            worker.push(Work::File(entry.path().to_path_buf()))
        }

        for _ in 0..num_cpus::get() {
            worker.push(Work::Quit)
        }
    })
}
