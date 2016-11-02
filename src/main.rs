extern crate crossbeam;
extern crate csv;
extern crate clap;
extern crate num_cpus;
extern crate quick_csv;
extern crate walkdir;
extern crate zip;

use clap::{Arg, App};
use crossbeam::sync::chase_lev;
use crossbeam::sync::chase_lev::{Steal, Worker};
use std::ascii::AsciiExt;
use std::borrow::Borrow;
use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
use std::str;
use std::sync::mpsc::{Receiver, Sender, channel};
use walkdir::WalkDir;

static WANTED_COL: &'static str = "b";
static ZIP_FILE: &'static str = r"t/q.csv";
static FILTER_CHAR: char = '2';


enum Work {
    Quit,
    File(PathBuf),
}

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

fn proc_findings(out_path: &str, found_rx: Receiver<Finding>) {
    let mut header = None;
    let mut out_file = None;

    for finding in found_rx {
        match finding {
            Finding::Header(h) => {
                match header {
                    Some(ref cur_header) => {
                        if *cur_header != h.row {
                            panic!("The header from '{:?}' ({:?}) doesn't match the \
                                            expected header ({:?})",
                                   h.file,
                                   h.row,
                                   *cur_header)
                        }
                    }
                    None => header = Some(h.row),
                };
            }

            Finding::MatchedRow(r) => {
                if let None = out_file {
                    let mut csv_writer = csv::Writer::from_file(out_path).unwrap();
                    let mut hrow = header.as_ref().expect("Row before header???").clone();
                    hrow.insert(0, "file_path".to_string());
                    csv_writer.encode(hrow).unwrap();
                    out_file = Some(csv_writer);
                }

                let mut row = r.row;
                row.insert(0,
                           r.file
                               .to_string_lossy()
                               .as_ref()
                               .to_string());
                out_file.as_mut().unwrap().encode(row).unwrap();
            }
        };
    }
}

fn find_and_proc_zips(start_dirs: Vec<String>, worker: &mut Worker<Work>) {
    for start_dir in start_dirs {
        for entry in WalkDir::new(start_dir)
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
    }
}

fn main() {
    let args = App::new("null_finder")
        .about("Finds rows with nulls in csvs in embedded zips")
        .version("1.0")
        .author("Adam Lesperance <lespea@gmail.com>")
        .arg(Arg::with_name("output_file")
            .help("where to write the findings to")
            .required(true))
        .arg(Arg::with_name("search_dirs")
            .help("the directories to search for zips")
            .multiple(true)
            .required(true))
        .get_matches();

    let out_path = args.value_of_lossy("output_file").unwrap();
    let in_dirs = args.values_of_lossy("search_dirs").unwrap();

    let num_workers = num_cpus::get();

    crossbeam::scope(move |scope| {
        let (mut worker, stealer) = chase_lev::deque();
        let (found_tx, found_rx) = channel();

        for _ in 0..num_workers {
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

        scope.spawn(move || proc_findings(out_path.borrow(), found_rx));

        find_and_proc_zips(in_dirs, &mut worker);

        for _ in 0..num_workers {
            worker.push(Work::Quit)
        }
    })
}
