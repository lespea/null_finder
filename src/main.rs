extern crate crossbeam;
extern crate csv;
extern crate clap;
extern crate memchr;
extern crate num_cpus;
extern crate quick_csv;
extern crate regex;
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

static FILTER_CHAR: u8 = b'\x00';

enum Work {
    Quit,
    File(PathBuf),
}

struct HeaderRow {
    file: PathBuf,
    row: Vec<String>,
}

enum Finding {
    Header(HeaderRow),
    MatchedRow(Vec<String>),
}

fn proc_zip(path: PathBuf,
            csv_path: &regex::Regex,
            wanted_col: &str,
            sender: &Sender<Finding>)
            -> std::result::Result<(), std::io::Error> {
    let fh = File::open(path.as_path())?;
    let mut zip = zip::ZipArchive::new(BufReader::new(fh))?;

    for idx in 0..zip.len() {
        if let Ok(z_file) = zip.by_index(idx) {
            if !csv_path.is_match(z_file.name()) {
                continue;
            }
            let csv_path = format!("{}:{}", path.to_string_lossy(), z_file.name());
            println!("Processing '{}'", csv_path);

            let mut csv = quick_csv::Csv::from_reader(BufReader::new(z_file)).has_header(true);

            let headers = csv.headers();
            if !headers.is_empty() {
                let want_idx = headers.iter()
                    .enumerate()
                    .find(|&(_, h)| h.eq_ignore_ascii_case(wanted_col))
                    .map(|(i, _)| i);

                if let Some(idx) = want_idx {
                    let _ = sender.send(Finding::Header(HeaderRow {
                        file: path.clone(),
                        row: headers,
                    }));

                    for (row_num, row) in csv.filter_map(|e| e.ok()).enumerate() {
                        if let Some(data) = row.bytes_columns().nth(idx) {
                            if memchr::memchr(FILTER_CHAR, data).is_some() {
                                if let Ok(cols) = row.columns() {
                                    let mut data = Vec::with_capacity(cols.len() + 2);

                                    data.push(csv_path.to_string());
                                    data.push(format!("{}", row_num).to_string());
                                    data.extend(cols.map(|s| s.to_string()));

                                    let _ = sender.send(Finding::MatchedRow(data));
                                }
                            }
                        }
                    }
                };
            }
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

            Finding::MatchedRow(row) => {
                if let None = out_file {
                    let mut csv_writer = csv::Writer::from_file(out_path).unwrap();
                    let mut hrow = header.as_ref().expect("Row before header???").clone();
                    hrow.insert(0, "file_path".to_string());
                    hrow.insert(1, "line_num".to_string());
                    csv_writer.encode(hrow).unwrap();
                    out_file = Some(csv_writer);
                }

                out_file.as_mut().unwrap().encode(row).unwrap();
            }
        };
    }

    // If we didn't find anything, create an empty file so as not to confuse the
    // caller
    if let None = out_file {
        let mut csv_writer = csv::Writer::from_file(out_path).unwrap();
        if let Some(mut hrow) = header {
            hrow.insert(0, "file_path".to_string());
            csv_writer.encode(hrow).unwrap();
        }
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
        .about("Finds rows where the specified column starts with a null in the csv in the \
                embedded zips")
        .version("1.0")
        .author("Adam Lesperance <lespea@gmail.com>")
        .arg(Arg::with_name("wanted_col")
            .help("the column name to search")
            .short("c")
            .long("column")
            .default_value("Key"))
        .arg(Arg::with_name("csv_path")
            .help("regex of ")
            .short("p")
            .long("csv_regex")
            .default_value(r".*\wregistry.*\.csv$"))
        .arg(Arg::with_name("output_file")
            .help("where to write the findings to")
            .required(true))
        .arg(Arg::with_name("search_dirs")
            .help("the directories to search for zips")
            .multiple(true)
            .required(true))
        .get_matches();

    let wanted_col = args.value_of_lossy("wanted_col").unwrap();
    let file_regex = &regex::Regex::new(args.value_of_lossy("csv_path").unwrap().borrow()).unwrap();

    let out_path = args.value_of_lossy("output_file").unwrap();
    let in_dirs = args.values_of_lossy("search_dirs").unwrap();

    let num_workers = num_cpus::get();

    crossbeam::scope(move |scope| {
        let (mut worker, stealer) = chase_lev::deque();
        let (found_tx, found_rx) = channel();

        for _ in 0..num_workers {
            let stealer = stealer.clone();
            let found_tx = found_tx.clone();

            let wanted_col = wanted_col.clone();

            scope.spawn(move || loop {
                match stealer.steal() {
                    Steal::Empty | Steal::Abort => (),
                    Steal::Data(d) => {
                        match d {
                            Work::Quit => break,
                            Work::File(path) => {
                                let _ = proc_zip(path, file_regex, wanted_col.borrow(), &found_tx);
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
