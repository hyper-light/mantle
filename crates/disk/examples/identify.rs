//! Prints what the OS says about the storage under each path argument.
#![allow(clippy::disallowed_macros)]

fn main() {
    for path in std::env::args().skip(1) {
        println!(
            "{path}: {:#?}",
            mantle_disk::probe::identify(std::path::Path::new(&path))
        );
    }
}
