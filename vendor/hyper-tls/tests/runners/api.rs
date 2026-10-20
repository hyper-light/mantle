//! Upstream rustls's API tests, run against hyper-tls (test code: the no-panic wall is off).
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable,
    clippy::todo,
    clippy::unimplemented,
    clippy::indexing_slicing,
    clippy::string_slice,
    clippy::arithmetic_side_effects,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap,
    clippy::disallowed_macros,
    clippy::disallowed_types,
    clippy::disallowed_methods,
    clippy::cognitive_complexity,
    clippy::unwrap_in_result,
    clippy::panic_in_result_fn,
    clippy::missing_panics_doc,
    clippy::dbg_macro,
    unreachable_pub
)]

use std::cell::RefCell;

#[macro_use]
mod macros;

#[path = "."]
mod tests_with_aws_lc_rs {
    use super::*;

    provider_aws_lc_rs!();

    #[path = "../api.rs"]
    mod tests;
}

// this must be outside tests_with_*, as we want
// one thread_local!, not one per provider.
thread_local!(static COUNTS: RefCell<LogCounts> = RefCell::new(LogCounts::new()));

struct CountingLogger;

#[allow(dead_code)]
static LOGGER: CountingLogger = CountingLogger;

#[allow(dead_code)]
impl CountingLogger {
    fn install() {
        let _ = log::set_logger(&LOGGER);
        log::set_max_level(log::LevelFilter::Trace);
    }

    fn reset() {
        COUNTS.with(|c| {
            c.borrow_mut().reset();
        });
    }
}

impl log::Log for CountingLogger {
    fn enabled(&self, _metadata: &log::Metadata) -> bool {
        true
    }

    fn log(&self, record: &log::Record) {
        println!("logging at {:?}: {:?}", record.level(), record.args());

        COUNTS.with(|c| {
            c.borrow_mut()
                .add(record.level(), format!("{}", record.args()));
        });
    }

    fn flush(&self) {}
}

#[derive(Default, Debug)]
struct LogCounts {
    trace: Vec<String>,
    debug: Vec<String>,
    info: Vec<String>,
    warn: Vec<String>,
    error: Vec<String>,
}

impl LogCounts {
    fn new() -> Self {
        Self {
            ..Default::default()
        }
    }

    fn reset(&mut self) {
        *self = Self::new();
    }

    fn add(&mut self, level: log::Level, message: String) {
        match level {
            log::Level::Trace => &mut self.trace,
            log::Level::Debug => &mut self.debug,
            log::Level::Info => &mut self.info,
            log::Level::Warn => &mut self.warn,
            log::Level::Error => &mut self.error,
        }
        .push(message);
    }
}
