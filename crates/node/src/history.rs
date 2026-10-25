//! A node's history and the cell's (docs/design/node.md §2.4, item 7): the time each node has run
//! and its runs that ended without an orderly stop, kept in the root range, whose sum is the prior
//! evidence every node's failure detectors start from (`hyper_liveness::Settings::history`): the
//! MTBF is the Jeffreys posterior over the cell's node time and crashes (hyper-raft
//! `docs/timing.md` §2.6, "MTBF"), not over the seconds a run has watched. A run that stopped in
//! order is no crash: a stop hands its leadership over and needs no detection, which is all the
//! MTBF prices (§2.2's crash term).

use mantle_meta::record::History;

use crate::layout::Stopped;

/// The node's history at a start: what the root range holds of it, its last run and its last
/// orderly stop. Runs are numbered from one, so the runs before this start that did not stop in
/// order are the last run less the stops, exactly, however many statements to the root range were
/// lost to crashes; its time run is the more of the root range's and its last stop's.
pub fn at_start(held: History, last_run: Option<u64>, stopped: Option<Stopped>) -> History {
    let runs = last_run.unwrap_or(0);
    let (stops, exposure_ns) = stopped.map_or((0, 0), |s| (s.stops, s.exposure_ns));
    History {
        exposure_ns: held.exposure_ns.max(exposure_ns),
        crashes: held.crashes.max(runs.saturating_sub(stops)),
    }
}

/// The stop record a directory keeps once none is: every start keeps one, so the record a start
/// reads names the runs its counts begin from. A fresh directory's counts begin before its first
/// run; one whose runs began before the record was kept counts none of them a crash, their ends
/// being unknown, and its time run is what the root range holds.
pub fn first_stop(last_run: Option<u64>, held: History) -> Stopped {
    let runs = last_run.unwrap_or(0);
    Stopped {
        run: runs,
        exposure_ns: held.exposure_ns,
        stops: runs,
    }
}

/// The cell's history with this node's own as it now stands: the root range's sum `cell`, which
/// holds this node's row `held`, with `own` for it. Saturating, as the sum is.
pub fn cell_with(cell: History, held: History, own: History) -> History {
    History {
        exposure_ns: cell
            .exposure_ns
            .saturating_sub(held.exposure_ns)
            .saturating_add(own.exposure_ns),
        crashes: cell
            .crashes
            .saturating_sub(held.crashes)
            .saturating_add(own.crashes),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(exposure_ns: u64, crashes: u64) -> History {
        History {
            exposure_ns,
            crashes,
        }
    }

    fn stopped(run: u64, exposure_ns: u64, stops: u64) -> Option<Stopped> {
        Some(Stopped {
            run,
            exposure_ns,
            stops,
        })
    }

    #[test]
    fn a_first_start_holds_what_the_root_range_holds() {
        assert_eq!(at_start(h(0, 0), None, None), h(0, 0));
        assert_eq!(at_start(h(5, 1), None, None), h(5, 1));
    }

    #[test]
    fn runs_that_did_not_stop_in_order_are_the_crashes_however_many_statements_were_lost() {
        // Run 1 stopped in order after 10; run 2 crashed; this is run 3.
        assert_eq!(at_start(h(10, 0), Some(2), stopped(1, 10, 1)), h(10, 1));
        // Runs 1 and 2 crashed before stating anything; this is run 3.
        assert_eq!(at_start(h(0, 0), Some(2), None), h(0, 2));
        // Every run stopped in order: no crash, the last stop's time.
        assert_eq!(at_start(h(10, 0), Some(4), stopped(4, 40, 4)), h(40, 0));
    }

    #[test]
    fn the_more_of_the_root_ranges_time_and_the_last_stops_is_kept() {
        // A run that crashed after stating more time than the stop before it kept.
        assert_eq!(at_start(h(90, 0), Some(2), stopped(1, 30, 1)), h(90, 1));
        // The root range told less than the stop kept: the statement was lost.
        assert_eq!(at_start(h(20, 0), Some(1), stopped(1, 30, 1)), h(30, 0));
    }

    #[test]
    fn a_directory_without_a_stop_record_counts_from_its_next_run() {
        // Fresh: its first run counts from zero, so a crash in it is one.
        let first = first_stop(None, h(0, 0));
        assert_eq!(at_start(h(0, 0), None, Some(first)), h(0, 0));
        assert_eq!(at_start(h(0, 0), Some(1), Some(first)), h(0, 1));
        // Runs before the record whose ends are unknown are no crashes; later ones count.
        let older = first_stop(Some(5), h(70, 0));
        assert_eq!(at_start(h(70, 0), Some(5), Some(older)), h(70, 0));
        assert_eq!(at_start(h(70, 0), Some(6), Some(older)), h(70, 1));
    }

    #[test]
    fn the_cells_history_takes_this_nodes_own_in_place_of_its_row() {
        assert_eq!(cell_with(h(100, 3), h(40, 1), h(55, 2)), h(115, 4));
        assert_eq!(cell_with(h(0, 0), h(0, 0), h(7, 1)), h(7, 1));
        assert_eq!(cell_with(h(u64::MAX, 0), h(1, 0), h(9, 0)), h(u64::MAX, 0));
    }
}
