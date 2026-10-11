//! The device a test's volume sits on, run as a node runs it (CLAUDE.md §5; docs/design/node.md
//! §1.2): aligned to the block sizes the OS reports for it, its issuer as deep as the queue the
//! OS reports and the depth its random reads are measured to saturate at. Nothing about the
//! host is stated: a small or busy host measures what it is.
#![allow(dead_code, clippy::unwrap_used, clippy::expect_used)]

use std::path::Path;

use hyper_block::buf::Alignment;
use hyper_block::issuer;
use mantle_disk::calibrate::{self, Plan};

/// Where a test that re-launches itself hands the device it measured to the child: the
/// alignment and the depth, in bytes and transfers.
pub const DEVICE_ENV: &str = "MANTLE_TEST_DEVICE";

/// The page every supported file system maps and the largest logical block common devices use;
/// a device reporting a larger block raises it (docs/design/constants.md, `store::MIN_PAGE`).
const MIN_PAGE: usize = 4096;

#[derive(Clone, Copy, Debug)]
pub struct Device {
    pub align: Alignment,
    pub depth: usize,
}

impl Device {
    /// The device under `dir`: as the parent measured it, when this process is its child, and
    /// identified and measured now otherwise, as `mantle bench` does before it formats a volume.
    pub fn of(dir: &Path) -> Device {
        if let Ok(handed) = std::env::var(DEVICE_ENV) {
            let mut fields = handed.split(' ').map(|f| f.parse::<usize>().unwrap());
            return Device {
                align: Alignment::new(fields.next().unwrap()).unwrap(),
                depth: fields.next().unwrap(),
            };
        }
        let id = mantle_disk::probe::identify(dir);
        let align = [id.logical_block, id.physical_block]
            .into_iter()
            .flatten()
            .filter_map(|b| usize::try_from(b).ok())
            .filter_map(|b| Alignment::new(b).ok())
            .fold(Alignment::new(MIN_PAGE).unwrap(), Alignment::max);
        let measured = calibrate::calibrate(
            dir,
            align,
            id.file_system.available_bytes,
            &Plan::standard(align, id.queue_depth),
        )
        .unwrap();
        let depth = issuer::depth(
            id.queue_depth,
            measured.random_read_saturation().map(|p| p.depth),
        );
        Device { align, depth }
    }

    /// The device as a child reads it from [`DEVICE_ENV`].
    pub fn handed(&self) -> String {
        format!("{} {}", self.align.get(), self.depth)
    }
}
