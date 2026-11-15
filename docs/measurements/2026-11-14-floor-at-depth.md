# A node's floor at its device's measured depth, and the cells' probe budget on Windows (2026-11-14)

Two CI failures, measured where they happened: the KIND cell's founder that refused to start at
its threads (kind-cell job 131575146397, lean-doors `c33e5bf0`), and windows-x86_64's nodes that
refused to start at their wake probe beside busy loops (14 of 86 runs on jupiter).

## 1. The floor grows with the depth calibration measures

Instrumented on CI (branches `ci-health-diag-kind` and `ci-health-diag-kind-c33`, never merged): each
of the KIND cell's pods printed its floor class by class and what it measured of its volume's device,
the chart's limits unchanged (2 GiB, one CPU). Eight runners, three pods each:

| Tree | Runner's disk (queue) | Pods' measured depths | Floor at depth 1 | Floor at 4 | Floor at 16 | Floor at 64 |
|---|---|---|---|---|---|---|
| lean-doors `40a4f84e` | MSFT NVMe Accelerator (255) | 4, 1, 64; 4, 64, 64 | 1,414,505,933 | 1,421,413,829 | | 1,559,571,749 |
| lean-doors `40a4f84e` | Azure Virtual Disk (316) | 4, 64, 4 | | 1,421,413,829 | | 1,559,571,749 |
| lean-doors `40a4f84e` | Amazon EBS, dm-0 (none: 32) | 4, 1, 1 | 1,414,505,933 | 1,421,413,829 | | |
| `c33e5bf0` | Azure Virtual Disk (316) | 1, 4, 64; 4, 64, 16 | 1,448,051,659 | 1,454,959,555 | 1,482,591,139 | 1,593,117,475 |
| `c33e5bf0` | Amazon EBS, dm-0 (none: 32) | 4, 4, 4; 4, 16, 4 | | 1,454,959,555 | 1,482,591,139 | |

Runs 43675803389 (lean-doors) and 43676924862 (`c33e5bf0`). One disk measured depths of 1 to 64
from one pod to the next: calibration's knee moves with the host's load. Every unit of depth added
2,302,632 bytes: 2,097,152 of a pool worker's stack (`derive::threads` counted a worker for each read
the device was measured to take, each stack 2 MiB reserved whole) and 205,480 of the transport's and
the ranges' bounds for the volume's reads at the device and waiting. At `c33e5bf0` the floor at depth
316, the queue of the failing runner's disk and the ladder's cap, is 1,448,051,659 + 315 x 2,302,632 =
2,173,380,739 bytes, the failing pod's figure exactly, 681,574,400 of it stacks: it passes 2 GiB at the
threads, the last class counted. (Lean-doors' transport is 33,545,726 bytes smaller, the QUIC DATAGRAM
offer gone, so there the floor passes 2 GiB only past a depth of 319.)

Under the chart's limits on our hardware (Docker on adas-mac-studio-3: `--memory 2g --cpus 1`, the
volume's device's `nr_requests` bind-mounted as 316, its kept profile's depth set to 316):

| Binary | Floor's threads | Outcome |
|---|---|---|
| `c33e5bf0` | 325 threads | refused: "the node's floor of 2149350655 bytes passes its allowance of 2147483648 at its threads" |
| ci-health | 9 threads, 18,874,368 bytes | started: floor 1,453,104,897, 694,378,751 remaining; `memory.need` 1,761,692,335 |

(The VM's disk has 512-byte sectors where the runner's has 4 KiB ones, so its floor differs from the
runner's by the volume's index; the class that crosses is the same.) The outcome is a function of the
device's facts and the stated configuration: one run of each is exact.

## 2. A probe budget below the platform's quantum refuses on a loaded Windows host

windows-2025 runner, AMD EPYC 7763, 2 cores and 4 logical processors, 32 busy loops (`while :; do :;
done` in Git Bash), branch `ci-health-diag-win`, run 43678610491:

| Measurement | Result |
|---|---|
| a 1 ms `recv_timeout`'s lateness, idle (2,000 waits) | p50 14,668 µs, p90 14,908, p99 15,150, max 15,713 |
| the same beside 32 busy loops | p50 244,213 µs, p90 277,654, p99 380,189, max 454,663 |
| the wake probe at the cells' 40 ms budget, beside the loops | refused `MeasurementTimeout`, 40 of 40, each in 715-1,044 ms |
| its rounds | every one ended with its waiter thread never run within the probe's span (536 ms) |
| the wake probe at the derived 5,531 ms budget (`derive::probe_budget`), beside the loops | refused none of 10; 4,012 to 15,163 wakes, mean 2.6-15.0 µs; 24-87 s each |
| tests/node.rs's `objects_round_trip_through_any_node_and_read_back_after_one_is_killed`, beside the loops, lean-doors and lean-doors with 76dac1b3 and 85de7ab4 | 40 of 40 and 40 of 40 failed at the cell's start, a node refusing at its wake probe |

The cells' nodes stated `runtime.probe_budget_ms = 40`, the engine bench's figure, where a node
derives 59 samples at its platform's quantum each (93.75 ms on Windows). A round's waiter is a new
thread, which runs only once the busy loops on its core have taken their quanta; a 536 ms span holds
none of that. Windows ends a 1 ms timed wait at its 15.625 ms clock tick, so on an idle Windows host a
1 ms wait is late by most of a tick with no hold of the process at all.
