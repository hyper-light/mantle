# The macOS probe released a null Core Foundation object

**Symptom.** Identifying a path on an attached disk image killed the process with SIGTRAP
(exit 133) and no message. The crash report's backtrace: `CFRelease` ← `Owned::drop` ←
`bool::then_some` ← `Owned::new` ← `Service::property`.

**Cause.** `Owned::new(cf)` was `(!cf.is_null()).then_some(Self(cf))`. `then_some` takes its
argument by value, so `Self(cf)` was built before the condition was known; for a null `cf`
the unused wrapper was dropped at once, and its `Drop` called `CFRelease(NULL)`, which
Core Foundation traps on. The internal SSD's registry entry has every property the probe
reads, so the null path never ran there; the disk image's IOMedia has no
"Physical Block Size".

**Fix.** Construct the wrapper only for a non-null object. The class is closed by lint:
`bool::then_some` is a disallowed method (`clippy.toml`), since any argument with a `Drop`
is built and dropped whether or not it is used.

**Regression test.** `probe::macos::tests::identifies_an_attached_disk_image` attaches a
disk image with hdiutil(1) and identifies it.
