//! The positional file operations mantle's storage engines are written against, so a test can
//! put a simulated device with power-loss semantics (`crate::sim`) where the real file goes.

use crate::DiskError;
use crate::buf::Alignment;
use crate::file::DeviceFile;

pub trait BlockFile: Send + Sync {
    /// The alignment every transfer's offset and length must meet.
    fn alignment(&self) -> Alignment;

    fn len(&self) -> Result<u64, DiskError>;

    fn is_empty(&self) -> Result<bool, DiskError> {
        self.len().map(|len| len == 0)
    }

    /// Fills `buf` from `offset`; reaching the end of the file first is an error.
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<(), DiskError>;

    /// Writes all of `buf` at `offset`, extending the file if it ends past the end.
    fn write_all_at(&self, buf: &[u8], offset: u64) -> Result<(), DiskError>;

    /// Makes every completed write durable. A failure leaves durability of those writes
    /// unknown (Rebello et al., ATC 2020): the caller must stop trusting what it wrote.
    fn sync_data(&self) -> Result<(), DiskError>;

    /// A second handle to the same file, whose writes and flushes are the file's own: what a
    /// volume hands its device's issuer while it keeps reading through its own handle
    /// (crate::issuer). A file that has no second handle refuses.
    fn try_clone(&self) -> Result<Self, DiskError>
    where
        Self: Sized,
    {
        Err(DiskError::Io {
            op: "duplicate a file handle",
            path: std::path::PathBuf::new(),
            source: std::io::Error::from(std::io::ErrorKind::Unsupported),
        })
    }
}

impl BlockFile for DeviceFile {
    fn alignment(&self) -> Alignment {
        DeviceFile::alignment(self)
    }

    fn len(&self) -> Result<u64, DiskError> {
        DeviceFile::len(self)
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<(), DiskError> {
        DeviceFile::read_exact_at(self, buf, offset)
    }

    fn write_all_at(&self, buf: &[u8], offset: u64) -> Result<(), DiskError> {
        DeviceFile::write_all_at(self, buf, offset)
    }

    fn sync_data(&self) -> Result<(), DiskError> {
        DeviceFile::sync_data(self)
    }

    fn try_clone(&self) -> Result<Self, DiskError> {
        DeviceFile::try_clone(self)
    }
}

impl<T: BlockFile + ?Sized> BlockFile for std::sync::Arc<T> {
    fn alignment(&self) -> Alignment {
        (**self).alignment()
    }

    fn len(&self) -> Result<u64, DiskError> {
        (**self).len()
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<(), DiskError> {
        (**self).read_exact_at(buf, offset)
    }

    fn write_all_at(&self, buf: &[u8], offset: u64) -> Result<(), DiskError> {
        (**self).write_all_at(buf, offset)
    }

    fn sync_data(&self) -> Result<(), DiskError> {
        (**self).sync_data()
    }

    fn try_clone(&self) -> Result<Self, DiskError> {
        Ok(std::sync::Arc::clone(self))
    }
}
