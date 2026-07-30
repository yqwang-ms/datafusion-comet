use std::collections::HashMap;
use std::ffi::CString;
use std::io::{Error, ErrorKind, Read, Result, Seek, SeekFrom, Write};
use std::ptr;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Mutex, OnceLock};

use hdfs_sys::*;
use libc::c_void;
use log::debug;

use crate::Client;

// at most 2^30 bytes, ~1GB
const FILE_LIMIT: usize = 1073741824;

/// File will hold the underlying pointer to `hdfsFile`.
///
/// The internal file will be closed while `Drop`, so their is no need to close it manually.
///
/// # Examples
///
/// ```no_run
/// use hdrs::{Client, ClientBuilder};
///
/// let fs = ClientBuilder::new("default")
///     .with_user("default")
///     .with_kerberos_ticket_cache_path("/tmp/krb5_111")
///     .connect()
///     .expect("client connect succeed");
/// let mut f = fs
///     .open_file()
///     .read(true)
///     .open("/tmp/hello.txt")
///     .expect("must open success");
/// ```
#[derive(Debug)]
pub struct File {
    fs: hdfsFS,
    f: hdfsFile,
    path: String,
    /// When `Some`, this `File` shares a process-wide cached read-only handle: reads use
    /// positional `hdfsPread` at this logical offset (interior-mutable so the shared-`&File`
    /// path used by the async reader can advance it), `seek` only moves this offset, and
    /// `Drop` does NOT close the handle. When `None`, this is an owned handle with normal
    /// sequential read/seek and close-on-drop.
    pread_offset: Option<AtomicI64>,
}

/// HDFS's client handle is thread safe.
unsafe impl Send for File {}
unsafe impl Sync for File {}

/// Process-wide cache of open read-only `hdfsFile` handles, keyed by `(fs_ptr, path)`.
/// Reused across tasks and range reads so each file is opened once per process (a NameNode
/// round-trip) instead of once per byte-range read. Handles are read via positional
/// `hdfsPread` and are never closed (kept for the process lifetime). Stored as `usize` so
/// the map is `Send + Sync`.
fn file_cache() -> &'static Mutex<HashMap<(usize, String), usize>> {
    static CACHE: OnceLock<Mutex<HashMap<(usize, String), usize>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

impl Drop for File {
    fn drop(&mut self) {
        // Shared cached read-only handles are owned by the process-wide cache; never close
        // them here (other `File`s and future opens reuse the same handle).
        if self.pread_offset.is_some() {
            return;
        }
        unsafe {
            debug!("file has been closed");
            let _ = hdfsCloseFile(self.fs, self.f);
            // hdfsCloseFile will free self.f no matter success or failed.
            self.f = ptr::null_mut();
        }
    }
}

impl File {
    pub(crate) fn new(fs: hdfsFS, f: hdfsFile, path: &str) -> Self {
        File {
            fs,
            f,
            path: path.to_string(),
            pread_offset: None,
        }
    }

    /// Construct a `File` over a process-wide cached read-only handle: reads are positional
    /// (`hdfsPread`) at an interior-mutable logical offset and the handle is not closed on drop.
    pub(crate) fn new_shared(fs: hdfsFS, f: hdfsFile, path: &str) -> Self {
        File {
            fs,
            f,
            path: path.to_string(),
            pread_offset: Some(AtomicI64::new(0)),
        }
    }

    /// Open a read-only file through the process-wide handle cache. A given file is opened
    /// once per process via `hdfsOpenFile`; every subsequent range read reuses the same
    /// handle via positional `hdfsPread`. Parquet scans issue many range reads per file, so
    /// avoiding a fresh open (a NameNode round-trip) per range is the dominant latency win.
    pub(crate) fn open_cached_readonly(fs: hdfsFS, path: &str) -> Result<File> {
        let key = (fs as usize, path.to_string());
        if let Ok(cache) = file_cache().lock() {
            if let Some(&h) = cache.get(&key) {
                return Ok(File::new_shared(fs, h as hdfsFile, path));
            }
        }
        // Open outside the lock so opens of different files don't serialize.
        let _t = crate::inst_start();
        let f = unsafe {
            let p = CString::new(path)?;
            hdfsOpenFile(fs, p.as_ptr(), libc::O_RDONLY, 0, 0, 0)
        };
        crate::inst_end(_t, "open", fs as usize, f as usize, path, -1, 0, 0);
        if f.is_null() {
            return Err(crate::hdfs_err_ctx(&format!("open({path})")));
        }
        if let Ok(mut cache) = file_cache().lock() {
            if let Some(&existing) = cache.get(&key) {
                // Lost a race with another thread: reuse its handle and close ours.
                unsafe {
                    let _ = hdfsCloseFile(fs, f);
                }
                return Ok(File::new_shared(fs, existing as hdfsFile, path));
            }
            cache.insert(key, f as usize);
        }
        Ok(File::new_shared(fs, f, path))
    }

    /// Works only for files opened in read-only mode.
    fn inner_seek(&self, offset: i64) -> Result<()> {
        let _t = crate::inst_start();
        let n = unsafe { hdfsSeek(self.fs, self.f, offset) };
        crate::inst_end(_t, "seek", self.fs as usize, self.f as usize, &self.path, offset, 0, 0);

        if n == -1 {
            return Err(crate::hdfs_err_ctx("seek"));
        }

        Ok(())
    }

    fn tell(&self) -> Result<i64> {
        let n = unsafe { hdfsTell(self.fs, self.f) };

        if n == -1 {
            return Err(Error::last_os_error());
        }

        Ok(n)
    }

    pub fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        let _t = crate::inst_start();
        let req = buf.len().min(FILE_LIMIT);
        let n = unsafe {
            hdfsPread(
                self.fs,
                self.f,
                offset as i64,
                buf.as_ptr() as *mut c_void,
                req as i32,
            )
        };
        if n == -1 {
            crate::inst_end(_t, "pread", self.fs as usize, self.f as usize, &self.path, offset as i64, req, 0);
            return Err(crate::hdfs_err_ctx("pread"));
        }
        crate::inst_end(_t, "pread", self.fs as usize, self.f as usize, &self.path, offset as i64, req, n as usize);
        Ok(n as usize)
    }
}

impl Read for File {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        // Shared cached read-only handles read positionally so many concurrent readers can
        // share one open handle without a shared cursor.
        if let Some(off) = self.pread_offset.as_ref() {
            let pos = off.load(Ordering::SeqCst);
            let n = self.read_at(buf, pos as u64)?;
            off.fetch_add(n as i64, Ordering::SeqCst);
            return Ok(n);
        }
        let _t = crate::inst_start();
        let req = buf.len().min(FILE_LIMIT);
        let n = unsafe {
            hdfsRead(
                self.fs,
                self.f,
                buf.as_ptr() as *mut c_void,
                req as i32,
            )
        };
        if n == -1 {
            crate::inst_end(_t, "read", self.fs as usize, self.f as usize, &self.path, -1, req, 0);
            return Err(crate::hdfs_err_ctx("read"));
        }
        crate::inst_end(_t, "read", self.fs as usize, self.f as usize, &self.path, -1, req, n as usize);
        Ok(n as usize)
    }
}

impl Seek for File {
    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        if let Some(off) = self.pread_offset.as_ref() {
            let newpos: i64 = match pos {
                SeekFrom::Start(n) => n as i64,
                SeekFrom::Current(n) => off.load(Ordering::SeqCst) + n,
                SeekFrom::End(n) => Client::new(self.fs).metadata(&self.path)?.len() as i64 + n,
            };
            off.store(newpos, Ordering::SeqCst);
            return Ok(newpos as u64);
        }
        match pos {
            SeekFrom::Start(n) => {
                self.inner_seek(n as i64)?;
                Ok(n)
            }
            SeekFrom::Current(n) => {
                let current = self.tell()?;
                let offset = (current + n) as u64;
                self.inner_seek(offset as i64)?;
                Ok(offset)
            }
            SeekFrom::End(n) => {
                let meta = Client::new(self.fs).metadata(&self.path)?;
                let offset = meta.len() as i64 + n;
                self.inner_seek(offset)?;
                Ok(offset as u64)
            }
        }
    }
}

impl Write for File {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        let n = unsafe {
            hdfsWrite(
                self.fs,
                self.f,
                buf.as_ptr() as *const c_void,
                buf.len().min(FILE_LIMIT) as i32,
            )
        };

        if n == -1 {
            return Err(Error::last_os_error());
        }

        Ok(n as usize)
    }

    fn flush(&mut self) -> Result<()> {
        let n = unsafe { hdfsFlush(self.fs, self.f) };

        if n == -1 {
            return Err(Error::last_os_error());
        }

        Ok(())
    }
}

impl Read for &File {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        // Shared cached read-only handles read positionally (see `impl Read for File`).
        if let Some(off) = self.pread_offset.as_ref() {
            let pos = off.load(Ordering::SeqCst);
            let n = self.read_at(buf, pos as u64)?;
            off.fetch_add(n as i64, Ordering::SeqCst);
            return Ok(n);
        }
        let _t = crate::inst_start();
        let req = buf.len().min(FILE_LIMIT);
        let n = unsafe {
            hdfsRead(
                self.fs,
                self.f,
                buf.as_ptr() as *mut c_void,
                req as i32,
            )
        };
        if n == -1 {
            crate::inst_end(_t, "read", self.fs as usize, self.f as usize, &self.path, -1, req, 0);
            return Err(crate::hdfs_err_ctx("read"));
        }
        crate::inst_end(_t, "read", self.fs as usize, self.f as usize, &self.path, -1, req, n as usize);
        Ok(n as usize)
    }
}

impl Seek for &File {
    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        if let Some(off) = self.pread_offset.as_ref() {
            let newpos: i64 = match pos {
                SeekFrom::Start(n) => n as i64,
                SeekFrom::Current(n) => off.load(Ordering::SeqCst) + n,
                SeekFrom::End(n) => Client::new(self.fs).metadata(&self.path)?.len() as i64 + n,
            };
            off.store(newpos, Ordering::SeqCst);
            return Ok(newpos as u64);
        }
        match pos {
            SeekFrom::Start(n) => {
                self.inner_seek(n as i64)?;
                Ok(n)
            }
            SeekFrom::Current(n) => {
                let current = self.tell()?;
                let offset = (current + n) as u64;
                self.inner_seek(offset as i64)?;
                Ok(offset)
            }
            SeekFrom::End(_) => Err(Error::new(
                ErrorKind::Unsupported,
                "hdfs doesn't support seek from end",
            )),
        }
    }
}

impl Write for &File {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        let n = unsafe {
            hdfsWrite(
                self.fs,
                self.f,
                buf.as_ptr() as *const c_void,
                buf.len().min(FILE_LIMIT) as i32,
            )
        };

        if n == -1 {
            return Err(Error::last_os_error());
        }

        Ok(n as usize)
    }

    fn flush(&mut self) -> Result<()> {
        let n = unsafe { hdfsFlush(self.fs, self.f) };

        if n == -1 {
            return Err(Error::last_os_error());
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::ClientBuilder;

    #[test]
    fn test_file_build() {
        let _ = env_logger::try_init();

        let fs = ClientBuilder::new("default")
            .connect()
            .expect("init success");

        let path = uuid::Uuid::new_v4().to_string();

        let f = fs
            .open_file()
            .create(true)
            .write(true)
            .open(&format!("/tmp/{path}"))
            .expect("open file success");

        assert!(!f.f.is_null());
        assert!(!f.fs.is_null());
    }

    #[test]
    fn test_file_write() {
        let _ = env_logger::try_init();

        let fs = ClientBuilder::new("default")
            .connect()
            .expect("init success");

        let path = uuid::Uuid::new_v4().to_string();

        let mut f = fs
            .open_file()
            .create(true)
            .write(true)
            .open(&format!("/tmp/{path}"))
            .expect("open file success");

        let n = f
            .write("Hello, World!".as_bytes())
            .expect("write must success");
        assert_eq!(n, 13)
    }

    #[test]
    fn test_file_read() {
        let _ = env_logger::try_init();

        let fs = ClientBuilder::new("default")
            .connect()
            .expect("init success");

        let path = uuid::Uuid::new_v4().to_string();

        let mut f = fs
            .open_file()
            .create(true)
            .write(true)
            .open(&format!("/tmp/{path}"))
            .expect("open file success");

        let n = f
            .write("Hello, World!".as_bytes())
            .expect("write must success");
        assert_eq!(n, 13)
    }
}
