//! hdrs is a HDFS Native Client in Rust based on [hdfs-sys](https://github.com/Xuanwo/hdfs-sys).
//!
//! # Examples
//!
//! ```no_run
//! use std::io::{Read, Write};
//!
//! use hdrs::Client;
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use hdrs::ClientBuilder;
//! let fs = ClientBuilder::new("default").connect()?;
//!
//! let mut f = fs
//!     .open_file()
//!     .write(true)
//!     .create(true)
//!     .open("/tmp/hello.txt")?;
//! let n = f.write("Hello, World!".as_bytes())?;
//!
//! let mut f = fs.open_file().read(true).open("/tmp/hello.txt")?;
//! let mut buf = vec![0; 1024];
//! let n = f.read(&mut buf)?;
//!
//! let _ = fs.remove_file("/tmp/hello.txt")?;
//! # Ok(())
//! # }
//! ```
//!
//! # Features
//!
//! - `async_file`: Enable async operation support
//! - `vendored`: Ignore lib loading logic, enforce to complie and staticly link libhdfs
//!
//! # Compiletime
//! `hdrs` depends on [hdfs-sys](https://github.com/Xuanwo/hdfs-sys) which links `libjvm` to work.
//!
//! Please make sure `JAVA_HOME` is set correctly:
//!
//! ```shell
//! export JAVA_HOME=/path/to/java
//! export LD_LIBRARY_PATH=${JAVA_HOME}/lib/server:${LD_LIBRARY_PATH}
//! ```
//!
//! - Enable `vendored` feature to compile `libhdfs` and link in static.
//! - Specify `HDFS_LIB_DIR` or `HADOOP_HOME` to load from specified path instead of compile.
//! - Specify `HDFS_STATIC=1` to link `libhdfs` in static.
//! - And finally, we will fallback to compile `libhdfs` and link in static.
//!
//! # Runtime
//!
//! `hdrs` depends on [hdfs-sys](https://github.com/Xuanwo/hdfs-sys) which uses JNI to call functions provided by jars that provided by hadoop releases.
//!
//! Please also make sure `HADOOP_HOME`, `LD_LIBRARY_PATH`, `CLASSPATH` is set correctly during runtime:
//!
//! ```shell
//! export HADOOP_HOME=/path/to/hadoop
//! export LD_LIBRARY_PATH=${JAVA_HOME}/lib/server:${LD_LIBRARY_PATH}
//! export CLASSPATH=$(${HADOOP_HOME}/bin/hadoop classpath --glob)
//! ```
//!
//! If `libhdfs` is configued to link dynamiclly, please also add `${HADOOP_HOME}/lib/native` in `LD_LIBRARY_PATH` to make sure linker can find `libhdfs.so`:
//!
//! ```shell
//! export LD_LIBRARY_PATH=${JAVA_HOME}/lib/server:${HADOOP_HOME}/lib/native:${LD_LIBRARY_PATH}
//! ```

/// Build an `io::Error` that surfaces the real libhdfs/JNI exception root cause when
/// available. On Windows `io::Error::last_os_error()` reads `GetLastError()`, which
/// libhdfs does not set (it reports failures via a thread-local Java exception), so the
/// bare OS error shows up as "os error 0" / "The operation completed successfully". This
/// helper asks libhdfs for the last exception root cause and folds it into the message.
pub(crate) fn last_hdfs_error() -> std::io::Error {
    let os = std::io::Error::last_os_error();
    unsafe {
        let cause = hdfs_sys::hdfsGetLastExceptionRootCause();
        if !cause.is_null() {
            let msg = std::ffi::CStr::from_ptr(cause).to_string_lossy().into_owned();
            // libhdfs allocates this string; intentionally leaked (error path only) to
            // avoid a cross-allocator free between the MSVC CRT and libhdfs.
            if !msg.trim().is_empty() {
                return std::io::Error::new(os.kind(), format!("libhdfs: {msg} (os: {os})"));
            }
        }
    }
    os
}

mod client;
pub use client::{Client, ClientBuilder};

mod file;
pub use file::File;

#[cfg(feature = "async_file")]
mod async_file;
#[cfg(feature = "async_file")]
pub use async_file::AsyncFile;

mod open_options;
pub use open_options::OpenOptions;

mod metadata;
pub use metadata::Metadata;

mod readdir;
pub use readdir::Readdir;
