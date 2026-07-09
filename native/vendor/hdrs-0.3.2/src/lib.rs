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
/// Build an `io::Error` that surfaces the real libhdfs/JNI failure.
///
/// On Windows `io::Error::last_os_error()` reads `GetLastError()`, which libhdfs does not
/// set, so failures otherwise show up as "os error 0". This pulls BOTH the last exception
/// root cause AND its stack trace (thread-local, owned by libhdfs, valid until the next
/// libHDFS call on this thread — do not free), plus an optional operation/path context, so
/// Windows failures are debuggable.
pub(crate) fn hdfs_err_ctx(ctx: &str) -> std::io::Error {
    let os = std::io::Error::last_os_error();
    // libhdfs sets the C runtime errno on failure; on Windows `last_os_error()` reads
    // GetLastError() which libhdfs does NOT set (so it's often 0). Read the C errno too.
    let c_errno = errno::errno();
    let root = unsafe { cstr_owned(hdfs_sys::hdfsGetLastExceptionRootCause()) };
    let trace = unsafe { cstr_owned(hdfs_sys::hdfsGetLastExceptionStackTrace()) };
    let detail = match (root, trace) {
        (Some(r), Some(t)) => format!("{r} || stack: {}", clip(&t, 1500)),
        (Some(r), None) => r,
        (None, Some(t)) => format!("<no root cause> || stack: {}", clip(&t, 1500)),
        (None, None) => "<no JNI exception recorded>".to_string(),
    };
    let prefix = if ctx.is_empty() {
        String::new()
    } else {
        format!("{ctx}: ")
    };
    std::io::Error::new(
        os.kind(),
        format!("hdrs/libhdfs: {prefix}{detail} (c_errno={c_errno}, os: {os})"),
    )
}

unsafe fn cstr_owned(p: *mut std::os::raw::c_char) -> Option<String> {
    if p.is_null() {
        return None;
    }
    let s = std::ffi::CStr::from_ptr(p).to_string_lossy().into_owned();
    if s.trim().is_empty() {
        None
    } else {
        Some(s)
    }
}

fn clip(s: &str, n: usize) -> String {
    s.chars().take(n).collect()
}

/// On Windows, Comet's native HDFS runs *embedded inside the Spark executor JVM*. The MT
/// libhdfs (built with DYNAMIC_LINKING_JVM_DLL) exposes `jvmInit`, which must be called once
/// before any other libhdfs call so it ATTACHES to the preexisting JVM (reuseJvm=1) instead
/// of trying to create a new one. Without it, `hdfsBuilderConnect` returns NULL silently
/// (getGlobalJNIEnv takes the create-VM path -> "CLASSPATH not set" / wildcard_expandPath).
#[cfg(windows)]
pub(crate) fn ensure_jvm_init() {
    use std::os::raw::{c_char, c_int};
    use std::sync::Once;
    extern "C" {
        fn jvmInit(
            reuse_jvm: c_int,
            jre_path: *const c_char,
            use_java11: c_int,
            use_ic: c_int,
            jar_path_4ic: *const c_char,
            conf_dir_path_4ic: *const c_char,
            e_cps_4ic: *mut *mut c_char,
            e_cps_4ic_len: usize,
        ) -> c_int;
    }
    static JVM_INIT: Once = Once::new();
    JVM_INIT.call_once(|| {
        // reuseJvm=1 (attach to executor JVM), useJava11=1, useIC=0 (use the default
        // classloader of the preexisting JVM, which already has the Hadoop classes).
        let rc = unsafe {
            jvmInit(
                1,
                std::ptr::null(),
                1,
                0,
                std::ptr::null(),
                std::ptr::null(),
                std::ptr::null_mut(),
                0,
            )
        };
        if rc != 0 {
            log::warn!("hdrs: jvmInit(reuseJvm=1) returned {rc}");
        }
    });
}

#[cfg(not(windows))]
pub(crate) fn ensure_jvm_init() {}

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
