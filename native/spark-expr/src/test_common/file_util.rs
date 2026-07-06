// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::{env, fs, io::Write, path::{Path, PathBuf}};

/// Returns file handle for a temp file in 'target' directory with a provided content
pub fn get_temp_file(file_name: &str, content: &[u8]) -> fs::File {
    // build tmp path to a file in "target/debug/testdata"
    let mut path_buf = env::current_dir().unwrap();
    path_buf.push("target");
    path_buf.push("debug");
    path_buf.push("testdata");
    fs::create_dir_all(&path_buf).unwrap();
    path_buf.push(file_name);

    // write file content
    let mut tmp_file = fs::File::create(path_buf.as_path()).unwrap();
    tmp_file.write_all(content).unwrap();
    tmp_file.sync_all().unwrap();

    // return file handle for both read and write
    let file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path_buf.as_path());
    assert!(file.is_ok());
    file.unwrap()
}

pub fn get_temp_filename() -> PathBuf {
    let mut path_buf = env::current_dir().unwrap();
    path_buf.push("target");
    path_buf.push("debug");
    path_buf.push("testdata");
    fs::create_dir_all(&path_buf).unwrap();
    path_buf.push(rand::random::<i16>().to_string());

    path_buf
}

/// Convert a filesystem path into a string suitable for object_store and
/// `datafusion::datasource::listing::PartitionedFile::from_path`.
///
/// object_store uses `/` as its only path separator and percent-encodes `\`,
/// so a Windows `PathBuf` (which renders with `\`) would otherwise be turned
/// into a double-encoded location (`D:%5C...`) that `LocalFileSystem` rejects.
/// Converting to forward slashes yields a valid object_store path on Windows
/// and is a no-op for the paths produced by these tests on Unix.
pub fn object_store_path_str(path: impl AsRef<Path>) -> String {
    path.as_ref().to_string_lossy().replace('\\', "/")
}
