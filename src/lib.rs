use std::fs::{File, OpenOptions};
use std::io;
use std::io::{Read, Write};
use std::path::PathBuf;

use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;

/// Represents a file that supports gzip compression and decompression.
pub struct CompressedFile {
    /// Path to the file on the filesystem.
    path: PathBuf,
}

impl CompressedFile {
    /// Creates a new compressed file at the specified path.
    ///
    /// If the file already exists, its content will be truncated.
    ///
    /// # Arguments
    ///
    /// * `path` - A `PathBuf` that specifies the path where the file will be created.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing a new `CompressedFile` instance if successful,
    /// or an `io::Error` if the file cannot be created.
    pub fn create_file(path: PathBuf) -> Result<CompressedFile, io::Error> {
        File::create(path.clone())?;
        Ok(Self {
            path,
        })
    }

    /// Creates an object compressed from the specified path.
    ///
    /// # Arguments
    ///
    /// * `path` - A string slice that specifies the path where the file will be created.
    ///
    /// # Returns
    ///
    /// Returns a `CompressedFile` instance.
    pub fn new(path: PathBuf) -> Self{
        Self {
            path
        }
    }

    /// Opens the file for reading and decompresses its content on the fly.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing a `Box<dyn Read>` if successful,
    /// or an `io::Error` if the file cannot be opened or read.
    pub fn open_for_read(&self) -> Result<Box<dyn Read>, io::Error> {
        let file = File::open(&self.path)?;
        Ok(Box::new(GzDecoder::new(file)))
    }

    /// Opens the file for writing and compresses its content on the fly.
    ///
    /// # Arguments
    ///
    /// * `append` - A boolean indicating whether to append to the existing file (`true`)
    ///   or overwrite it (`false`).
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing a `Box<dyn Write>` if successful,
    /// or an `io::Error` if the file cannot be opened or written.
    pub fn open_for_write(&self, append: bool) -> Result<Box<dyn Write>, io::Error> {
        let file = OpenOptions::new()
            .write(true)
            .append(append)
            .open(&self.path)?;
        Ok(Box::new(GzEncoder::new(file, Compression::default())))
    }
}
