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
    pub fn open_for_write(&self) -> Result<Box<dyn Write>, io::Error> {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&self.path)?;
        Ok(Box::new(GzEncoder::new(file, Compression::default())))
    }

    /// Appends the given text to the compressed file.
    ///
    /// This method reads the existing content of the file (if any), combines it with the new text,
    /// and writes the entire content back to the file using gzip compression.
    ///
    /// # Parameters
    ///
    /// - `text`: A byte slice containing the text to be appended to the file.
    ///
    /// # Returns
    ///
    /// - `Ok(())` if the text was successfully appended
    /// - `Err(io::Error)` if there was an issue reading the existing file or writing the new content
    ///
    /// # Behavior
    ///
    /// - If the file does not exist, it creates a new compressed file with the given text
    /// - If the file exists, it reads the existing content, appends the new text, and rewrites the file
    /// - Uses default gzip compression
    ///
    /// # Note
    ///
    /// This method rewrites the entire file for each append operation,
    /// which may not be efficient for very large files.
    pub fn append_to_file(&self, text: &[u8]) -> Result<(), io::Error> {
        let mut existing_content = Vec::new();
        if self.path.exists() {
            let mut reader = self.open_for_read()?;
            reader.read_to_end(&mut existing_content)?;
        }

        let combined_content = [existing_content, text.to_vec()].concat();

        let file = File::create(&self.path)?;
        let mut writer = GzEncoder::new(file, Compression::default());
        writer.write_all(&combined_content)?;
        writer.flush()?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Write, Read};
    use std::fs;

    fn create_temp_file(name: &str) -> PathBuf {
        let dir = std::env::temp_dir();
        dir.join(name)
    }

    #[test]
    fn test_create_file() {
        let path = create_temp_file("test_create_file.gz");

        // Ensure the file doesn't exist before creation
        if path.exists() {
            fs::remove_file(&path).unwrap();
        }

        let compressed_file = CompressedFile::create_file(path.clone());
        assert!(compressed_file.is_ok());
        assert!(path.exists());
    }

    #[test]
    fn test_write_and_read_file() {
        let path = create_temp_file("test_write_and_read.gz");
        let content = b"Hello, compressed world!";

        // Write to the compressed file
        let compressed_file = CompressedFile::create_file(path.clone()).unwrap();
        {
            let mut writer = compressed_file.open_for_write().unwrap();
            writer.write_all(content).unwrap();
            writer.flush().unwrap();
        }

        // Read from the compressed file
        let mut reader = compressed_file.open_for_read().unwrap();
        let mut decompressed_content = Vec::new();
        reader.read_to_end(&mut decompressed_content).unwrap();

        assert_eq!(content, &decompressed_content[..]);
    }

    #[test]
    fn test_append_to_file() {
        let path = create_temp_file("test_append_to_file.gz");
        let content1 = b"First line.";
        let content2 = b"Second line.";

        // Write the first line
        {
            let compressed_file = CompressedFile::new(path.clone());
            let mut writer = compressed_file.open_for_write().unwrap();
            writer.write_all(content1).unwrap();
            writer.flush().unwrap();
        }

        // Append the second line using the new method
        {
            let compressed_file = CompressedFile::new(path.clone());
            compressed_file.append_to_file(content2).unwrap();
        }

        // Read and verify the content
        let mut decompressed_content = Vec::new();
        let mut reader = CompressedFile::new(path).open_for_read().unwrap();
        reader.read_to_end(&mut decompressed_content).unwrap();

        let expected_content: Vec<u8> = Vec::from(content1)
            .into_iter()
            .chain(content2.iter().cloned())
            .collect();

        assert_eq!(expected_content, decompressed_content);
    }

    #[test]
    fn test_open_nonexistent_file_for_read() {
        let path = create_temp_file("nonexistent_file.gz");

        let compressed_file = CompressedFile::new(path);
        let result = compressed_file.open_for_read();

        assert!(result.is_err());
    }

    #[test]
    fn test_open_nonexistent_file_for_write() {
        let path = create_temp_file("nonexistent_file_write.gz");

        let compressed_file = CompressedFile::new(path.clone());
        let result = compressed_file.open_for_write();

        // Writing should create the file even if it doesn't exist
        assert!(result.is_ok());
        assert!(path.exists());
    }
}
