use std::fs::{File, OpenOptions};
use std::io;
use std::io::{Read, Seek, SeekFrom, Write};
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
        writer.finish()?;

        Ok(())
    }
}

/// Writes a hash to a specific position in a gzip-compressed file while preserving the rest of the content.
///
/// This function reads the entire compressed file, modifies the content at the specified position,
/// and then rewrites the entire file with the modifications.
///
/// # Parameters
///
/// - `hash`: The hash value to be written to the file.
/// - `file`: A reference to the file to be modified.
/// - `pos`: The position (byte offset) where the hash should be written.
///
/// # Returns
///
/// - `Ok(())` if the hash was successfully written
/// - `Err(io::Error)` if there was an issue reading or writing the file
///
/// # Behavior
///
/// - Reads the entire content of the gzip-compressed file
/// - Replaces the content at the specified position with the new hash
/// - Extends the content if the position is beyond the current file length
/// - Rewrites the entire file, maintaining the gzip compression
///
/// # Notes
///
/// - This method is less efficient for very large files as it reads and rewrites the entire file
/// - The file must be opened with both read and write permissions
pub fn write_string_file_gz(hash: String, file: &mut File, pos: u64) -> Result<(), io::Error> {
    let mut existing_content = Vec::new();

    if file.metadata()?.len() > 0 {
        let mut gz_reader = GzDecoder::new(file.try_clone()?);
        gz_reader.read_to_end(&mut existing_content)?;
    }

    let hash_bytes = hash.as_bytes();

    if pos as usize + hash_bytes.len() > existing_content.len() {
        existing_content.resize(pos as usize + hash_bytes.len(), 0);
    }

    existing_content[pos as usize..pos as usize + hash_bytes.len()].copy_from_slice(hash_bytes);

    file.seek(SeekFrom::Start(0))?;

    let mut gz_writer = GzEncoder::new(file, Compression::default());
    gz_writer.write_all(&existing_content)?;
    gz_writer.finish()?;

    Ok(())
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

    #[test]
    fn test_write_hash_file_gz() {
        // Create a temporary file
        let file_path = create_temp_file("test_hash_file.gz");
        let file = File::create(&file_path).unwrap();

        // Initial content to write
        let initial_content = b"Hello world, this is some initial content!";

        // Compress initial content
        {
            let mut gz_writer = GzEncoder::new(&file, Compression::default());
            gz_writer.write_all(initial_content).unwrap();
            gz_writer.finish().unwrap();
        }

        // Reopen the file for reading and writing
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&file_path)
            .unwrap();

        // Write hash at a specific position
        let hash_to_write = "new_hash_value".to_string();
        write_string_file_gz(hash_to_write.clone(), &mut file, 6).unwrap();

        // Read back the content to verify
        let mut reader = File::open(&file_path).unwrap();
        let mut gz_reader = GzDecoder::new(&mut reader);
        let mut decompressed_content = Vec::new();
        gz_reader.read_to_end(&mut decompressed_content).unwrap();

        // Construct expected content
        let mut expected_content = initial_content.to_vec();
        let hash_bytes = hash_to_write.as_bytes();
        expected_content[6..6+hash_bytes.len()].copy_from_slice(hash_bytes);

        // Assert that the content matches
        assert_eq!(expected_content, decompressed_content);
    }
}
