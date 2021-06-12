use commitlog::{message::MessageError, reader::LogSliceReader};
use nix::errno::Errno;
use std::fs::File;
use std::io;
use std::os::unix::io::{AsRawFd, RawFd};

pub struct FileSlice {
    file: RawFd,
    offset: u64,
    bytes: usize,
}

impl FileSlice {
    pub fn send(&mut self, socket: RawFd) -> Result<(), io::Error> {
        debug!(
            "Attempting write. Offset={}, bytes={}",
            self.offset, self.bytes
        );

        match self.sendfile(socket) {
            Ok(sent) => {
                debug!(
                    "Sent {} bytes. New offset={}, num_bytes={}",
                    sent, self.offset, self.bytes
                );
                Ok(())
            }
            Err(e) => {
                error!("Err {}", e);
                Err(e)
            }
        }
    }

    #[cfg(any(target_os = "macos"))]
    fn sendfile(&mut self, socket: RawFd) -> Result<usize, io::Error> {
        use std::ptr;

        let off = self.offset as libc::off_t;
        let mut len = self.bytes as libc::off_t;

        let ret = unsafe {
            libc::sendfile(
                self.file,
                socket,
                off,
                &mut len as &mut _,
                ptr::null_mut(),
                0,
            )
        };
        match Errno::result(ret) {
            Ok(_) => {
                let sent = len as usize;
                self.offset += sent as u64;
                self.bytes = self.bytes.saturating_sub(sent);
                Ok(sent)
            }
            Err(nix::Error::Sys(err)) => Err(io::Error::from_raw_os_error(err as i32)),
            Err(_) => unreachable!(),
        }
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    fn sendfile(&mut self, socket: RawFd) -> Result<usize, io::Error> {
        let mut off = self.offset as i64;
        let len = self.bytes as usize;
        let ret = unsafe { libc::sendfile(socket, self.file, &mut off as *mut _, len) };
        match Errno::result(ret) {
            Ok(0) => {
                debug!("Connection closed");
                Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "Connection closed while sending",
                ))
            }
            Ok(sent) => {
                let sent = sent as usize;
                self.offset += sent as u64;
                self.bytes = self.bytes.saturating_sub(sent);
                Ok(sent)
            }
            Err(nix::Error::Sys(err)) => Err(io::Error::from_raw_os_error(err as i32)),
            Err(_) => unreachable!(),
        }
    }

    pub fn completed(&self) -> bool {
        self.bytes == 0
    }

    pub fn remaining_bytes(&self) -> usize {
        self.bytes
    }

    pub fn file_offset(&self) -> u64 {
        self.offset
    }
}

pub struct FileSliceMessageReader;

impl LogSliceReader for FileSliceMessageReader {
    type Result = FileSlice;

    fn read_from(
        &mut self,
        file: &File,
        offset: u32,
        bytes: usize,
    ) -> Result<Self::Result, MessageError> {
        Ok(FileSlice {
            file: file.as_raw_fd(),
            offset: u64::from(offset),
            bytes,
        })
    }
}
