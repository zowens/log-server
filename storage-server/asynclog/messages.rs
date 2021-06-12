use byteorder::{ByteOrder, LittleEndian};
use bytes::{Bytes, BytesMut};
use commitlog::{
    message::{serialize, MessageSet, MessageSetMut, HEADER_SIZE},
    Offset,
};

const METADATA_SIZE: usize = 16;

/// Single message append, with client_id, client_req_id and payload
pub type SingleMessage = (u64, u64, Bytes);

/// Readonly view of messages, either via replication or as
/// a result of appending log entries.
#[derive(Clone)]
pub struct Messages {
    bytes: Bytes,
    len: usize,
    next_offset: Option<Offset>,
}

impl MessageSet for Messages {
    fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    fn len(&self) -> usize {
        self.len
    }
}

/// Error parsing the binary message set
#[derive(Debug, Clone, Copy)]
pub enum MessageParseError {
    /// A message has an invalid offset. This is an indication
    /// that the replication was not completed correctly.
    InvalidHash,

    /// No messages were appended.
    NoMessages,
}

impl Messages {
    /// Parses the messages from another byte source (such as an upstream replica).
    ///
    /// The messages must be non-empty.
    pub fn parse(bytes: Bytes) -> Result<Messages, MessageParseError> {
        // temporary struct to enumerate messages
        //
        // this optimization will cache the length
        // and next offset, which are used within the
        // async log when appending from replication
        // (which happens in the hot path)
        struct TmpMessages(Bytes);

        impl MessageSet for TmpMessages {
            fn bytes(&self) -> &[u8] {
                &self.0
            }
        }

        let msgs = TmpMessages(bytes);

        let mut len = 0;
        let mut last_offset = None;

        for msg in msgs.iter() {
            if rare!(!msg.verify_hash()) {
                return Err(MessageParseError::InvalidHash);
            }

            len += 1;
            last_offset = Some(msg.offset());
        }

        match last_offset {
            Some(off) => Ok(Messages {
                bytes: msgs.0,
                len,
                next_offset: Some(off + 1),
            }),
            None => Err(MessageParseError::NoMessages),
        }
    }

    /// Next offset to be appended.
    #[inline]
    pub fn next_offset(&self) -> Option<Offset> {
        self.next_offset
    }
}

impl AsRef<[u8]> for Messages {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

/// Mutable message set based on `BytesMut`.
pub struct MessagesMut(BytesMut);

impl From<BytesMut> for MessagesMut {
    fn from(bytes: BytesMut) -> MessagesMut {
        MessagesMut(bytes)
    }
}

impl MessageSet for MessagesMut {
    fn bytes(&self) -> &[u8] {
        &self.0
    }
}

impl MessageSetMut for MessagesMut {
    fn bytes_mut(&mut self) -> &mut [u8] {
        &mut self.0
    }
}

#[derive(Eq, PartialEq, Debug, Copy, Clone)]
pub enum MessagePushError {
    /// No capacity to add the message
    OutOfCapacity,

    /// The message size plus metadata size exceed overall buffer capacity
    MessageExceedsCapacity,
}

impl MessagesMut {
    /// Freezes the messages from modification.
    pub fn freeze(self) -> Messages {
        let len = self.len();
        Messages {
            bytes: self.0.freeze(),
            len,
            next_offset: None,
        }
    }

    pub fn into_inner(self) -> BytesMut {
        self.0
    }

    /// Insert a new log entry to the message set.
    #[inline]
    pub fn push<B: AsRef<[u8]>>(
        &mut self,
        client_id: u64,
        client_req_id: u64,
        payload: B,
    ) -> Result<(), MessagePushError> {
        let payload_bytes = payload.as_ref();

        let append_size = payload_bytes.len() + METADATA_SIZE + HEADER_SIZE;
        if rare!(append_size > self.0.capacity()) {
            return Err(MessagePushError::MessageExceedsCapacity);
        }

        if rare!(append_size + self.0.len() > self.0.capacity()) {
            return Err(MessagePushError::OutOfCapacity);
        }

        let mut meta = [0u8; METADATA_SIZE];
        LittleEndian::write_u64(&mut meta[0..8], client_id);
        LittleEndian::write_u64(&mut meta[8..16], client_req_id);
        serialize(&mut self.0, 0, &meta, payload_bytes).map_err(|_| MessagePushError::OutOfCapacity)
    }

    /// Insert a new log entry to the message set without metadata
    #[inline]
    #[allow(dead_code)]
    pub fn push_no_metadata<B: AsRef<[u8]>>(&mut self, payload: B) -> Result<(), MessagePushError> {
        let payload_bytes = payload.as_ref();

        let append_size = payload_bytes.len() + METADATA_SIZE + HEADER_SIZE;
        if rare!(append_size > self.0.capacity()) {
            return Err(MessagePushError::MessageExceedsCapacity);
        }

        if rare!(append_size + self.0.len() > self.0.capacity()) {
            return Err(MessagePushError::OutOfCapacity);
        }

        serialize(&mut self.0, 0, &[], payload_bytes).map_err(|_| MessagePushError::OutOfCapacity)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_mut_push_read() {
        let mut buf: MessagesMut = BytesMut::with_capacity(256).into();
        buf.push(5, 10, b"0123456789").unwrap();

        assert_eq!(1, buf.len());

        let msg = buf.iter().next().unwrap();
        assert_eq!(b"0123456789", msg.payload());
        let meta = msg.metadata();
        assert_eq!(16, meta.len());
        assert_eq!(5, LittleEndian::read_u64(&meta[0..8]));
        assert_eq!(10, LittleEndian::read_u64(&meta[8..16]));
    }

    #[test]
    fn message_mut_push_out_of_capacity() {
        let mut buf: MessagesMut = BytesMut::with_capacity(51).into();
        let msg_bytes = [1u8; 15];
        assert!(buf.push(5, 10, &msg_bytes).is_ok());
        let msg_bytes2 = [1u8; 10];
        assert_eq!(
            buf.push(1000, 11, &msg_bytes2),
            Err(MessagePushError::OutOfCapacity)
        );
    }

    #[test]
    fn message_mut_push_message_exceeds_capacity() {
        let mut buf: MessagesMut = BytesMut::with_capacity(48).into();
        let msg_bytes = [1u8; 30];
        assert_eq!(
            buf.push(5, 11, &msg_bytes),
            Err(MessagePushError::MessageExceedsCapacity)
        );
    }

    #[test]
    fn message_mut_push_no_metadata_read() {
        let mut buf: MessagesMut = BytesMut::with_capacity(256).into();
        buf.push_no_metadata(b"0123456789").unwrap();

        assert_eq!(1, buf.len());

        let msg = buf.iter().next().unwrap();
        assert_eq!(b"0123456789", msg.payload());
        let meta = msg.metadata();
        assert_eq!(0, meta.len());
    }

    #[test]
    fn message_mut_push_multiple_messages() {
        let mut buf: MessagesMut = BytesMut::with_capacity(512).into();
        buf.push_no_metadata(b"0123456789").unwrap();
        buf.push_no_metadata(b"----------").unwrap();

        assert_eq!(2, buf.len());

        let mut iter = buf.iter();
        {
            let msg = iter.next().unwrap();
            assert_eq!(b"0123456789", msg.payload());
            let meta = msg.metadata();
            assert_eq!(0, meta.len());
        }
        {
            let msg = iter.next().unwrap();
            assert_eq!(b"----------", msg.payload());
            let meta = msg.metadata();
            assert_eq!(0, meta.len());
        }
        assert!(iter.next().is_none());
    }

}
