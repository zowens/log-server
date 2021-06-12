use bytes::BytesMut;
use prometheus::Counter;
use std::collections::VecDeque;
use std::rc::Rc;
use std::cell::RefCell;

lazy_static! {
    static ref UNPOOLED_BUFFER_CREATE: Counter = register_counter!(
        "msg_unpooled_buffer",
        "Number of buffers created due to pool depletion"
    )
    .unwrap();
}

/// Pool for writable bytes
#[derive(Clone)]
pub struct BytesPool {
    buf_capacity: usize,
    unused: Rc<RefCell<VecDeque<BytesMut>>>,
}

impl BytesPool {
    /// Creates a new pool for byte buffers with a given side capacity per byte buffer.
    pub fn new(buf_capacity: usize) -> BytesPool {
        BytesPool {
            buf_capacity,
            unused: Rc::new(RefCell::new(VecDeque::new())),
        }
    }

    /// Capacity per buffer.
    #[inline]
    pub fn buffer_capacity(&self) -> usize {
        self.buf_capacity
    }

    /// Pull a buffer from the pool, or create one.
    pub fn take(&mut self) -> BytesMut {
        // try to pull from the front, if that doesn't
        // work assume that the rest aren't drained either
        let front_bytes = self.unused.borrow_mut().pop_front();
        match front_bytes {
            Some(mut bytes_mut) => {
                bytes_mut.clear();
                bytes_mut
            }
            None => {
                UNPOOLED_BUFFER_CREATE.inc();
                BytesMut::with_capacity(self.buf_capacity)
            }
        }
    }

    /// Return an unused buffer, which may have outstanding references.
    #[inline]
    pub fn push(&mut self, buf: BytesMut) {
        self.unused.borrow_mut().push_back(buf);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BufMut;

    #[test]
    fn take_fresh() {
        let mut pool = BytesPool::new(1024);
        let buf = pool.take();
        assert_eq!(1024, buf.capacity());
        assert_eq!(0, buf.len());
    }

    #[test]
    fn take_retrned_buf() {
        let mut pool = BytesPool::new(1024);
        let mut buf = pool.take();
        buf.put_slice(b"test");
        assert_eq!(1024, buf.capacity());
        assert_eq!(4, buf.len());

        let data_ptr = buf.as_ptr();

        pool.push(buf);

        let buf = pool.take();
        assert_eq!(1024, buf.capacity());
        assert_eq!(0, buf.len());
        assert_eq!(data_ptr, buf.as_ptr());
    }
}
