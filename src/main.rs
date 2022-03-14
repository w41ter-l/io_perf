
#![feature(write_all_vectored)]

use io_uring::{opcode, squeue::Flags, types, IoUring};
use std::{
    collections::{HashMap, VecDeque},
    ops::{Deref, DerefMut, Add},
    os::unix::fs::OpenOptionsExt,
    os::unix::io::AsRawFd,
    sync::{Arc, Mutex},
    thread::JoinHandle, io::{Write, IoSlice}, char::MAX,
};

use async_trait::async_trait;
use futures::channel::oneshot;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

trait SequentialWriter {
    fn append(&mut self, bytes: Vec<u8>) -> Result<()>;
}

#[async_trait]
trait Env {
    async fn new_sequential_writer(&self) -> Box<dyn SequentialWriter>;

    fn submit_and_wait(&self, writer: &dyn SequentialWriter) -> Result<Box<dyn SequentialWriter>>;
}

struct WriteReq {
    payload: Box<[u8]>,
    sender: oneshot::Sender<u64>,
}

struct ChannelCore {
    requests: VecDeque<WriteReq>,
}

#[derive(Clone)]
struct Channel {
    core: Arc<Mutex<ChannelCore>>,
}

impl Channel {
    fn new() -> Self {
        Channel {
            core: Arc::new(Mutex::new(ChannelCore {
                requests: VecDeque::new(),
            })),
        }
    }

    fn write(&self, payload: Box<[u8]>) -> oneshot::Receiver<u64> {
        let mut core = self.core.lock().unwrap();
        let (sender, receiver) = oneshot::channel();
        core.requests.push_back(WriteReq { payload, sender });
        receiver
    }

    fn take(&self) -> VecDeque<WriteReq> {
        let mut core = self.core.lock().unwrap();
        std::mem::take(&mut core.requests)
    }
}

const BLOCK_SIZE: usize = 1024;

#[repr(align(1024))]
struct Aligned<const CHUNK_SIZE: usize>([u8; CHUNK_SIZE]);

impl<const CHUNK_SIZE: usize> Deref for Aligned<CHUNK_SIZE> {
    type Target = [u8; CHUNK_SIZE];
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<const CHUNK_SIZE: usize> DerefMut for Aligned<CHUNK_SIZE> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

struct IOBlock {
    space: Box<Aligned<MAX_BLOCK_SIZE>>,
    ref_count: usize,
    allocated: usize,
}

impl IOBlock {
    fn new() -> Self {
        IOBlock {
            ref_count: 0,
            allocated: 0,
            space: Box::new(Aligned([0u8; MAX_BLOCK_SIZE])),
        }
    }

    fn deref_block(raw_block: *mut IOBlock) {
        if let Some(block) = unsafe { raw_block.as_mut() } {
            block.ref_count -= 1;
            if block.ref_count == 0 {
                unsafe { Box::from_raw(block) };
            }
        }
    }
}

struct IOBlockRef {
    size: usize,
    ptr: *mut IOBlock,
}

impl IOBlockRef {
    fn new() -> Self {
        let mut block = Box::new(IOBlock::new());
        block.ref_count += 1;
        IOBlockRef {
            size: 0,
            ptr: Box::leak(block),
        }
    }

    fn consumed(&self) -> usize {
        let block = unsafe { &mut *self.ptr };
        block.allocated + self.size
    }

    #[inline(always)]
    fn len(&self) -> usize {
        self.size
    }

    fn free(&self) -> usize {
        let block = unsafe { &mut *self.ptr };
        MAX_BLOCK_SIZE - block.allocated - self.size
    }

    fn extend(&mut self, src: &[u8]) {
        let block = unsafe { &mut *self.ptr };
        let first_free = block.allocated + self.size;
        assert!(first_free + src.len() <= MAX_BLOCK_SIZE);
        let space = &mut block.space[first_free..];
        space[..src.len()].copy_from_slice(src);
        self.size += src.len();
    }

    fn extend_zero(&mut self, size: usize) {
        let block = unsafe { &mut *self.ptr };
        let first_free = block.allocated + self.size;
        assert!(first_free + size <= MAX_BLOCK_SIZE);
        let space = &mut block.space[first_free..];
        space[..size].fill(0);
        self.size += size;
    }

    fn split(&mut self) -> IOBlockView {
        let block = unsafe { &mut *self.ptr };
        let begin = block.allocated;
        let end = block.allocated + self.size;
        let buf = &block.space[begin..end];

        block.allocated += self.size;
        self.size = 0;
        block.ref_count += 1;

        IOBlockView { ptr: self.ptr, buf }
    }
}

impl Drop for IOBlockRef {
    fn drop(&mut self) {
        IOBlock::deref_block(self.ptr);
    }
}

struct IOBlockView {
    ptr: *mut IOBlock,
    buf: &'static [u8],
}

impl Drop for IOBlockView {
    fn drop(&mut self) {
        IOBlock::deref_block(self.ptr);
    }
}

struct Completion {
    requests: Vec<(u64, oneshot::Sender<u64>)>,
    block_ref: IOBlockView,
    iov: libc::iovec,
}

impl Completion {
    fn new(requests: Vec<(u64, oneshot::Sender<u64>)>, block_ref: IOBlockView) -> Self {
        let iov = libc::iovec {
            iov_base: (block_ref.buf as *const [u8]) as *mut _,
            iov_len: block_ref.buf.len(),
        };
        Completion {
            requests,
            block_ref,
            iov,
        }
    }

    fn on_complete(self, result: i32) {
        if result >= 0 {
            assert!(self.block_ref.buf.len() == result as usize);
            for (seq, sender) in self.requests {
                sender.send(seq).unwrap_or_default();
            }
        } else {
            for (seq, sender) in self.requests {
                // TODO send error
                sender.send(seq).unwrap_or_default();
            }
        }
    }
}

// +---------+-----------+-----------+----------------+--- ... ---+
// |CRC (4B) | Size (2B) | Type (1B) | Log number (4B)| Payload   |
// +---------+-----------+-----------+----------------+--- ... ---+
const MAX_BLOCK_SIZE: usize = 1024 * 32;
const RECORD_HEADER_SIZE: usize = 11;
const RECORD_HEAD: u8 = 1;
const RECORD_MID: u8 = 2;
const RECORD_TAIL: u8 = 3;
const RECORD_FULL: u8 = 4;

#[derive(Default)]
struct IoStats {
    num_busy: usize,
    num_files: usize,
    num_reqs: usize,
    num_bytes: usize,
}

impl std::fmt::Display for IoStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "io stats: busy {}, reqs {}, bytes {}, files {}",
            self.num_busy, self.num_reqs, self.num_bytes, self.num_files
        )
    }
}

struct Worker {
    ring: IoUring,

    next_req_id: u64,
    pending_completions: HashMap<u64, Box<Completion>>,
    drain_req: Option<u64>,

    block_ref: IOBlockRef,
    doing: Option<Box<Completion>>,

    offset: usize,
    max_offset: usize,

    cached_requests: Vec<(u64, oneshot::Sender<u64>)>,

    // The requests still need to finished.
    requests: VecDeque<WriteReq>,

    // The number of bytes the first req are consumed.
    first_req_consumed: usize,

    stats: IoStats,
}

impl Worker {
    /// Try append a entry into ring, or save it to doing if ring is full.
    fn append<Writer: AsRawFd>(&mut self, writer: &Writer, completion: Box<Completion>) -> bool {
        let write = opcode::Writev::new(types::Fd(writer.as_raw_fd()), &completion.iov, 1)
            .offset(self.offset as i64)
            .build()
            .flags(Flags::IO_LINK)
            .user_data(self.next_req_id);
        // let sync_file_range = opcode::SyncFileRange::new(types::Fd(writer.as_raw_fd()), completion.iov.iov_len as u32)
        //     .offset(self.offset as i64)
        //     .flags(libc::SYNC_FILE_RANGE_WRITE | libc::SYNC_FILE_RANGE_WAIT_AFTER)
        //     .build()
        //     .user_data(self.next_req_id + 1);

        let data_sync = opcode::Fsync::new(types::Fd(writer.as_raw_fd()))
            .flags(io_uring::types::FsyncFlags::DATASYNC)
            .build()
            .user_data(self.next_req_id);

        let completion_capacity = self.ring.completion().capacity();
        if self.pending_completions.len() >= completion_capacity
            || unsafe { self.ring.submission().push_multiple(&[write]) }.is_err()
        {
            self.stats.num_busy += 1;
            // println!("submission is fulled");
            // submission queue is fulled.
            self.doing = Some(completion);
            false
        } else {
            self.ring.submit_and_wait(0).expect("");
            self.offset += completion.block_ref.buf.len();
            self.stats.num_bytes += completion.block_ref.buf.len();
            self.pending_completions
                .insert(self.next_req_id, completion);
            self.next_req_id += 1;
            true
        }
    }

    fn consume_next_request(&mut self) -> bool {
        if self.requests.is_empty() {
            println!(" no requests");
            return false;
        }

        let capacity = self.block_ref.free();
        debug_assert!(capacity > RECORD_HEADER_SIZE);

        let front = self.requests.front().unwrap();
        let left_payload_size = front.payload.len() - self.first_req_consumed;
        let size = (capacity - RECORD_HEADER_SIZE).min(left_payload_size);
        let payload = &front.payload[self.first_req_consumed..(self.first_req_consumed + size)];
        let crc = crc32fast::hash(payload);
        let kind = if size == front.payload.len() {
            RECORD_FULL
        } else if self.first_req_consumed == 0 {
            RECORD_HEAD
        } else if size + self.first_req_consumed == front.payload.len() {
            RECORD_TAIL
        } else {
            RECORD_MID
        };
        // println!("kind is {} consumed {}", kind, self.first_req_consumed);

        self.block_ref.extend(&crc.to_le_bytes());
        self.block_ref.extend(&(size as u16).to_le_bytes());
        self.block_ref.extend(&[kind]);
        self.block_ref.extend(&(1 as u32).to_le_bytes());
        self.block_ref.extend(payload);
        self.first_req_consumed += size;
        if kind == RECORD_TAIL || kind == RECORD_FULL {
            self.first_req_consumed = 0;

            // FIXME: setup requests id
            self.cached_requests
                .push((0, self.requests.pop_front().unwrap().sender));
        }

        true
    }

    fn submit_switch_task<Writer: AsRawFd>(&mut self, writer: &Writer) {
        if self.drain_req.is_none() {
            let completion_capacity = self.ring.completion().capacity();
            if completion_capacity <= self.pending_completions.len() * 2{
                return;
            }

            // need switch file.
            let drain = opcode::Nop::new()
                .build()
                .flags(Flags::IO_DRAIN)
                .user_data(self.next_req_id);

            if unsafe { self.ring.submission().push(&drain) }.is_ok() {
                self.ring.submit_and_wait(0).expect("");
                // println!(
                //     "submit drain req success, req id {}, submission len {}",
                //     self.next_req_id,
                //     self.ring.submission().len()
                // );
                self.drain_req = Some(self.next_req_id);
                self.next_req_id += 1;
            }
        }
    }
}

fn log_worker_sync_with_block_size(
    channel: Channel,
    dir: String,
    per_file_size: usize,
    capacity: usize,
    direct: bool,
) -> JoinHandle<()> {
    std::thread::spawn(move || {
        let mut files = VecDeque::new();
        let mut num_files = capacity / per_file_size;
        if num_files == 0 {
            num_files = 1;
        }
        for i in 0..num_files {
            let file = std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .custom_flags(if direct { libc::O_DIRECT } else { 0 })
                .open(format!("{}/{}.log", dir, i))
                .expect("open");
            unsafe {
                if libc::fallocate(file.as_raw_fd(), 0, 0, per_file_size as i64) != 0 {
                    panic!("{:?}", std::io::Error::last_os_error());
                }
            }
            file.sync_all().unwrap();
            files.push_back(file);
        }
        println!("total {} files", files.len());

        let mut writer = files.pop_front().unwrap();
        let mut requests = VecDeque::new();
        let mut offset: usize = 0;
        let mut first_req_consumed: usize = 0;
        let mut cached_requests: Vec<(u64, oneshot::Sender<u64>)> = Vec::new();
        let mut block_ref = IOBlockRef::new();
        loop {
            requests = channel.take();
            while !requests.is_empty() {
                let front = requests.front().unwrap();
                if offset + front.payload.len() + RECORD_HEADER_SIZE > per_file_size {
                    // println!("switch file");
                    // switch file.
                    if offset < per_file_size {
                        block_ref.extend_zero(per_file_size - offset);
                    }
                    if block_ref.len() > 0 {
                        let view = block_ref.split();
                        writer.write_all(view.buf).unwrap();
                        writer.sync_data().unwrap();
                    }
                    files.push_back(writer);
                    writer = files.pop_front().unwrap();
                    block_ref = IOBlockRef::new();
                    offset = 0;

                    for (seq, sender) in std::mem::take(&mut cached_requests) {
                        sender.send(seq).unwrap();
                    }
                }

                let capacity = MAX_BLOCK_SIZE - (offset % MAX_BLOCK_SIZE);
                let left_payload_size = front.payload.len() - first_req_consumed;
                let size = (capacity - RECORD_HEADER_SIZE).min(left_payload_size);
                let payload = &front.payload[first_req_consumed..(first_req_consumed + size)];
                let crc = crc32fast::hash(payload);
                let kind = if size == front.payload.len() {
                    RECORD_FULL
                } else if first_req_consumed == 0 {
                    RECORD_HEAD
                } else if size + first_req_consumed == front.payload.len() {
                    RECORD_TAIL
                } else {
                    RECORD_MID
                };

                // println!("block_ref consumed {}, len {} offset {} block capacity {}", block_ref.consumed(), block_ref.len(), offset, offset % MAX_BLOCK_SIZE);
                block_ref.extend(&crc.to_le_bytes());
                block_ref.extend(&(size as u16).to_le_bytes());
                block_ref.extend(&[kind]);
                block_ref.extend(&(1 as u32).to_le_bytes());
                block_ref.extend(&payload);
                first_req_consumed += size;
                offset += RECORD_HEADER_SIZE + size;

                if kind == RECORD_TAIL || kind == RECORD_FULL {
                    first_req_consumed = 0;

                    // FIXME: setup requests id
                    cached_requests
                        .push((0, requests.pop_front().unwrap().sender));
                }
                let capacity = MAX_BLOCK_SIZE - (offset % MAX_BLOCK_SIZE);
                // println!("block free space is {}", MAX_BLOCK_SIZE - (offset % MAX_BLOCK_SIZE));
                // block free space is less than a record.
                if capacity <= RECORD_HEADER_SIZE || offset % MAX_BLOCK_SIZE == 0 {
                    // println!("switch block");
                    if offset % MAX_BLOCK_SIZE != 0 {
                        block_ref.extend_zero(capacity);
                        offset += capacity;
                    }
                    let view = block_ref.split();
                    writer.write_all(view.buf).unwrap();
                    writer.sync_data().unwrap();
                    block_ref = IOBlockRef::new();

                    for (seq, sender) in std::mem::take(&mut cached_requests) {
                        sender.send(seq).unwrap();
                    }
                }
            }

            if !cached_requests.is_empty() {
                let capacity = 4096 - (offset % 4096);
                if capacity < 4096 {
                    // println!("extend block ref {}", capacity);
                    block_ref.extend_zero(capacity);
                    let view = block_ref.split();
                    writer.write_all(view.buf).unwrap();
                    offset += capacity;
                    if block_ref.consumed() == MAX_BLOCK_SIZE {
                        block_ref = IOBlockRef::new();
                    }
                }

                writer.sync_data().unwrap();
                for (seq, sender) in std::mem::take(&mut cached_requests) {
                    sender.send(seq).unwrap();
                }
            }
        }
    })
}

fn log_worker_sync(
    channel: Channel,
    dir: String,
    per_file_size: usize,
    capacity: usize,
    sync_file_range: bool,
) -> JoinHandle<()> {
    std::thread::spawn(move || {
        let mut files = VecDeque::new();
        let mut num_files = capacity / per_file_size;
        if num_files == 0 {
            num_files = 1;
        }
        for i in 0..num_files {
            let file = std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .open(format!("{}/{}.log", dir, i))
                .expect("open");
            unsafe {
                if libc::fallocate(file.as_raw_fd(), 0, 0, per_file_size as i64) != 0 {
                    panic!("{:?}", std::io::Error::last_os_error());
                }
            }
            file.sync_all().unwrap();
            files.push_back(file);
        }
        println!("total {} files", files.len());

        let mut writer = files.pop_front().unwrap();
        let mut requests = VecDeque::new();
        let mut offset: usize = 0;
        let mut synced_offset: usize = 0;
        let mut first_req_consumed: usize = 0;
        let mut cached_requests: Vec<(u64, oneshot::Sender<u64>)> = Vec::new();
        loop {
            requests = channel.take();
            while !requests.is_empty() {
                let front = requests.front().unwrap();
                if offset + front.payload.len() + RECORD_HEADER_SIZE > per_file_size {
                    // switch file.
                    if offset < per_file_size {
                        let buf = vec![0u8; per_file_size - offset];
                        writer.write_all(&buf).unwrap();
                        writer.sync_data().unwrap();
                    }
                    files.push_back(writer);
                    writer = files.pop_front().unwrap();
                    offset = 0;
                    synced_offset = 0;

                    for (seq, sender) in std::mem::take(&mut cached_requests) {
                        sender.send(seq).unwrap();
                    }
                }

                let capacity = MAX_BLOCK_SIZE - (offset % MAX_BLOCK_SIZE);
                let left_payload_size = front.payload.len() - first_req_consumed;
                let size = (capacity - RECORD_HEADER_SIZE).min(left_payload_size);
                let payload = &front.payload[first_req_consumed..(first_req_consumed + size)];
                let crc = crc32fast::hash(payload);
                let kind = if size == front.payload.len() {
                    RECORD_FULL
                } else if first_req_consumed == 0 {
                    RECORD_HEAD
                } else if size + first_req_consumed == front.payload.len() {
                    RECORD_TAIL
                } else {
                    RECORD_MID
                };

                let mut header = vec![];
                header.extend_from_slice(&crc.to_le_bytes());
                header.extend_from_slice(&(size as u16).to_le_bytes());
                header.extend_from_slice(&[kind]);
                header.extend_from_slice(&(1 as u32).to_le_bytes());
                first_req_consumed += size;

                let bufs = &mut [
                    IoSlice::new(&header),
                    IoSlice::new(&payload),
                ];
                writer.write_all_vectored(bufs).unwrap();
                offset += RECORD_HEADER_SIZE + size;

                if kind == RECORD_TAIL || kind == RECORD_FULL {
                    first_req_consumed = 0;

                    // FIXME: setup requests id
                    cached_requests
                        .push((0, requests.pop_front().unwrap().sender));
                }
                let capacity = MAX_BLOCK_SIZE - (offset % MAX_BLOCK_SIZE);
                if capacity <= RECORD_HEADER_SIZE || offset % MAX_BLOCK_SIZE == 0 {
                    // align block
                    if offset % MAX_BLOCK_SIZE != 0 {
                        let buf = vec![0u8; capacity];
                        offset += capacity;
                        writer.write_all(&buf).unwrap();
                    }
                    writer.sync_data().unwrap();
                    synced_offset = offset;
                    for (seq, sender) in std::mem::take(&mut cached_requests) {
                        sender.send(seq).unwrap();
                    }
                } else if sync_file_range && offset - synced_offset >= 4096 {
                    let num_pages = (offset - synced_offset) % 4096;
                    unsafe {
                        libc::sync_file_range(
                            writer.as_raw_fd(),
                            synced_offset as i64,
                            (num_pages * 4096) as i64,
                            libc::SYNC_FILE_RANGE_WRITE);
                    }
                    synced_offset += num_pages * 4096;
                }
            }

            if !cached_requests.is_empty() {
                // align Page, avoid partial page write.
                let capacity = 4096 - (offset % 4096);
                if capacity < 4096 {
                    let buf = vec![0u8; capacity];
                    writer.write_all(&buf).unwrap();
                    offset += capacity;
                }

                writer.sync_data().unwrap();
                synced_offset = offset;
                for (seq, sender) in std::mem::take(&mut cached_requests) {
                    sender.send(seq).unwrap();
                }
            }
        }
    })
}

fn log_worker(
    channel: Channel,
    dir: String,
    per_file_size: usize,
    capacity: usize,
) -> JoinHandle<()> {
    std::thread::spawn(move || {
        let mut files: VecDeque<std::fs::File> = VecDeque::new();
        let mut num_files = capacity / per_file_size;
        if num_files == 0 {
            num_files = 1;
        }
        for i in 0..num_files {
            let file = std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .custom_flags(libc::O_DIRECT)
                .open(format!("{}/{}.log", dir, i))
                .expect("open");
            unsafe {
                if libc::fallocate(file.as_raw_fd(), 0, 0, per_file_size as i64) != 0 {
                    panic!("{:?}", std::io::Error::last_os_error());
                }
            }
            file.sync_all().unwrap();
            files.push_back(file);
        }
        println!("total {} files", files.len());

        let mut writer = files.pop_front().unwrap();

        let ring = IoUring::builder()
            .dontfork()
            .setup_sqpoll(1002)
            .build(32)
            .unwrap();

        let mut w = Worker {
            ring,
            next_req_id: 0,
            pending_completions: HashMap::new(),
            drain_req: None,
            block_ref: IOBlockRef::new(),
            doing: None,
            offset: 0,
            max_offset: per_file_size,
            cached_requests: Vec::new(),
            requests: VecDeque::new(),
            first_req_consumed: 0,
            stats: IoStats::default(),
        };
        loop {
            // TODO:
            // 2. how to lazy polling?
            if !w.pending_completions.is_empty() || w.drain_req.is_some() {
                while let Some(entry) = w.ring.completion().next() {
                    w.stats.num_reqs += 1;
                    let req_id = entry.user_data();
                    if w.drain_req.map(|r| r == req_id).unwrap_or_default() {
                        // all tasks are drained, it is success to switch writer.
                        let new_writer = files.pop_front().unwrap();
                        files.push_back(writer);
                        writer = new_writer;
                        w.drain_req = None;
                        w.offset = 0;
                        w.stats.num_files += 1;
                        println!("switch to next io file: {}", w.stats);
                        continue;
                    }

                    let result = entry.result();
                    w.pending_completions
                        .remove(&req_id)
                        .map(|c| c.on_complete(result));
                }

            }
            if let Some(completion) = w.doing.take() {
                if !w.append(&writer, completion) {
                    // println!("still doing");
                    // still fulled, try again
                    continue;
                }
                // println!("doing is submitted");
            }

            if w.drain_req.is_none() && w.offset == w.max_offset {
                w.submit_switch_task(&writer);
            }
            if w.drain_req.is_some() {
                w.stats.num_busy += 1;
                // let overflow = w.ring.completion().overflow();
                // let len = w.ring.completion().len();
                // let sub_len = w.ring.submission().len();
                // if len > 0 || overflow > 0 || sub_len > 0 {
                //     println!(
                //         "wait drain req, overflow {}, len {} sub len {}",
                //         overflow, len, sub_len
                //     );
                // }
                continue;
            }

            if w.requests.is_empty() {
                w.requests = channel.take();
                if w.requests.is_empty() {
                    std::thread::yield_now();
                    // } else {
                    //     println!("take {} requests", w.requests.len());
                }
            }

            // TODO if the pending buf isn't enough to full?
            while !w.requests.is_empty() {
                let front = w.requests.front().unwrap();
                // println!(
                //     "try advance request, consumed {}, capacity {}, size {} payload {}",
                //     w.block_ref.consumed(),
                //     w.block_ref.free(),
                //     w.block_ref.len(),
                //     front.payload.len()
                // );
                if w.offset + front.payload.len() > w.max_offset {
                    w.block_ref.extend_zero(w.max_offset - w.offset);
                    // println!("out of pages, offset {}, max {}", w.offset, w.max_offset);
                    break;
                }

                if !w.consume_next_request() {
                    println!("consume failed");
                    break;
                }

                let capacity = w.block_ref.free();
                if capacity <= RECORD_HEADER_SIZE {
                    w.block_ref.extend_zero(capacity);
                }

                // If block is fulled, switch to next block
                if w.block_ref.consumed() == MAX_BLOCK_SIZE {
                    let completion = Box::new(Completion::new(
                        std::mem::take(&mut w.cached_requests),
                        w.block_ref.split(),
                    ));
                    // switch to next block.
                    w.block_ref = IOBlockRef::new();
                    if !w.append(&writer, completion) {
                        // println!("append channel full switch block");
                        // already fulled
                        break;
                    }
                }
            }
            // let overflow = w.ring.completion().overflow();
            // let len = w.ring.completion().len();
            // println!(
            //     "break loop, consumed {}, size {} drain {} doint {} requests {} pending {} capacity {}, completion {} {}",
            //     w.block_ref.consumed(),
            //     w.block_ref.len(),
            //     w.drain_req.is_some(),
            //     w.doing.is_some(),
            //     w.requests.len(),
            //     w.pending_completions.len(),
            //     w.ring.submission().capacity(),
            //     overflow,
            //     len,
            // );

            if w.block_ref.len() > 0 && w.doing.is_none() {
                // Align to BLOCK SIZE.
                let exceeds = w.block_ref.consumed() % BLOCK_SIZE;
                w.block_ref.extend_zero(BLOCK_SIZE - exceeds);

                let completion = Box::new(Completion::new(
                    std::mem::take(&mut w.cached_requests),
                    w.block_ref.split(),
                ));
                if w.block_ref.consumed() == MAX_BLOCK_SIZE {
                    w.block_ref = IOBlockRef::new();
                }
                if !w.append(&writer, completion) {
                    // println!("append channel full");
                    continue;
                }
            }
        }
        println!("break to here");
    })
}

#[tokio::main(flavor = "multi_thread", worker_threads = 7)]
async fn main() {
    let channel = Channel::new();
    let handle = log_worker_sync(
        channel.clone(),
        "./engine/".to_owned(),
        256 * 1024 * 1024,
        2560 * 1024 * 1024,
        true,
    );

    // let channel2 = Channel::new();
    // let handle = log_worker(
    //     channel.clone(),
    //     "./engine2/".to_owned(),
    //     256 * 1024 * 1024,
    //     2560 * 1024 * 1024,
    // );

    for i in 0..6 {
        let cloned_channel = channel.clone();
        tokio::spawn(async move {
            use std::time::{Instant, Duration};
            let mut duration = Duration::default();
            let mut total: usize = 0;
            let mut futures = VecDeque::new();
            let mut start_at = Instant::now();
            loop {
                while futures.len() < 1024 {
                    futures.push_back((Instant::now(), cloned_channel.write(Box::new([i; 1024]))));
                }
                while futures.len() > 512 {
                    if let Some((begin_at, future)) = futures.pop_front() {
                        future.await.unwrap_or_default();
                        duration = duration.add(Instant::now() - begin_at);
                        total += 1;
                    }
                }
                if total >= 1000000 {
                    let total_duration = Instant::now() - start_at;
                    println!("total {} ops, duration {} ms, avg duration {} us, per request avg duration {} us",
                        total, total_duration.as_millis(), total_duration.as_micros() / (total as u128), 
                        duration.as_micros() / (total as u128)
                    );
                    duration = Duration::default();
                    start_at = Instant::now();
                    total = 0;
                }
            }
        });
    }
    // for i in 0..6 {
    //     let cloned_channel = channel2.clone();
    //     tokio::spawn(async move {
    //         use std::time::{Instant, Duration};
    //         let mut duration = Duration::default();
    //         let mut total: usize = 0;
    //         let mut futures = VecDeque::new();
    //         let mut start_at = Instant::now();
    //         loop {
    //             while futures.len() < 1024 {
    //                 futures.push_back((Instant::now(), cloned_channel.write(Box::new([i; 1024]))));
    //             }
    //             while futures.len() > 512 {
    //                 if let Some((begin_at, future)) = futures.pop_front() {
    //                     future.await.unwrap_or_default();
    //                     duration = duration.add(Instant::now() - begin_at);
    //                     total += 1;
    //                 }
    //             }
    //             if total >= 1000000 {
    //                 let total_duration = Instant::now() - start_at;
    //                 println!("total {} ops, duration {} ms, avg duration {} us, per request avg duration {} us",
    //                     total, total_duration.as_millis(), total_duration.as_micros() / (total as u128), 
    //                     duration.as_micros() / (total as u128)
    //                 );
    //                 duration = Duration::default();
    //                 start_at = Instant::now();
    //                 total = 0;
    //             }
    //         }
    //     });
    // }

    loop {
        std::thread::sleep(std::time::Duration::from_secs(1));
    }

    handle.join().unwrap_or_default();
}
