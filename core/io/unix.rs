use super::{Completion, File, OpenFlags, SharedWalLockKind, SharedWalMappedRegion, IO};
use crate::error::{io_error, CompletionError, LimboError};
use crate::io::clock::{Clock, DefaultClock, MonotonicInstant, WallClockInstant};
use crate::io::common;
use crate::io::FileSyncType;
use crate::sync::Mutex;
use crate::thread::{self, JoinHandle};
use crate::Result;
use rustix::{
    fd::{AsFd, AsRawFd},
    fs::{self, FlockOperation},
};
use std::os::fd::RawFd;
use std::ptr::NonNull;
use std::sync::Weak;

#[cfg(shuttle)]
use shuttle::sync::{mpsc, Condvar, Mutex as WaitMutex};
#[cfg(not(shuttle))]
use std::sync::{mpsc, Condvar, Mutex as WaitMutex};

use std::{io::ErrorKind, sync::Arc};
#[cfg(feature = "fs")]
use tracing::debug;
use tracing::{instrument, trace, Level};

// Darwin fails pwrite() and pwritev() calls with buffer size larger than INT_MAX so let's treat
// that as maximum buffer size.
const MAX_PWRITE_LEN: usize = i32::MAX as usize;

const MAX_IOV: usize = 1024;

pub struct UnixIO {
    sync_queue: Arc<Mutex<SyncQueue>>,
    sync_thread: Option<JoinHandle<()>>,
    wake_thread: Option<JoinHandle<()>>,
    #[cfg(test)]
    worker_lifetime: Arc<()>,
    #[cfg(test)]
    wake_lifetime: Arc<()>,
    #[cfg(test)]
    sync_hook: Arc<crate::sync::Mutex<Option<SyncHook>>>,
}

#[cfg(test)]
type SyncHook = Arc<dyn Fn(FileSyncType) -> std::result::Result<(), CompletionError> + Send + Sync>;

impl UnixIO {
    #[cfg(feature = "fs")]
    pub fn new() -> Result<Self> {
        debug!("Using IO backend 'syscall'");
        let (sender, receiver) = mpsc::channel::<SyncJob>();
        let (wake_sender, wake_receiver) = mpsc::channel::<SyncNotification>();
        #[cfg(test)]
        let wake_lifetime = Arc::new(());
        #[cfg(test)]
        let wake_lifetime_for_thread = wake_lifetime.clone();
        let wake_thread = thread::Builder::new()
            .name("turso-sync-wake".to_owned())
            .spawn(move || {
                #[cfg(test)]
                let _wake_lifetime = wake_lifetime_for_thread;
                while let Ok(notification) = wake_receiver.recv() {
                    let _result = notification.result.wait();
                    if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        notification.context.wake();
                    }))
                    .is_err()
                    {
                        tracing::error!("Unix sync waker panicked");
                    }
                }
            })
            .map_err(|err| io_error(err, "start sync wake worker"))?;
        #[cfg(test)]
        let worker_lifetime = Arc::new(());
        #[cfg(test)]
        let worker_lifetime_for_thread = worker_lifetime.clone();
        let sync_thread =
            match thread::Builder::new()
                .name("turso-sync".to_owned())
                .spawn(move || {
                    #[cfg(test)]
                    let _worker_lifetime = worker_lifetime_for_thread;
                    while let Ok(mut job) = receiver.recv() {
                        job.run();
                    }
                }) {
                Ok(worker) => worker,
                Err(err) => {
                    drop(wake_sender);
                    wake_thread
                        .join()
                        .expect("sync wake worker failed during startup");
                    return Err(io_error(err, "start sync worker"));
                }
            };
        Ok(Self {
            sync_queue: Arc::new(Mutex::new(SyncQueue {
                sender: Some(sender),
                wake_sender: Some(wake_sender),
                pending: Vec::new(),
            })),
            sync_thread: Some(sync_thread),
            wake_thread: Some(wake_thread),
            #[cfg(test)]
            worker_lifetime,
            #[cfg(test)]
            wake_lifetime,
            #[cfg(test)]
            sync_hook: Arc::new(crate::sync::Mutex::new(None)),
        })
    }
}

impl Clock for UnixIO {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        DefaultClock.current_time_monotonic()
    }

    fn current_time_wall_clock(&self) -> WallClockInstant {
        DefaultClock.current_time_wall_clock()
    }
}

impl IO for UnixIO {
    fn supports_shared_wal_coordination(&self) -> bool {
        true
    }

    fn open_file(&self, path: &str, flags: OpenFlags, _direct: bool) -> Result<Arc<dyn File>> {
        trace!("open_file(path = {})", path);
        let mut file = std::fs::File::options();
        file.read(true);

        if !flags.contains(OpenFlags::ReadOnly) {
            file.write(true);
            file.create(flags.contains(OpenFlags::Create));
        }

        let file = file.open(path).map_err(|e| io_error(e, "open"))?;

        #[allow(clippy::arc_with_non_send_sync)]
        let unix_file = Arc::new(UnixFile {
            file: Arc::new(file),
            path: path.to_string(),
            sync_queue: Arc::downgrade(&self.sync_queue),
            pending_sync: Mutex::new(Weak::new()),
            #[cfg(test)]
            sync_hook: self.sync_hook.clone(),
        });
        if std::env::var(common::ENV_DISABLE_FILE_LOCK).is_err()
            && !flags.intersects(OpenFlags::ReadOnly | OpenFlags::NoLock)
        {
            unix_file.lock_file(true)?;
        }
        Ok(unix_file)
    }

    fn remove_file(&self, path: &str) -> Result<()> {
        std::fs::remove_file(path).map_err(|e| io_error(e, "remove_file"))?;
        Ok(())
    }

    #[instrument(err, skip_all, level = Level::TRACE)]
    fn step(&self) -> Result<()> {
        loop {
            let pending = {
                let mut queue = self.sync_queue.lock();
                let Some(index) = queue
                    .pending
                    .iter()
                    .position(|pending| pending.result.value.lock().unwrap().is_some())
                else {
                    break;
                };
                queue.pending.remove(index)
            };
            match pending.result.wait() {
                Ok(()) => pending.completion.complete(0),
                Err(err) => pending.completion.error(err),
            }
        }
        Ok(())
    }

    fn cancel(&self, completions: &[Completion]) -> Result<()> {
        self.wait_for_syncs(completions);
        completions.iter().for_each(Completion::abort);
        completions.iter().for_each(Completion::step_io);
        self.step()
    }

    fn drain_completions(&self, completions: &[Completion]) -> Result<()> {
        self.wait_for_syncs(completions);
        completions.iter().for_each(Completion::step_io);
        while completions.iter().any(|c| !c.finished()) {
            completions.iter().for_each(Completion::step_io);
            self.step()?;
            thread::yield_now();
        }
        self.step()
    }

    fn wait_for_completion(&self, c: Completion) -> Result<()> {
        self.drain_completions(std::slice::from_ref(&c))?;
        c.get_error().map_or(Ok(()), |err| Err(err.into()))
    }
}

impl UnixIO {
    fn wait_for_syncs(&self, completions: &[Completion]) {
        completions.iter().for_each(Completion::wait_for_io);
    }
}

impl Drop for UnixIO {
    fn drop(&mut self) {
        {
            let mut queue = self.sync_queue.lock();
            queue.sender.take();
            queue.wake_sender.take();
        }
        if let Some(worker) = self.sync_thread.take() {
            if worker.join().is_err() {
                tracing::error!("Unix sync worker panicked");
            }
        }
        if let Some(worker) = self.wake_thread.take() {
            if worker.thread().id() != thread::current().id() && worker.join().is_err() {
                tracing::error!("Unix sync wake worker panicked");
            }
        }
        self.step()
            .expect("failed to dispatch sync results during shutdown");
    }
}

struct SyncQueue {
    sender: Option<mpsc::Sender<SyncJob>>,
    wake_sender: Option<mpsc::Sender<SyncNotification>>,
    pending: Vec<PendingSync>,
}

struct PendingSync {
    completion: Completion,
    result: Arc<SyncResult>,
}

struct SyncJob {
    file: Option<Arc<std::fs::File>>,
    sync_type: FileSyncType,
    result: Arc<SyncResult>,
    #[cfg(test)]
    hook: Option<SyncHook>,
}

impl SyncJob {
    fn run(&mut self) {
        let result = self.sync_file();
        self.file.take();
        *self.result.value.lock().unwrap() = Some(result);
    }

    fn sync_file(&self) -> std::result::Result<(), CompletionError> {
        #[cfg(test)]
        if let Some(hook) = &self.hook {
            hook(self.sync_type)?;
        }
        sync_file(self.file.as_ref().unwrap(), self.sync_type)
    }
}

impl Drop for SyncJob {
    fn drop(&mut self) {
        self.file.take();
        self.result
            .value
            .lock()
            .unwrap()
            .get_or_insert(Err(CompletionError::IOError(
                ErrorKind::BrokenPipe,
                "sync worker stopped",
            )));
        self.result.retired.notify_all();
    }
}

struct SyncNotification {
    result: Arc<SyncResult>,
    context: super::completions::Context,
}

struct SyncResult {
    value: WaitMutex<Option<std::result::Result<(), CompletionError>>>,
    retired: Condvar,
    queue: Weak<Mutex<SyncQueue>>,
}

impl super::completions::CompletionOwner for SyncResult {
    fn step(&self) {
        if self.value.lock().unwrap().is_none() {
            return;
        }
        let pending = self.queue.upgrade().and_then(|queue| {
            let mut queue = queue.lock();
            let index = queue
                .pending
                .iter()
                .position(|pending| std::ptr::eq(pending.result.as_ref(), self))?;
            Some(queue.pending.remove(index))
        });
        if let Some(pending) = pending {
            match self.wait() {
                Ok(()) => pending.completion.complete(0),
                Err(err) => pending.completion.error(err),
            }
        }
    }

    fn wait(&self) {
        let _result = SyncResult::wait(self);
    }

    fn finished(&self) -> bool {
        self.value.lock().unwrap().is_some()
    }
}

impl SyncResult {
    fn wait(&self) -> std::result::Result<(), CompletionError> {
        let mut value = self.value.lock().unwrap();
        loop {
            if let Some(result) = *value {
                return result;
            }
            value = self.retired.wait(value).unwrap();
        }
    }
}

pub struct UnixFile {
    file: Arc<std::fs::File>,
    path: String,
    sync_queue: Weak<Mutex<SyncQueue>>,
    pending_sync: Mutex<Weak<SyncResult>>,
    #[cfg(test)]
    sync_hook: Arc<crate::sync::Mutex<Option<SyncHook>>>,
}

pub(crate) struct UnixSharedWalMapping {
    mapping_ptr: NonNull<u8>,
    mapping_len: usize,
    ptr: NonNull<u8>,
    len: usize,
}

unsafe impl Send for UnixSharedWalMapping {}
unsafe impl Sync for UnixSharedWalMapping {}

impl SharedWalMappedRegion for UnixSharedWalMapping {
    fn ptr(&self) -> NonNull<u8> {
        self.ptr
    }

    fn len(&self) -> usize {
        self.len
    }
}

impl Drop for UnixSharedWalMapping {
    fn drop(&mut self) {
        let rc = unsafe { libc::munmap(self.mapping_ptr.as_ptr().cast(), self.mapping_len) };
        if rc != 0 {
            // Log rather than panic — panicking in Drop aborts if we're already
            // unwinding (double panic).
            tracing::error!(
                "munmap failed for shared WAL coordination region: {}",
                std::io::Error::last_os_error()
            );
        }
    }
}

pub(crate) fn unix_shared_wal_lock_byte(
    fd: RawFd,
    offset: u64,
    exclusive: bool,
    blocking: bool,
    kind: SharedWalLockKind,
) -> Result<bool> {
    let mut flock = libc::flock {
        l_type: if exclusive {
            libc::F_WRLCK as libc::c_short
        } else {
            libc::F_RDLCK as libc::c_short
        },
        l_whence: libc::SEEK_SET as libc::c_short,
        l_start: offset as libc::off_t,
        l_len: 1,
        l_pid: 0,
        #[cfg(target_os = "freebsd")]
        l_sysid: 0,
    };
    let cmd = match (kind, blocking) {
        #[cfg(target_os = "linux")]
        (SharedWalLockKind::LinuxOfd, true) => libc::F_OFD_SETLKW,
        #[cfg(target_os = "linux")]
        (SharedWalLockKind::LinuxOfd, false) => libc::F_OFD_SETLK,
        (SharedWalLockKind::ProcessScopedFcntl, true) => libc::F_SETLKW,
        (SharedWalLockKind::ProcessScopedFcntl, false) => libc::F_SETLK,
        #[cfg(not(target_os = "linux"))]
        (SharedWalLockKind::LinuxOfd, _) => {
            return Err(LimboError::InternalError(
                "linux OFD locks are not supported on this platform".into(),
            ))
        }
    };
    loop {
        let rc = unsafe { libc::fcntl(fd, cmd, &mut flock) };
        if rc == -1 {
            let error = std::io::Error::last_os_error();
            if blocking && error.kind() == ErrorKind::Interrupted {
                continue;
            }
            if !blocking && error.kind() == ErrorKind::WouldBlock {
                return Ok(false);
            }
            let message = match error.kind() {
                ErrorKind::WouldBlock => {
                    "Failed locking shared WAL coordination file. File is locked by another process"
                        .to_string()
                }
                _ => format!("Failed locking shared WAL coordination file, {error}"),
            };
            return Err(LimboError::LockingError(message));
        }
        return Ok(true);
    }
}

pub(crate) fn unix_shared_wal_unlock_byte(
    fd: RawFd,
    offset: u64,
    kind: SharedWalLockKind,
) -> Result<()> {
    let mut flock = libc::flock {
        l_type: libc::F_UNLCK as libc::c_short,
        l_whence: libc::SEEK_SET as libc::c_short,
        l_start: offset as libc::off_t,
        l_len: 1,
        l_pid: 0,
        #[cfg(target_os = "freebsd")]
        l_sysid: 0,
    };
    let cmd = match kind {
        #[cfg(target_os = "linux")]
        SharedWalLockKind::LinuxOfd => libc::F_OFD_SETLK,
        SharedWalLockKind::ProcessScopedFcntl => libc::F_SETLK,
        #[cfg(not(target_os = "linux"))]
        SharedWalLockKind::LinuxOfd => {
            return Err(LimboError::InternalError(
                "linux OFD locks are not supported on this platform".into(),
            ))
        }
    };
    let rc = unsafe { libc::fcntl(fd, cmd, &mut flock) };
    if rc == -1 {
        Err(LimboError::LockingError(format!(
            "Failed to release shared WAL coordination lock: {}",
            std::io::Error::last_os_error()
        )))
    } else {
        Ok(())
    }
}

pub(crate) fn unix_shared_wal_map(
    offset: u64,
    len: usize,
    fd: RawFd,
) -> Result<Box<dyn SharedWalMappedRegion>> {
    if len == 0 {
        return Err(LimboError::InternalError(
            "cannot mmap shared WAL coordination region with zero length".into(),
        ));
    }
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if page_size <= 0 {
        return Err(LimboError::LockingError(format!(
            "failed to determine shared WAL mmap page size: {}",
            std::io::Error::last_os_error()
        )));
    }
    let page_size = page_size as u64;
    let aligned_offset = offset / page_size * page_size;
    let prefix_len = (offset - aligned_offset) as usize;
    let mapping_len = prefix_len
        .checked_add(len)
        .ok_or_else(|| LimboError::InternalError("shared WAL mmap length overflow".into()))?;
    let mapping_ptr = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            mapping_len,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED,
            fd,
            aligned_offset as libc::off_t,
        )
    };
    if mapping_ptr == libc::MAP_FAILED {
        let error = std::io::Error::last_os_error();
        let file_size = unsafe {
            let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
            if libc::fstat(fd, stat.as_mut_ptr()) == 0 {
                stat.assume_init().st_size
            } else {
                -1
            }
        };
        return Err(LimboError::LockingError(format!(
            "mmap shared WAL coordination file failed: {error} (offset={offset}, aligned_offset={aligned_offset}, len={len}, mapping_len={mapping_len}, fd={fd}, file_size={file_size})"
        )));
    }
    let mapping_ptr =
        NonNull::new(mapping_ptr.cast::<u8>()).expect("mmap returned null for shared WAL map");
    let ptr = NonNull::new(unsafe { mapping_ptr.as_ptr().add(prefix_len) })
        .expect("aligned mmap base plus prefix_len returned null");
    Ok(Box::new(UnixSharedWalMapping {
        mapping_ptr,
        mapping_len,
        ptr,
        len,
    }))
}

impl File for UnixFile {
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        let fd = self.file.as_fd();
        // F_SETLK is a non-blocking lock. The lock will be released when the file is closed
        // or the process exits or after an explicit unlock.
        fs::fcntl_lock(
            fd,
            if exclusive {
                FlockOperation::NonBlockingLockExclusive
            } else {
                FlockOperation::NonBlockingLockShared
            },
        )
        .map_err(|e| {
            let io_error = std::io::Error::from(e);
            let message = match io_error.kind() {
                ErrorKind::WouldBlock => format!(
                    "Failed locking file '{}'. File is locked by another process",
                    self.path
                ),
                _ => format!("Failed locking file '{}', {io_error}", self.path),
            };
            LimboError::LockingError(message)
        })?;

        Ok(())
    }

    fn unlock_file(&self) -> Result<()> {
        let pending = self.pending_sync.lock();
        if let Some(result) = pending.upgrade() {
            let _outcome = result.wait();
        }
        let fd = self.file.as_fd();
        fs::fcntl_lock(fd, FlockOperation::NonBlockingUnlock).map_err(|e| {
            LimboError::LockingError(format!(
                "Failed to release file lock: {}",
                std::io::Error::from(e)
            ))
        })?;
        Ok(())
    }

    #[instrument(err, skip_all, level = Level::TRACE)]
    fn pread(&self, pos: u64, c: Completion) -> Result<Completion> {
        let result = unsafe {
            let r = c.as_read();
            let buf = r.buf();
            let slice = buf.as_mut_slice();
            libc::pread(
                self.file.as_raw_fd(),
                slice.as_mut_ptr() as *mut libc::c_void,
                slice.len(),
                pos as libc::off_t,
            )
        };
        if result == -1 {
            let e = std::io::Error::last_os_error();
            Err(io_error(e, "pread"))
        } else {
            trace!("pread n: {}", result);
            // Read succeeded immediately
            c.complete(result as i32);
            Ok(c)
        }
    }

    #[instrument(err, skip_all, level = Level::TRACE)]
    fn pwrite(&self, pos: u64, buffer: Arc<crate::Buffer>, c: Completion) -> Result<Completion> {
        let buf_slice = buffer.as_slice();
        let total_size = buf_slice.len();

        let mut total_written = 0usize;
        let mut current_pos = pos;

        while total_written < total_size {
            let remaining_slice = &buf_slice[total_written..];
            let write_len = remaining_slice.len().min(MAX_PWRITE_LEN);
            let result = unsafe {
                libc::pwrite(
                    self.file.as_raw_fd(),
                    remaining_slice.as_ptr() as *const libc::c_void,
                    write_len,
                    current_pos as libc::off_t,
                )
            };
            if result == -1 {
                let e = std::io::Error::last_os_error();
                if e.kind() == ErrorKind::Interrupted {
                    // EINTR, retry without advancing
                    continue;
                }
                return Err(io_error(e, "pwrite"));
            }
            let written = result as usize;
            if written == 0 {
                // Unexpected EOF for regular files
                return Err(LimboError::CompletionError(CompletionError::IOError(
                    ErrorKind::UnexpectedEof,
                    "pwrite",
                )));
            }

            total_written += written;
            current_pos += written as u64;
            trace!("pwrite iteration: wrote {written}, total {total_written}/{total_size}");
        }
        trace!("pwrite complete: wrote {total_written} bytes");
        c.complete(total_written as i32);
        Ok(c)
    }

    #[instrument(err, skip_all, level = Level::TRACE)]
    fn pwritev(
        &self,
        pos: u64,
        buffers: Vec<Arc<crate::Buffer>>,
        c: Completion,
    ) -> Result<Completion> {
        if buffers.len().eq(&1) {
            // use `pwrite` for single buffer
            return self.pwrite(pos, buffers[0].clone(), c);
        }

        let total_size: usize = buffers.iter().map(|b| b.as_slice().len()).sum();
        let mut iov: Vec<libc::iovec> = Vec::with_capacity(MAX_IOV);
        let mut buf_idx = 0;
        let mut buf_offset = 0;
        let mut total_written = 0usize;
        let mut current_pos = pos;

        // This loop converts buffers into MAX_IOV iovecs, submits them for I/O, and runs again.
        // If we we run out of iovecs before we convert a buffer in full, we keep track of buffer
        // offset, and resume conversion from there.
        loop {
            while buf_idx < buffers.len() {
                let buf = buffers[buf_idx].as_slice();
                buf_offset += buf_to_iovecs(&buf[buf_offset..], &mut iov, MAX_IOV);
                // If we ran out of iovecs, let's submit them for I/O.
                if buf_offset < buf.len() {
                    break;
                }
                // Buffer was fully conveted to iovec, move to next buffer.
                buf_idx += 1;
                buf_offset = 0;
            }
            if iov.is_empty() {
                break;
            }
            let n = unsafe {
                libc::pwritev(
                    self.file.as_raw_fd(),
                    iov.as_ptr(),
                    iov.len() as i32,
                    current_pos as libc::off_t,
                )
            };
            if n < 0 {
                let e = std::io::Error::last_os_error();
                if e.kind() == ErrorKind::Interrupted {
                    continue;
                }
                return Err(io_error(e, "pwritev"));
            }
            let written = n as usize;
            if written == 0 {
                return Err(LimboError::CompletionError(CompletionError::IOError(
                    ErrorKind::UnexpectedEof,
                    "pwritev",
                )));
            }
            total_written += written;
            current_pos += written as u64;
            trim_iovecs(&mut iov, written);
            trace!("pwritev iteration: wrote {written}, total {total_written}/{total_size}");
        }
        trace!("pwritev complete: wrote {total_written} bytes");
        c.complete(total_written as i32);
        Ok(c)
    }

    #[instrument(err, skip_all, level = Level::TRACE)]
    fn sync(&self, c: Completion, sync_type: FileSyncType) -> Result<Completion> {
        let mut pending = self.pending_sync.lock();
        let queue = self.sync_queue.upgrade().ok_or_else(|| {
            io_error(
                std::io::Error::from(ErrorKind::BrokenPipe),
                "sync IO dropped",
            )
        })?;
        let mut queue = queue.lock();
        let sender = queue.sender.as_ref().ok_or_else(|| {
            io_error(
                std::io::Error::from(ErrorKind::BrokenPipe),
                "sync IO stopped",
            )
        })?;
        let result = Arc::new(SyncResult {
            value: WaitMutex::new(None),
            retired: Condvar::new(),
            queue: self.sync_queue.clone(),
        });
        c.set_io_owner(result.clone());
        let job = SyncJob {
            file: Some(self.file.clone()),
            sync_type,
            result: result.clone(),
            #[cfg(test)]
            hook: self.sync_hook.lock().clone(),
        };
        sender
            .send(job)
            .map_err(|_| io_error(std::io::Error::from(ErrorKind::BrokenPipe), "submit sync"))?;
        *pending = Arc::downgrade(&result);
        queue.pending.push(PendingSync {
            completion: c.clone(),
            result: result.clone(),
        });
        let notification = queue
            .wake_sender
            .as_ref()
            .expect("running sync queue has a wake worker")
            .send(SyncNotification {
                result,
                context: c.progress_context(),
            });
        drop(queue);
        drop(pending);
        notification.unwrap_or_else(|_| panic!("sync wake worker stopped with accepted work"));
        Ok(c)
    }

    #[instrument(err, skip_all, level = Level::TRACE)]
    fn size(&self) -> Result<u64> {
        Ok(self
            .file
            .metadata()
            .map_err(|e| io_error(e, "metadata"))?
            .len())
    }

    #[instrument(err, skip_all, level = Level::DEBUG)]
    fn truncate(&self, len: u64, c: Completion) -> Result<Completion> {
        let result = self.file.set_len(len);
        match result {
            Ok(()) => {
                trace!("file truncated to len=({})", len);
                c.complete(0);
                Ok(c)
            }
            Err(e) => Err(io_error(e, "truncate")),
        }
    }

    fn shared_wal_lock_byte(
        &self,
        offset: u64,
        exclusive: bool,
        kind: SharedWalLockKind,
    ) -> Result<()> {
        unix_shared_wal_lock_byte(self.file.as_raw_fd(), offset, exclusive, true, kind).map(|_| ())
    }

    fn shared_wal_try_lock_byte(
        &self,
        offset: u64,
        exclusive: bool,
        kind: SharedWalLockKind,
    ) -> Result<bool> {
        unix_shared_wal_lock_byte(self.file.as_raw_fd(), offset, exclusive, false, kind)
    }

    fn shared_wal_unlock_byte(&self, offset: u64, kind: SharedWalLockKind) -> Result<()> {
        let pending = self.pending_sync.lock();
        if let Some(result) = pending.upgrade() {
            let _outcome = result.wait();
        }
        unix_shared_wal_unlock_byte(self.file.as_raw_fd(), offset, kind)
    }

    fn shared_wal_set_len(&self, len: u64) -> Result<()> {
        self.file
            .set_len(len)
            .map_err(|err| io_error(err, "resize shared WAL coordination file"))
    }

    fn shared_wal_map(&self, offset: u64, len: usize) -> Result<Box<dyn SharedWalMappedRegion>> {
        unix_shared_wal_map(offset, len, self.file.as_raw_fd())
    }
}

fn sync_file(
    file: &std::fs::File,
    sync_type: FileSyncType,
) -> std::result::Result<(), CompletionError> {
    let result = unsafe {
        #[cfg(target_vendor = "apple")]
        {
            match sync_type {
                FileSyncType::Fsync => libc::fsync(file.as_raw_fd()),
                FileSyncType::FullFsync => libc::fcntl(file.as_raw_fd(), libc::F_FULLFSYNC),
            }
        }
        #[cfg(not(target_vendor = "apple"))]
        {
            let _ = sync_type;
            libc::fsync(file.as_raw_fd())
        }
    };
    if result == -1 {
        Err(CompletionError::IOError(
            std::io::Error::last_os_error().kind(),
            "sync",
        ))
    } else {
        trace!(?sync_type, "sync completed");
        Ok(())
    }
}

/// Append iovec entries for `buf` to `iovecs`, splitting `buf` into chunks of
/// at most `MAX_PWRITE_LEN` bytes. Stops once `iovecs.len()` reaches `max_iovecs`.
/// Returns the number of bytes consumed from `buf`.
fn buf_to_iovecs(buf: &[u8], iovecs: &mut Vec<libc::iovec>, max_iovecs: usize) -> usize {
    let mut slice = buf;
    while !slice.is_empty() && iovecs.len() < max_iovecs {
        let chunk_len = slice.len().min(MAX_PWRITE_LEN);
        iovecs.push(libc::iovec {
            iov_base: slice.as_ptr() as *mut libc::c_void,
            iov_len: chunk_len,
        });
        slice = &slice[chunk_len..];
    }
    buf.len() - slice.len()
}

/// Drop the first `n` bytes from the front of `iov`, advancing the leading
/// entry's pointer if a partial iovec was consumed.
fn trim_iovecs(iov: &mut Vec<libc::iovec>, mut n: usize) {
    let mut idx = 0;
    while idx < iov.len() {
        if iov[idx].iov_len > n {
            break;
        }
        n -= iov[idx].iov_len;
        idx += 1;
    }
    iov.drain(..idx);
    if n > 0 {
        assert!(!iov.is_empty());
        let front = &mut iov[0];
        front.iov_base = unsafe { (front.iov_base as *mut u8).add(n) as *mut libc::c_void };
        front.iov_len -= n;
    }
}

impl Drop for UnixFile {
    fn drop(&mut self) {
        self.unlock_file().expect("Failed to unlock file");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[cfg(not(shuttle))]
    #[test]
    fn sync_submission_and_step_return_before_syscall_finishes() {
        use std::sync::mpsc;
        use std::time::Duration;

        let io = Arc::new(UnixIO::new().unwrap());
        let temp = tempfile::NamedTempFile::new().unwrap();
        let file = io
            .open_file(temp.path().to_str().unwrap(), OpenFlags::None, false)
            .unwrap();
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let release_rx = std::sync::Mutex::new(release_rx);
        *io.sync_hook.lock() = Some(Arc::new(move |_| {
            entered_tx.send(()).unwrap();
            release_rx.lock().unwrap().recv().unwrap();
            Ok(())
        }));
        let (submitted_tx, submitted_rx) = mpsc::channel();
        let (stepped_tx, stepped_rx) = mpsc::channel();
        let submitting_io = io.clone();
        let submitter = std::thread::spawn(move || {
            let c = file
                .sync(
                    Completion::new_sync(|result| assert_eq!(result, Ok(0))),
                    FileSyncType::Fsync,
                )
                .unwrap();
            submitted_tx.send(c).unwrap();
            submitting_io.step().unwrap();
            stepped_tx.send(()).unwrap();
        });
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let submitted = submitted_rx.recv_timeout(Duration::from_secs(5));
        let stepped = submitted
            .as_ref()
            .ok()
            .map(|_| stepped_rx.recv_timeout(Duration::from_secs(5)));
        let pending = submitted.as_ref().is_ok_and(|c| !c.finished());
        release_tx.send(()).unwrap();
        submitter.join().unwrap();
        assert!(
            submitted.is_ok(),
            "sync submission waited for the held syscall"
        );
        assert!(stepped.unwrap().is_ok(), "step waited for the held syscall");
        assert!(pending, "sync completed before the held syscall returned");
        io.wait_for_completion(submitted.unwrap()).unwrap();
    }

    #[test]
    fn test_multiple_processes_cannot_open_file() {
        common::tests::test_multiple_processes_cannot_open_file(UnixIO::new);
    }

    #[cfg(not(shuttle))]
    mod async_sync {
        use super::*;
        use crate::io::CompletionGroup;
        use crate::types::IOCompletions;
        use crate::{Buffer, Database, SqliteDialect, StepResult, Value};
        use std::future::Future;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::task::{Context, Poll, Wake, Waker};
        use std::time::{Duration, Instant};

        #[test]
        fn reviewfix_returning_reset_dispatches_attached_sync() {
            returning_attached_cleanup(false);
        }

        #[test]
        fn reviewfix_returning_drop_dispatches_attached_sync() {
            returning_attached_cleanup(true);
        }

        #[test]
        fn reviewfix_returning_reset_preserves_sync_error_and_rows() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("returning-error.db");
            let io = Arc::new(UnixIO::new().unwrap());
            let db =
                Database::open_file(io.clone(), path.to_str().unwrap(), Arc::new(SqliteDialect))
                    .unwrap();
            let conn = db.connect().unwrap();
            conn.execute("PRAGMA synchronous=FULL; PRAGMA wal_autocheckpoint=0; CREATE TABLE t(x); INSERT INTO t VALUES(1)").unwrap();
            let mut stmt = conn.prepare("INSERT INTO t VALUES(2) RETURNING x").unwrap();
            loop {
                match stmt.step().unwrap() {
                    StepResult::Row => break,
                    StepResult::IO => stmt
                        .take_io_completions()
                        .unwrap()
                        .wait(io.as_ref())
                        .unwrap(),
                    other => panic!("expected RETURNING row, got {other:?}"),
                }
            }
            *io.sync_hook.lock() = Some(Arc::new(|_| {
                Err(CompletionError::IOError(ErrorKind::Other, "reset sync"))
            }));
            let error = stmt.reset().unwrap_err();
            assert!(error.to_string().contains("reset sync"));
            *io.sync_hook.lock() = None;
            drop(stmt);
            assert_eq!(rows(&conn), vec![vec![Value::from_i64(1)]]);
            conn.execute("INSERT INTO t VALUES(3)").unwrap();
            conn.close().unwrap();
            drop(conn);
            drop(db);
            let reopened =
                Database::open_file(io, path.to_str().unwrap(), Arc::new(SqliteDialect)).unwrap();
            assert_eq!(
                rows(&reopened.connect().unwrap()),
                vec![vec![Value::from_i64(1)], vec![Value::from_i64(3)]]
            );
        }

        fn returning_attached_cleanup(drop_statement: bool) {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("returning.db");
            let db = Database::open_file_with_flags(
                Arc::new(crate::MemoryIO::new()),
                ":memory:",
                OpenFlags::default(),
                crate::DatabaseOpts::new().with_attach(true),
                None,
                Arc::new(SqliteDialect),
            )
            .unwrap();
            let conn = db.connect().unwrap();
            conn.execute(format!("PRAGMA temp_store=MEMORY; ATTACH '{}' AS aux; PRAGMA aux.synchronous=FULL; CREATE TABLE aux.t(x); INSERT INTO aux.t VALUES(1)", path.display())).unwrap();
            let mut stmt = conn
                .prepare("INSERT INTO aux.t VALUES(7),(8) RETURNING x")
                .unwrap();
            loop {
                match stmt.step().unwrap() {
                    StepResult::Row => break,
                    StepResult::IO => stmt
                        .take_io_completions()
                        .unwrap()
                        .wait(&crate::MemoryIO::new())
                        .unwrap(),
                    other => panic!("expected RETURNING row, got {other:?}"),
                }
            }
            if drop_statement {
                drop(stmt);
            } else {
                stmt.reset().unwrap();
                drop(stmt);
            }
            conn.execute("INSERT INTO aux.t VALUES(9); DETACH aux")
                .unwrap();
            let reopened = Database::open_file(
                Arc::new(UnixIO::new().unwrap()),
                path.to_str().unwrap(),
                Arc::new(SqliteDialect),
            )
            .unwrap();
            assert_eq!(
                rows(&reopened.connect().unwrap()),
                [1, 7, 8, 9]
                    .into_iter()
                    .map(|x| vec![Value::from_i64(x)])
                    .collect::<Vec<_>>()
            );
        }

        #[test]
        fn reviewfix_ready_callback_waits_for_ready_sibling() {
            let held = HeldSync::new();
            let sibling = Completion::new_sync(|r| assert_eq!(r, Ok(0)));
            let sibling_in_callback = sibling.clone();
            let weak_io = Arc::downgrade(&held.io);
            let first = held
                .file
                .sync(
                    Completion::new_sync(move |r| {
                        assert_eq!(r, Ok(0));
                        weak_io
                            .upgrade()
                            .unwrap()
                            .wait_for_completion(sibling_in_callback.clone())
                            .unwrap();
                    }),
                    FileSyncType::Fsync,
                )
                .unwrap();
            held.entered.recv_timeout(Duration::from_secs(5)).unwrap();
            let sibling = held.file.sync(sibling, FileSyncType::Fsync).unwrap();
            held.release.send(()).unwrap();
            held.release.send(()).unwrap();
            sibling.wait_for_io();
            held.io.step().unwrap();
            assert!(first.succeeded() && sibling.succeeded());
            assert!(held.io.sync_queue.lock().pending.is_empty());
        }

        #[test]
        fn reviewfix_callback_unwind_preserves_ready_sibling() {
            let held = HeldSync::new();
            let first = held
                .file
                .sync(
                    Completion::new_sync(|_| panic!("callback unwind")),
                    FileSyncType::Fsync,
                )
                .unwrap();
            held.entered.recv_timeout(Duration::from_secs(5)).unwrap();
            let sibling = held
                .file
                .sync(
                    Completion::new_sync(|r| assert_eq!(r, Ok(0))),
                    FileSyncType::Fsync,
                )
                .unwrap();
            held.release.send(()).unwrap();
            held.release.send(()).unwrap();
            sibling.wait_for_io();
            assert!(
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| held.io.step())).is_err()
            );
            assert!(!first.finished());
            assert_eq!(held.io.sync_queue.lock().pending.len(), 1);
            held.io.wait_for_completion(sibling.clone()).unwrap();
            assert!(sibling.succeeded());
            assert!(held.io.sync_queue.lock().pending.is_empty());
        }

        #[test]
        fn reviewfix_waker_drops_last_io_with_queued_job() {
            let held = HeldSync::new();
            let worker = Arc::downgrade(&held.io.worker_lifetime);
            let wake_worker = Arc::downgrade(&held.io.wake_lifetime);
            let queue = Arc::downgrade(&held.io.sync_queue);
            let (callbacks, received) = mpsc::channel();
            let first_callback = callbacks.clone();
            let first = held
                .file
                .sync(
                    Completion::new_sync(move |r| {
                        assert_eq!(r, Ok(0));
                        first_callback
                            .send(thread::current().name().unwrap_or("").to_owned())
                            .unwrap();
                    }),
                    FileSyncType::Fsync,
                )
                .unwrap();
            held.entered.recv_timeout(Duration::from_secs(5)).unwrap();
            let second = held
                .file
                .sync(
                    Completion::new_sync(move |r| {
                        assert_eq!(r, Ok(0));
                        callbacks
                            .send(thread::current().name().unwrap_or("").to_owned())
                            .unwrap();
                    }),
                    FileSyncType::Fsync,
                )
                .unwrap();
            let (done, finished) = mpsc::channel();
            let io = held.io;
            first.set_waker(&Waker::from(Arc::new(DropOnWake(Mutex::new(Some(
                Box::new(move || {
                    drop(io);
                    done.send(()).unwrap();
                }),
            ))))));
            held.release.send(()).unwrap();
            held.release.send(()).unwrap();
            finished
                .recv_timeout(Duration::from_secs(5))
                .expect("waker shutdown stalled");
            for _ in 0..2 {
                assert_ne!(
                    received.recv_timeout(Duration::from_secs(5)).unwrap(),
                    "turso-sync"
                );
            }
            assert!(first.succeeded() && second.succeeded());
            assert_eq!(worker.strong_count(), 0);
            assert!(queue.upgrade().is_none());
            let deadline = Instant::now() + Duration::from_secs(5);
            while wake_worker.strong_count() != 0 && Instant::now() < deadline {
                thread::yield_now();
            }
            assert_eq!(wake_worker.strong_count(), 0);
        }

        #[test]
        fn reviewfix_waker_drops_file_with_queued_job() {
            let held = HeldSync::new();
            let first = held
                .file
                .sync(
                    Completion::new_sync(|r| assert_eq!(r, Ok(0))),
                    FileSyncType::Fsync,
                )
                .unwrap();
            held.entered.recv_timeout(Duration::from_secs(5)).unwrap();
            let second = held
                .file
                .sync(
                    Completion::new_sync(|r| assert_eq!(r, Ok(0))),
                    FileSyncType::Fsync,
                )
                .unwrap();
            let (done, finished) = mpsc::channel();
            let file = held.file;
            first.set_waker(&Waker::from(Arc::new(DropOnWake(Mutex::new(Some(
                Box::new(move || {
                    drop(file);
                    done.send(()).unwrap();
                }),
            ))))));
            held.release.send(()).unwrap();
            held.release.send(()).unwrap();
            finished
                .recv_timeout(Duration::from_secs(5))
                .expect("waker file drop stalled");
            held.io.wait_for_completion(first.clone()).unwrap();
            held.io.wait_for_completion(second.clone()).unwrap();
            assert!(first.succeeded() && second.succeeded());
        }

        #[test]
        fn reviewfix_waker_drops_statement_with_queued_sync() {
            let held = HeldSync::new();
            *held.io.sync_hook.lock() = None;
            let path = held.dir.path().join("waker-statement.db");
            let db = Database::open_file(
                held.io.clone(),
                path.to_str().unwrap(),
                Arc::new(SqliteDialect),
            )
            .unwrap();
            let conn = db.connect().unwrap();
            conn.execute("PRAGMA synchronous=FULL; PRAGMA wal_autocheckpoint=0; CREATE TABLE t(x); INSERT INTO t VALUES(1)").unwrap();
            *held.io.sync_hook.lock() = Some(held.hook);
            let first = held
                .file
                .sync(
                    Completion::new_sync(|r| assert_eq!(r, Ok(0))),
                    FileSyncType::Fsync,
                )
                .unwrap();
            held.entered.recv_timeout(Duration::from_secs(5)).unwrap();
            let mut stmt = conn.prepare("INSERT INTO t VALUES(2)").unwrap();
            loop {
                assert!(matches!(stmt.step().unwrap(), StepResult::IO));
                if held.io.sync_queue.lock().pending.len() == 2 {
                    break;
                }
                held.io.step().unwrap();
            }
            let external = stmt.take_io_completions().unwrap();
            let (done, finished) = mpsc::channel();
            first.set_waker(&Waker::from(Arc::new(DropOnWake(Mutex::new(Some(
                Box::new(move || {
                    drop(stmt);
                    done.send(()).unwrap();
                }),
            ))))));
            held.release.send(()).unwrap();
            held.release.send(()).unwrap();
            finished
                .recv_timeout(Duration::from_secs(5))
                .expect("waker statement drop stalled");
            *held.io.sync_hook.lock() = None;
            external.wait(held.io.as_ref()).unwrap();
            held.io.wait_for_completion(first).unwrap();
            assert_eq!(rows(&conn), vec![vec![Value::from_i64(1)]]);
            conn.execute("INSERT INTO t VALUES(3)").unwrap();
        }

        struct DropOnWake(Mutex<Option<Box<dyn FnOnce() + Send>>>);

        impl Wake for DropOnWake {
            fn wake(self: Arc<Self>) {
                let action = self.0.lock().take();
                if let Some(action) = action {
                    action();
                }
            }
        }

        #[test]
        fn sync_types_persist_data_and_dispatch_once_on_driver() {
            let io = UnixIO::new().unwrap();
            let temp = tempfile::NamedTempFile::new().unwrap();
            let file = io
                .open_file(temp.path().to_str().unwrap(), OpenFlags::None, false)
                .unwrap();
            let driver = thread::current().id();
            let count = Arc::new(AtomicUsize::new(0));
            for sync_type in [FileSyncType::Fsync, FileSyncType::FullFsync] {
                let c = file
                    .pwrite(
                        0,
                        Arc::new(Buffer::new(b"durable".to_vec())),
                        Completion::new_write(|_| {}),
                    )
                    .unwrap();
                io.wait_for_completion(c).unwrap();
                let count = count.clone();
                let start = Instant::now();
                let c = file
                    .sync(
                        Completion::new_sync(move |result| {
                            assert_eq!(result, Ok(0));
                            assert_eq!(thread::current().id(), driver);
                            count.fetch_add(1, Ordering::SeqCst);
                        }),
                        sync_type,
                    )
                    .unwrap();
                let submitted = start.elapsed();
                io.wait_for_completion(c).unwrap();
                eprintln!(
                    "sync_type={sync_type:?} submission_ns={} durable_total_ns={}",
                    submitted.as_nanos(),
                    start.elapsed().as_nanos()
                );
                io.step().unwrap();
                assert!(io.sync_queue.lock().pending.is_empty());
                assert_eq!(std::fs::read(temp.path()).unwrap(), b"durable");
            }
            assert_eq!(count.load(Ordering::SeqCst), 2);
        }

        #[test]
        fn real_syscall_error_is_delivered_as_error() {
            let io = UnixIO::new().unwrap();
            let file = io.open_file("/dev/null", OpenFlags::NoLock, false).unwrap();
            let expected = sync_file(
                &std::fs::File::open("/dev/null").unwrap(),
                FileSyncType::FullFsync,
            )
            .unwrap_err();
            let count = Arc::new(AtomicUsize::new(0));
            let called = count.clone();
            let c = file
                .sync(
                    Completion::new_sync(move |result| {
                        assert_eq!(result, Err(expected));
                        called.fetch_add(1, Ordering::SeqCst);
                    }),
                    FileSyncType::FullFsync,
                )
                .unwrap();
            assert!(io.wait_for_completion(c.clone()).is_err());
            assert_eq!(c.get_error(), Some(expected));
            io.step().unwrap();
            assert_eq!(count.load(Ordering::SeqCst), 1);
        }

        #[test]
        fn async_driver_wakes_before_and_after_registration_with_groups() {
            for before_registration in [true, false] {
                let held = HeldSync::new();
                let c = Completion::new_sync(|result| assert_eq!(result, Ok(0)));
                let mut group = CompletionGroup::new(|result| assert_eq!(result, Ok(0)));
                group.add(&c);
                let group = group.build();
                let mut outer = CompletionGroup::new(|result| assert_eq!(result, Ok(0)));
                outer.add(&group);
                let outer = outer.build();
                let c = held.file.sync(c, FileSyncType::Fsync).unwrap();
                held.entered.recv_timeout(Duration::from_secs(5)).unwrap();
                let signal = if before_registration {
                    held.release.send(()).unwrap();
                    held.io.wait_for_syncs(std::slice::from_ref(&c));
                    None
                } else {
                    Some(held.release)
                };
                wait_async(IOCompletions(outer).wait_async(held.io.as_ref()), signal).unwrap();
                assert!(c.succeeded());
                assert!(held.io.sync_queue.lock().pending.is_empty());
            }
        }

        #[test]
        fn aborted_completion_does_not_retire_physical_sync() {
            let held = HeldSync::new();
            let c = held
                .file
                .sync(
                    Completion::new_sync(|result| {
                        assert_eq!(result, Err(CompletionError::Aborted))
                    }),
                    FileSyncType::Fsync,
                )
                .unwrap();
            held.entered.recv_timeout(Duration::from_secs(5)).unwrap();
            c.abort();
            assert!(c.finished());
            let result = held.io.sync_queue.lock().pending[0].result.clone();
            assert!(result.value.lock().unwrap().is_none());
            let (done_tx, done_rx) = mpsc::channel();
            let io = held.io.clone();
            let waiter = thread::spawn(move || {
                io.drain_completions(&[c]).unwrap();
                done_tx.send(()).unwrap();
            });
            let early = done_rx.recv_timeout(Duration::from_millis(100)).is_ok();
            held.release.send(()).unwrap();
            waiter.join().unwrap();
            assert!(!early, "logical abort bypassed physical retirement");
            assert!(result.value.lock().unwrap().is_some());
            assert!(held.io.sync_queue.lock().pending.is_empty());
        }

        #[test]
        fn owner_nested_async_wait_uses_origin_before_and_after_group_registration() {
            for late in [false, true] {
                let held = HeldSync::new();
                let c = Completion::new_sync(|r| assert_eq!(r, Ok(0)));
                let early = (!late).then(|| nested_group(&c));
                let _submitted = held.file.sync(c.clone(), FileSyncType::Fsync).unwrap();
                held.entered.recv_timeout(Duration::from_secs(5)).unwrap();
                let group = early.unwrap_or_else(|| nested_group(&c));
                let memory = crate::MemoryIO::new();
                wait_async(IOCompletions(group).wait_async(&memory), Some(held.release)).unwrap();
                assert!(c.succeeded());
                assert!(held.io.sync_queue.lock().pending.is_empty());
            }
        }

        #[test]
        fn owner_nested_blocking_wait_uses_origin() {
            let held = HeldSync::new();
            let c = held
                .file
                .sync(Completion::new_sync(|_| {}), FileSyncType::Fsync)
                .unwrap();
            held.entered.recv_timeout(Duration::from_secs(5)).unwrap();
            let group = nested_group(&c);
            let (tx, rx) = mpsc::channel();
            let waiter = thread::spawn(move || {
                IOCompletions(group).wait(&crate::MemoryIO::new()).unwrap();
                tx.send(()).unwrap();
            });
            held.release.send(()).unwrap();
            let completed = rx.recv_timeout(Duration::from_secs(2)).is_ok();
            held.io.wait_for_completion(c).unwrap();
            waiter.join().unwrap();
            assert!(completed, "exported group did not drive its originating IO");
        }

        #[test]
        fn owner_memory_drain_waits_for_aborted_physical_sync() {
            let held = HeldSync::new();
            let c = held
                .file
                .sync(Completion::new_sync(|_| {}), FileSyncType::Fsync)
                .unwrap();
            held.entered.recv_timeout(Duration::from_secs(5)).unwrap();
            let group = nested_group(&c);
            c.abort();
            let (tx, rx) = mpsc::channel();
            let waiter = thread::spawn(move || {
                crate::MemoryIO::new().drain_completions(&[group]).unwrap();
                tx.send(()).unwrap();
            });
            let early = rx.recv_timeout(Duration::from_millis(100)).is_ok();
            held.release.send(()).unwrap();
            waiter.join().unwrap();
            held.io.drain_completions(&[c]).unwrap();
            assert!(!early, "main IO drain ignored the origin's physical work");
        }

        #[test]
        fn owner_late_group_after_physical_result_releases_callback_captures() {
            let held = HeldSync::new();
            let token = Arc::new(());
            let lifetime = Arc::downgrade(&token);
            let c = held
                .file
                .sync(
                    Completion::new_sync(move |_| {
                        assert_eq!(Arc::strong_count(&token), 1);
                    }),
                    FileSyncType::Fsync,
                )
                .unwrap();
            held.entered.recv_timeout(Duration::from_secs(5)).unwrap();
            held.release.send(()).unwrap();
            held.io.wait_for_syncs(std::slice::from_ref(&c));
            let group_token = Arc::new(());
            let group_lifetime = Arc::downgrade(&group_token);
            let mut group = CompletionGroup::new(move |_| {
                let _ = &group_token;
            });
            group.add(&c);
            let group = group.build();
            let outer = nested_group(&group);
            wait_async(
                IOCompletions(outer).wait_async(&crate::MemoryIO::new()),
                None,
            )
            .unwrap();
            assert!(held.io.sync_queue.lock().pending.is_empty());
            drop(group);
            drop(c);
            assert!(lifetime.upgrade().is_none());
            assert!(group_lifetime.upgrade().is_none());
        }

        #[test]
        fn owner_cancel_nested_group_waits_and_dispatches_aborted_once() {
            let held = HeldSync::new();
            let count = Arc::new(AtomicUsize::new(0));
            let called = count.clone();
            let c = held
                .file
                .sync(
                    Completion::new_sync(move |result| {
                        assert_eq!(result, Err(CompletionError::Aborted));
                        called.fetch_add(1, Ordering::SeqCst);
                    }),
                    FileSyncType::Fsync,
                )
                .unwrap();
            held.entered.recv_timeout(Duration::from_secs(5)).unwrap();
            let group = nested_group(&c);
            let (tx, rx) = mpsc::channel();
            let waiter = thread::spawn(move || {
                let memory = crate::MemoryIO::new();
                memory.cancel(std::slice::from_ref(&group)).unwrap();
                memory
                    .drain_completions(std::slice::from_ref(&group))
                    .unwrap();
                assert_eq!(group.get_error(), Some(CompletionError::Aborted));
                tx.send(()).unwrap();
            });
            let early = rx.recv_timeout(Duration::from_millis(100)).is_ok();
            held.release.send(()).unwrap();
            waiter.join().unwrap();
            assert!(!early);
            assert_eq!(count.load(Ordering::SeqCst), 1);
            assert!(held.io.sync_queue.lock().pending.is_empty());
        }

        #[test]
        fn owner_wait_does_not_wait_for_unrelated_sync() {
            let held = HeldSync::new();
            let first = held
                .file
                .sync(Completion::new_sync(|_| {}), FileSyncType::Fsync)
                .unwrap();
            held.entered.recv_timeout(Duration::from_secs(5)).unwrap();
            held.release.send(()).unwrap();
            held.io.wait_for_syncs(std::slice::from_ref(&first));
            let second = held
                .file
                .sync(Completion::new_sync(|_| {}), FileSyncType::Fsync)
                .unwrap();
            held.entered.recv_timeout(Duration::from_secs(5)).unwrap();
            let (tx, rx) = mpsc::channel();
            let waiter = thread::spawn(move || {
                IOCompletions(first).wait(&crate::MemoryIO::new()).unwrap();
                tx.send(()).unwrap();
            });
            let independent = rx.recv_timeout(Duration::from_secs(2)).is_ok();
            held.release.send(()).unwrap();
            held.io.wait_for_completion(second).unwrap();
            waiter.join().unwrap();
            assert!(independent, "wait included an unrelated held syscall");
        }

        fn nested_group(c: &Completion) -> Completion {
            let mut inner = CompletionGroup::new(|_| {});
            inner.add(c);
            let inner = inner.build();
            let mut outer = CompletionGroup::new(|_| {});
            outer.add(&inner);
            outer.build()
        }

        #[test]
        fn shutdown_drains_accepted_work_and_releases_queue() {
            let held = HeldSync::new();
            let worker = Arc::downgrade(&held.io.worker_lifetime);
            let c = held
                .file
                .sync(
                    Completion::new_sync(|result| assert_eq!(result, Ok(0))),
                    FileSyncType::Fsync,
                )
                .unwrap();
            held.entered.recv_timeout(Duration::from_secs(5)).unwrap();
            let queue = Arc::downgrade(&held.io.sync_queue);
            let (done_tx, done_rx) = mpsc::channel();
            let shutdown = thread::spawn(move || {
                drop(held.io);
                done_tx.send(()).unwrap();
            });
            let early = done_rx.recv_timeout(Duration::from_millis(100)).is_ok();
            held.release.send(()).unwrap();
            shutdown.join().unwrap();
            assert!(!early, "shutdown returned with a live syscall");
            assert!(c.succeeded());
            assert!(queue.upgrade().is_none());
            assert_eq!(worker.strong_count(), 0);
            assert!(held
                .file
                .sync(
                    Completion::new_sync(|_| panic!("rejected submission ran callback")),
                    FileSyncType::Fsync
                )
                .is_err());
        }

        #[test]
        fn callback_can_submit_another_sync_and_reenter_step() {
            let io = Arc::new(UnixIO::new().unwrap());
            let temp = tempfile::NamedTempFile::new().unwrap();
            let file = io
                .open_file(temp.path().to_str().unwrap(), OpenFlags::None, false)
                .unwrap();
            let weak_io = Arc::downgrade(&io);
            let captured_file = file.clone();
            let (tx, rx) = mpsc::channel();
            let c = file
                .sync(
                    Completion::new_sync(move |result| {
                        assert_eq!(result, Ok(0));
                        let next = captured_file
                            .sync(
                                Completion::new_sync(|result| assert_eq!(result, Ok(0))),
                                FileSyncType::Fsync,
                            )
                            .unwrap();
                        weak_io.upgrade().unwrap().step().unwrap();
                        tx.send(next).unwrap();
                    }),
                    FileSyncType::Fsync,
                )
                .unwrap();
            io.wait_for_completion(c).unwrap();
            io.wait_for_completion(rx.recv_timeout(Duration::from_secs(5)).unwrap())
                .unwrap();
            assert!(io.sync_queue.lock().pending.is_empty());
            let queue = Arc::downgrade(&io.sync_queue);
            drop(file);
            drop(io);
            assert!(queue.upgrade().is_none());
        }

        #[test]
        fn failed_sync_preserves_prior_rows_and_connection_usability() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("error.db");
            let io = Arc::new(UnixIO::new().unwrap());
            let db =
                Database::open_file(io.clone(), path.to_str().unwrap(), Arc::new(SqliteDialect))
                    .unwrap();
            let conn = db.connect().unwrap();
            conn.execute("PRAGMA synchronous=FULL; PRAGMA wal_autocheckpoint=0; CREATE TABLE t(x); INSERT INTO t VALUES(1)").unwrap();
            *io.sync_hook.lock() = Some(Arc::new(|_| {
                Err(CompletionError::IOError(ErrorKind::Other, "sync"))
            }));
            let error = conn.execute("INSERT INTO t VALUES(2)").unwrap_err();
            assert!(error.to_string().contains("sync"));
            *io.sync_hook.lock() = None;
            assert_eq!(rows(&conn), vec![vec![Value::from_i64(1)]]);
            conn.execute("INSERT INTO t VALUES(3)").unwrap();
            conn.close().unwrap();
            drop(conn);
            drop(db);
            let reopened =
                Database::open_file(io, path.to_str().unwrap(), Arc::new(SqliteDialect)).unwrap();
            assert_eq!(
                rows(&reopened.connect().unwrap()),
                vec![vec![Value::from_i64(1)], vec![Value::from_i64(3)]]
            );
        }

        #[test]
        fn dropping_statement_with_taken_completion_keeps_writer_until_sync_retires() {
            let held = HeldSync::new();
            *held.io.sync_hook.lock() = None;
            let path = held.dir.path().join("statement.db");
            let db = Database::open_file(
                held.io.clone(),
                path.to_str().unwrap(),
                Arc::new(SqliteDialect),
            )
            .unwrap();
            let conn = db.connect().unwrap();
            conn.execute("PRAGMA synchronous=FULL; PRAGMA wal_autocheckpoint=0; CREATE TABLE t(x); INSERT INTO t VALUES(1)").unwrap();
            *held.io.sync_hook.lock() = Some(held.hook);
            let mut stmt = conn.prepare("INSERT INTO t VALUES(2)").unwrap();
            loop {
                assert!(matches!(stmt.step().unwrap(), StepResult::IO));
                if !held.io.sync_queue.lock().pending.is_empty() {
                    break;
                }
                held.io.step().unwrap();
            }
            held.entered.recv_timeout(Duration::from_secs(5)).unwrap();
            let external = stmt.take_io_completions().unwrap();
            let (done_tx, done_rx) = mpsc::channel();
            let dropper = thread::spawn(move || {
                drop(stmt);
                done_tx.send(()).unwrap();
            });
            let early = done_rx.recv_timeout(Duration::from_millis(100)).is_ok();
            held.release.send(()).unwrap();
            dropper.join().unwrap();
            *held.io.sync_hook.lock() = None;
            external.wait(held.io.as_ref()).unwrap();
            assert!(
                !early,
                "statement released its transaction before the taken sync retired"
            );
            assert_eq!(rows(&conn), vec![vec![Value::from_i64(1)]]);
            conn.execute("INSERT INTO t VALUES(3)").unwrap();
        }

        #[test]
        fn dropping_file_retains_process_lock_until_sync_retires() {
            let held = HeldSync::new();
            let c = held
                .file
                .sync(
                    Completion::new_sync(|result| assert_eq!(result, Ok(0))),
                    FileSyncType::Fsync,
                )
                .unwrap();
            held.entered.recv_timeout(Duration::from_secs(5)).unwrap();
            let (started_tx, started_rx) = mpsc::channel();
            let dropper = thread::spawn(move || {
                started_tx.send(()).unwrap();
                drop(held.file);
            });
            started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            let path = held.dir.path().join("sync.db");
            let retained = lock_probe(&path, false);
            held.release.send(()).unwrap();
            dropper.join().unwrap();
            assert!(retained, "file lock was released during the held syscall");
            assert!(lock_probe(&path, true));
            held.io.wait_for_completion(c).unwrap();
            assert!(held.io.sync_queue.lock().pending.is_empty());
        }

        #[test]
        fn lock_probe_child() {
            let Ok(path) = std::env::var("TURSO_SYNC_LOCK_PROBE") else {
                return;
            };
            let expect_open = std::env::var("TURSO_SYNC_LOCK_AVAILABLE").unwrap() == "true";
            let io = UnixIO::new().unwrap();
            assert_eq!(
                io.open_file(&path, OpenFlags::None, false).is_ok(),
                expect_open
            );
        }

        fn lock_probe(path: &std::path::Path, expect_open: bool) -> bool {
            std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "io::unix::tests::async_sync::lock_probe_child",
                    "--nocapture",
                ])
                .env("TURSO_SYNC_LOCK_PROBE", path)
                .env("TURSO_SYNC_LOCK_AVAILABLE", expect_open.to_string())
                .output()
                .unwrap()
                .status
                .success()
        }

        fn rows(conn: &Arc<crate::Connection>) -> Vec<Vec<Value>> {
            let mut rows = Vec::new();
            conn.prepare("SELECT x FROM t ORDER BY x")
                .unwrap()
                .run_with_row_callback(|row| {
                    rows.push(row.get_values().cloned().collect());
                    Ok(())
                })
                .unwrap();
            rows
        }

        fn wait_async(
            future: impl Future<Output = Result<()>>,
            release: Option<mpsc::Sender<()>>,
        ) -> Result<()> {
            let (tx, rx) = mpsc::channel();
            let waker = Waker::from(Arc::new(Signal(tx)));
            let mut context = Context::from_waker(&waker);
            let mut future = Box::pin(future);
            let mut release = release;
            loop {
                match future.as_mut().poll(&mut context) {
                    Poll::Ready(result) => return result,
                    Poll::Pending => {
                        if let Some(release) = release.take() {
                            release.send(()).unwrap();
                        }
                        rx.recv_timeout(Duration::from_secs(5))
                            .expect("async driver was not woken");
                    }
                }
            }
        }

        struct Signal(mpsc::Sender<()>);

        impl Wake for Signal {
            fn wake(self: Arc<Self>) {
                let _ = self.0.send(());
            }
        }

        struct HeldSync {
            io: Arc<UnixIO>,
            file: Arc<dyn File>,
            dir: tempfile::TempDir,
            entered: mpsc::Receiver<()>,
            release: mpsc::Sender<()>,
            hook: SyncHook,
        }

        impl HeldSync {
            fn new() -> Self {
                let io = Arc::new(UnixIO::new().unwrap());
                let dir = tempfile::tempdir().unwrap();
                let file = io
                    .open_file(
                        dir.path().join("sync.db").to_str().unwrap(),
                        OpenFlags::Create,
                        false,
                    )
                    .unwrap();
                let (entered_tx, entered) = mpsc::channel();
                let (release, release_rx) = mpsc::channel();
                let release_rx = std::sync::Mutex::new(release_rx);
                let hook: SyncHook = Arc::new(move |_| {
                    entered_tx.send(()).unwrap();
                    release_rx.lock().unwrap().recv().unwrap();
                    Ok(())
                });
                *io.sync_hook.lock() = Some(hook.clone());
                Self {
                    io,
                    file,
                    dir,
                    entered,
                    release,
                    hook,
                }
            }
        }
    }

    #[test]
    fn test_shared_wal_map_supports_unaligned_logical_offset() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let backing_len = 128 * 1024;
        let bytes: Vec<u8> = (0..backing_len).map(|i| (i % 251) as u8).collect();
        file.as_file().write_all(&bytes).unwrap();
        file.as_file().sync_all().unwrap();

        let mapped = unix_shared_wal_map(4096, 81920, file.as_file().as_raw_fd()).unwrap();
        assert_eq!(mapped.len(), 81920);
        let slice = unsafe { std::slice::from_raw_parts(mapped.ptr().as_ptr(), mapped.len()) };
        assert_eq!(&slice[..128], &bytes[4096..4096 + 128]);
        assert_eq!(
            &slice[mapped.len() - 128..],
            &bytes[4096 + 81920 - 128..4096 + 81920]
        );
    }
}
