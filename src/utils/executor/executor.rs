//! The executor for libamac.

use std::future::Future;
use std::pin::Pin;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

const EXECUTOR_NUM_SLOTS: usize = 8;

type Task = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// The coroutine executor tailored for asynchronous memory access (AMAC).
pub struct Executor {
    // The number of active tasks.
    active_task_count: usize,
    // The task slots, of which (0..active_task_count) have active tasks.
    slots: [Option<Task>; EXECUTOR_NUM_SLOTS],
    // The task receiver.
    rx: Receiver<Task>,
}

impl Executor {
    /// Create a new AMAC executor and its associated task spawner.
    pub fn new() -> (Self, Spawner) {
        const EMPTY_TASK: Option<Task> = None;

        let (tx, rx) = channel();
        (
            Executor {
                active_task_count: 0,
                slots: [EMPTY_TASK; EXECUTOR_NUM_SLOTS],
                rx,
            },
            Spawner { tx },
        )
    }

    fn push_task(&mut self, task: Task) {
        self.slots[self.active_task_count] = Some(task);
        self.active_task_count += 1;
    }

    unsafe fn refill_task(&mut self, slot_id: usize) {
        self.active_task_count -= 1;
        let _ = self.slots.get_unchecked_mut(slot_id).take();
        self.slots.swap(slot_id, self.active_task_count);
    }

    fn receive_tasks(&mut self) -> bool {
        while self.active_task_count < EXECUTOR_NUM_SLOTS {
            if self.active_task_count == 0 {
                match self.rx.recv() {
                    Ok(task) => self.push_task(task),
                    Err(_e) => {
                        return false;
                    }
                }
            } else if let Ok(task) = self.rx.try_recv() {
                self.push_task(task);
            } else {
                break;
            }
        }
        true
    }

    /// Run the executor in the current thread. This will block the current thread, until the paired spanwer of the
    /// executor is dropped, and all remaining tasks are completed.
    pub fn run(mut self) {
        let mut waker = dummy_waker();
        let mut cx = Context::from_waker(&mut waker);

        while self.receive_tasks() {
            for slot_id in 0..EXECUTOR_NUM_SLOTS {
                if let Some(task) = unsafe { self.slots.get_unchecked_mut(slot_id) } {
                    if let Poll::Ready(_result) = task.as_mut().poll(&mut cx) {
                        unsafe { self.refill_task(slot_id) };
                    }
                } else {
                    break;
                }
            }
        }
    }
}

// Yield the execution of the current task to the executor.
pub fn yield_now() -> impl Future<Output = ()> + Send + Sync + 'static {
    #[derive(Default)]
    struct YieldNow {
        polled: bool,
    }

    impl Future for YieldNow {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
            if self.polled {
                Poll::Ready(())
            } else {
                self.polled = true;
                Poll::Pending
            }
        }
    }

    YieldNow::default()
}

/// The handle to push tasks to an executor.
pub struct Spawner {
    tx: Sender<Task>,
}

impl Spawner {
    /// Push a task to the executor.
    pub fn spawn<F: Future<Output = ()> + Send + 'static>(&self, fut: F) {
        self.tx
            .send(Box::pin(fut))
            .expect("the executor has been dropped")
    }
}

fn dummy_raw_waker() -> RawWaker {
    fn no_op(_: *const ()) {}
    fn clone(_: *const ()) -> RawWaker {
        dummy_raw_waker()
    }
    let vtable = &RawWakerVTable::new(clone, no_op, no_op, no_op);
    RawWaker::new(0 as *const (), vtable)
}

fn dummy_waker() -> Waker {
    unsafe { Waker::from_raw(dummy_raw_waker()) }
}
