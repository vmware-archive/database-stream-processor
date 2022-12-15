//! Exchange operators implement a N-to-N communication pattern where
//! each participant sends exactly one value to and receives exactly one
//! value from each peer at every clock cycle.

// TODO: We may want to generalize these operators to implement N-to-M
// communication, including 1-to-N and N-to-1.

use crate::{
    circuit::{
        metadata::OperatorLocation,
        operator_traits::{Operator, SinkOperator, SourceOperator},
        OwnershipPreference, Runtime, Scope,
    },
    circuit_cache_key, Circuit,
};
use arc_swap::ArcSwap;
use crossbeam::atomic::AtomicConsume;
use crossbeam_utils::CachePadded;
use std::{
    borrow::Cow,
    cell::UnsafeCell,
    marker::PhantomData,
    mem::MaybeUninit,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

// We use the `Runtime::local_store` mechanism to connect multiple workers
// to an `Exchange` instance.  During circuit construction each, worker
// allocates a unique id that happens to be the same across all workers.
// The worker then allocates a new `Exchange` and adds it to the local store
// using the id as a key.  If there already is an `Exchange` with this id in
// the store, created by another worker, a reference to that `Exchange` will
// be used instead.
circuit_cache_key!(local ExchangeId<T>(usize => Arc<Exchange<T>>));

type NotifyCallback = dyn Fn() + Send + Sync + 'static;

/// `Exchange` is an N-to-N communication primitive that partitions data across
/// multiple concurrent threads.
///
/// An instance of `Exchange` can be shared by multiple threads that communicate
/// in rounds.  In each round each peer _first_ sends exactly one data value to
/// every other peer (and itself) and then receives one value from each peer.
/// The send operation can only proceed when all peers have retrieved data
/// produced at the previous round.  Likewise, the receive operation can proceed
/// once all incoming values are ready for the current round.
pub(crate) struct Exchange<T> {
    /// Contains `n` notify callbacks, one for each worker. The first callback
    /// for any given worker is for that worker's receiver, the second is for
    /// its sender
    notify: Box<[[ArcSwap<Box<NotifyCallback>>; 2]]>,
    /// Contains `n^2` booleans, one for each value
    is_valid: Box<[CachePadded<AtomicBool>]>,
    /// Contains `n^2` slots, one for each send/recv pair
    values: Box<[CachePadded<UnsafeCell<MaybeUninit<T>>>]>,
}

impl<T> Exchange<T>
where
    T: Send + 'static,
{
    /// Create a new exchange operator for `threads` communicating threads.
    fn new(threads: usize) -> Self {
        fn noop_notify() {
            if cfg!(debug_assertions) {
                panic!("a notification callback was never set on an exchange node");
            }
        }

        debug_assert_ne!(threads, 0);

        let notify = (0..threads)
            .map(|_| {
                [
                    ArcSwap::new(Arc::new(Box::new(noop_notify) as Box<NotifyCallback>)),
                    ArcSwap::new(Arc::new(Box::new(noop_notify) as Box<NotifyCallback>)),
                ]
            })
            .collect();

        let slots = threads * threads;

        let is_valid = (0..slots)
            .map(|_| CachePadded::new(AtomicBool::new(false)))
            .collect();

        let mut values = Vec::with_capacity(slots);
        // Safety: `CachePadded<MaybeUninit<T>>` is valid to initialize as uninit
        #[allow(clippy::uninit_vec)]
        unsafe {
            values.set_len(slots);
        }

        Self {
            notify,
            is_valid,
            values: values.into_boxed_slice(),
        }
    }

    /// Create a new `Exchange` instance if an instance with the same id
    /// (created by another thread) does not yet exist within `runtime`.
    /// The number of peers will be set to `runtime.num_workers()`.
    pub(crate) fn with_runtime(runtime: &Runtime, exchange_id: usize) -> Arc<Self> {
        runtime
            .local_store()
            .entry(ExchangeId::new(exchange_id))
            .or_insert_with(|| Arc::new(Exchange::new(runtime.num_workers())))
            .value()
            .clone()
    }

    #[inline]
    fn workers(&self) -> usize {
        self.notify.len()
    }

    #[inline]
    fn receiver_callback(&self, receiver: usize) -> &ArcSwap<Box<NotifyCallback>> {
        &self.notify[receiver][0]
    }

    #[inline]
    fn sender_callback(&self, sender: usize) -> &ArcSwap<Box<NotifyCallback>> {
        &self.notify[sender][1]
    }

    #[inline]
    fn slot_index(&self, sender: usize, receiver: usize) -> usize {
        debug_assert!(sender < self.workers());
        debug_assert!(receiver < self.workers());

        debug_assert!(
            sender * self.workers() + receiver < self.is_valid.len(),
            "sender: {sender}, receiver: {receiver}",
        );
        sender * self.workers() + receiver
    }

    fn ready_to_send(&self, sender: usize) -> bool {
        debug_assert!(sender < self.workers());

        (0..self.workers())
            .all(|receiver| !self.is_valid[self.slot_index(sender, receiver)].load_consume())
    }

    fn ready_to_receive(&self, receiver: usize) -> bool {
        debug_assert!(receiver < self.workers());

        (0..self.workers())
            .all(|sender| self.is_valid[self.slot_index(sender, receiver)].load_consume())
    }

    /// Returns a reference to a mailbox for the sender/receiver pair.
    unsafe fn push(&self, sender: usize, receiver: usize, value: T) {
        let slot = self.slot_index(sender, receiver);

        if cfg!(debug_assertions) {
            // There shouldn't be any value stored within the channel when we're pushing
            let currently_filled = self.is_valid[slot].load_consume();
            assert!(!currently_filled);
        }

        unsafe {
            // Write the value to the slot
            self.values
                .get_unchecked(slot)
                .get()
                .write(MaybeUninit::new(value));

            // Mark the slot as valid
            self.is_valid
                .get_unchecked(slot)
                .store(true, Ordering::Release);
        }

        // Notify the receiver
        (self.receiver_callback(receiver).load())();
    }

    unsafe fn pop(&self, sender: usize, receiver: usize) -> T {
        let slot = self.slot_index(sender, receiver);

        unsafe {
            let slot_is_valid = self.is_valid.get_unchecked(slot);

            // Load the value currently stored in the channel (and synchronize against
            // previous writes)
            let is_valid = slot_is_valid.load_consume();
            debug_assert!(is_valid);

            // Read the value from the channel
            let value = (*self.values.get_unchecked(slot).get()).assume_init_read();

            // Set the slot to be invalid
            slot_is_valid.store(false, Ordering::Relaxed);

            // Notify the sender
            (self.sender_callback(sender).load())();

            value
        }
    }

    /// Write all outgoing messages for `sender` to mailboxes.
    ///
    /// Values to be sent are retrieved from the `data` iterator, with the
    /// first value delivered to receiver 0, second value delivered to receiver
    /// 1, and so on.
    ///
    /// # Errors
    ///
    /// Fails if at least one of the sender's outgoing mailboxes is not empty.
    ///
    /// # Panics
    ///
    /// Panics if `data` yields fewer than `self.npeers` items.
    pub(crate) fn try_send<I>(&self, sender: usize, data: &mut I) -> bool
    where
        I: Iterator<Item = T>,
    {
        if !self.ready_to_send(sender) {
            return false;
        }

        for receiver in 0..self.workers() {
            let data = data.next().unwrap();
            unsafe { self.push(sender, receiver, data) };
        }

        true
    }

    pub(crate) fn try_broadcast(&self, sender: usize, data: T) -> bool
    where
        T: Clone,
    {
        if !self.ready_to_send(sender) {
            return false;
        }

        for receiver in 0..self.workers() {
            unsafe { self.push(sender, receiver, data.clone()) };
        }

        true
    }

    /// Read all incoming messages for `receiver`.
    ///
    /// Values are passed to callback function `cb`.
    ///
    /// # Errors
    ///
    /// Fails if at least one of the receiver's incoming mailboxes is empty.
    pub(crate) fn try_receive<F>(&self, receiver: usize, mut callback: F) -> bool
    where
        F: FnMut(T),
    {
        if !self.ready_to_receive(receiver) {
            return false;
        }

        for sender in 0..self.workers() {
            let data = unsafe { self.pop(sender, receiver) };
            callback(data);
        }

        true
    }

    /// Register callback to be invoked whenever the `ready_to_send` condition
    /// becomes true.
    ///
    /// The callback can be setup at most once (e.g., when a scheduler attaches
    /// to the circuit) and cannot be unregistered.  Notifications delivered
    /// before the callback is registered are lost.  The client should call
    /// `ready_to_send` after installing the callback to check the status.
    ///
    /// After the callback has been registered, notifications are delivered with
    /// at-least-once semantics: a notification is generated whenever the
    /// status changes from not ready to ready, but spurious notifications
    /// can occur occasionally.  Therefore, the user must check the status
    /// explicitly by calling `ready_to_send` or be prepared that `try_send_all`
    /// can fail.
    pub(crate) fn register_sender_callback<F>(&self, sender: usize, callback: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.sender_callback(sender)
            .store(Arc::new(Box::new(callback)));
    }

    /// Register callback to be invoked whenever the `ready_to_receive`
    /// condition becomes true.
    ///
    /// The callback can be setup at most once (e.g., when a scheduler attaches
    /// to the circuit) and cannot be unregistered.  Notifications delivered
    /// before the callback is registered are lost.  The client should call
    /// `ready_to_receive` after installing the callback to check
    /// the status.
    ///
    /// After the callback has been registered, notifications are delivered with
    /// at-least-once semantics: a notification is generated whenever the
    /// status changes from not ready to ready, but spurious notifications
    /// can occur occasionally.  The user must check the status explicitly
    /// by calling `ready_to_receive` or be prepared that `try_receive_all`
    /// can fail.
    pub(crate) fn register_receiver_callback<F>(&self, receiver: usize, callback: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.receiver_callback(receiver)
            .store(Arc::new(Box::new(callback)));
    }
}

unsafe impl<T: Send> Send for Exchange<T> {}
unsafe impl<T: Send> Sync for Exchange<T> {}

/// Operator that partitions incoming data across all workers.
///
/// This operator works in tandem with [`ExchangeReceiver`], which reassembles
/// the data on the receiving side.  Together they implement an all-to-all
/// comunication mechanism, where at every clock cycle each worker partitions
/// its incoming data into `N` values, one for each worker, using a
/// user-provided closure.  It then reads values sent to it by all peers and
/// reassembles them into a single value using another user-provided closure.
///
/// The exchange mechanism is split into two operators, so that after sending
/// the data the circuit does not need to block waiting for its peers to finish
/// sending and can instead schedule other operators.
///
/// ```text
///                    ExchangeSender  ExchangeReceiver
///                       ┌───────┐      ┌───────┐
///                       │       │      │       │
///        ┌───────┐      │       │      │       │          ┌───────┐
///        │source ├─────►│       │      │       ├─────────►│ sink  │
///        └───────┘      │       │      │       │          └───────┘
///                       │       ├───┬─►│       │
///                       │       │   │  │       │
///                       └───────┘   │  └───────┘
/// WORKER 1                          │
/// ──────────────────────────────────┼──────────────────────────────
/// WORKER 2                          │
///                                   │
///                       ┌───────┐   │  ┌───────┐
///                       │       ├───┴─►│       │
///        ┌───────┐      │       │      │       │          ┌───────┐
///        │source ├─────►│       │      │       ├─────────►│ sink  │
///        └───────┘      │       │      │       │          └───────┘
///                       │       │      │       │
///                       │       │      │       │
///                       └───────┘      └───────┘
///                    ExchangeSender  ExchangeReceiver
/// ```
///
/// `ExchangeSender` is an asynchronous operator., i.e.,
/// [`ExchangeSender::is_async`] returns `true`.  It becomes schedulable
/// ([`ExchangeSender::ready`] returns `true`) once all peers have retrieved
/// values written by the operator in the previous clock cycle.  The scheduler
/// should use [`ExchangeSender::register_ready_callback`] to get notified when
/// the operator becomes schedulable.
///
/// `ExchangeSender` doesn't have a public constructor and must be instantiated
/// using the [`Circuit::new_exchange_operators`] function, which creates an
/// [`ExchangeSender`]/[`ExchangeReceiver`] pair of operators and connects them
/// to their counterparts in other workers as in the diagram above.
///
/// An [`ExchangeSender`]/[`ExchangeReceiver`] pair is added to a circuit using
/// the [`Circuit::add_exchange`](`crate::circuit::Circuit::add_exchange`)
/// method, which registers a dependency between them, making sure that
/// `ExchangeSender` is evaluated before `ExchangeReceiver`.
///
/// # Examples
///
/// The following example instantiates the circuit in the diagram above.
///
/// ```
/// # #[cfg(miri)]
/// # fn main() {}
///
/// # #[cfg(not(miri))]
/// # fn main() {
/// use dbsp::{operator::Generator, Circuit, Runtime};
///
/// const WORKERS: usize = 16;
/// const ROUNDS: usize = 10;
///
/// let hruntime = Runtime::run(WORKERS, || {
///     let circuit = Circuit::build(|circuit| {
///         // Create a data source that generates numbers 0, 1, 2, ...
///         let mut n: usize = 0;
///         let source = circuit.add_source(Generator::new(move || {
///             let result = n;
///             n += 1;
///             result
///         }));
///
///         // Create an `ExchangeSender`/`ExchangeReceiver pair`.
///         let (sender, receiver) = circuit.new_exchange_operators(
///             &Runtime::runtime().unwrap(),
///             Runtime::worker_index(),
///             None,
///             // Partitioning function sends a copy of the input `n` to each peer.
///             |n, output| {
///                 for _ in 0..WORKERS {
///                     output.push(n)
///                 }
///             },
///             // Reassemble received values into a vector.
///             |v: &mut Vec<usize>, n| v.push(n),
///         );
///
///         // Add exchange operators to the circuit.
///         let combined = circuit.add_exchange(sender, receiver, &source);
///         let mut round = 0;
///
///         // Expected output stream of`ExchangeReceiver`:
///         // [0,0,0,...]
///         // [1,1,1,...]
///         // [2,2,2,...]
///         // ...
///         combined.inspect(move |v| {
///             assert_eq!(&vec![round; WORKERS], v);
///             round += 1;
///         });
///     })
///     .unwrap()
///     .0;
///
///     for _ in 1..ROUNDS {
///         circuit.step();
///     }
/// });
///
/// hruntime.join().unwrap();
/// # }
/// ```
pub struct ExchangeSender<D, T, L> {
    worker_index: usize,
    location: OperatorLocation,
    partition: L,
    outputs: Vec<T>,
    exchange: Arc<Exchange<T>>,
    phantom: PhantomData<D>,
}

impl<D, T, L> ExchangeSender<D, T, L>
where
    T: Send + 'static,
{
    fn new(
        runtime: &Runtime,
        worker_index: usize,
        location: OperatorLocation,
        exchange_id: usize,
        partition: L,
    ) -> Self {
        debug_assert!(worker_index < runtime.num_workers());

        Self {
            worker_index,
            location,
            partition,
            outputs: Vec::with_capacity(runtime.num_workers()),
            exchange: Exchange::with_runtime(runtime, exchange_id),
            phantom: PhantomData,
        }
    }
}

impl<D, T, L> Operator for ExchangeSender<D, T, L>
where
    D: 'static,
    T: Send + 'static,
    L: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("ExchangeSender")
    }

    fn location(&self) -> OperatorLocation {
        self.location
    }

    fn is_async(&self) -> bool {
        true
    }

    fn register_ready_callback<F>(&mut self, callback: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.exchange
            .register_sender_callback(self.worker_index, callback)
    }

    fn ready(&self) -> bool {
        self.exchange.ready_to_send(self.worker_index)
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<D, T, L> SinkOperator<D> for ExchangeSender<D, T, L>
where
    D: Clone + 'static,
    T: Clone + Send + 'static,
    L: FnMut(D, &mut Vec<T>) + 'static,
{
    fn eval(&mut self, input: &D) {
        self.eval_owned(input.clone());
    }

    fn eval_owned(&mut self, input: D) {
        self.outputs.clear();
        (self.partition)(input, &mut self.outputs);

        self.exchange
            .try_send(self.worker_index, &mut self.outputs.drain(..));
    }

    fn input_preference(&self) -> OwnershipPreference {
        OwnershipPreference::PREFER_OWNED
    }
}

/// Operator that receives values sent by the `ExchangeSender` operator and
/// assembles them into a single output value.
///
/// See [`ExchangeSender`] documentation for details.
///
/// `ExchangeReceiver` is an asynchronous operator., i.e.,
/// [`ExchangeReceiver::is_async`] returns `true`.  It becomes schedulable
/// ([`ExchangeReceiver::ready`] returns `true`) once all peers have sent values
/// for this worker in the current clock cycle.  The scheduler should use
/// [`ExchangeReceiver::register_ready_callback`] to get notified when the
/// operator becomes schedulable.
pub struct ExchangeReceiver<T, L> {
    worker_index: usize,
    location: OperatorLocation,
    combine: L,
    exchange: Arc<Exchange<T>>,
}

impl<T, L> ExchangeReceiver<T, L>
where
    T: Send + 'static,
{
    fn new(
        runtime: &Runtime,
        worker_index: usize,
        location: OperatorLocation,
        exchange_id: usize,
        combine: L,
    ) -> Self {
        debug_assert!(worker_index < runtime.num_workers());

        Self {
            worker_index,
            location,
            combine,
            exchange: Exchange::with_runtime(runtime, exchange_id),
        }
    }
}

impl<T, L> Operator for ExchangeReceiver<T, L>
where
    T: Send + 'static,
    L: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("ExchangeReceiver")
    }

    fn location(&self) -> OperatorLocation {
        self.location
    }

    fn is_async(&self) -> bool {
        true
    }

    fn register_ready_callback<F>(&mut self, cb: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.exchange
            .register_receiver_callback(self.worker_index, cb)
    }

    fn ready(&self) -> bool {
        self.exchange.ready_to_receive(self.worker_index)
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<D, T, L> SourceOperator<D> for ExchangeReceiver<T, L>
where
    D: Default + Clone,
    T: Clone + Send + 'static,
    L: Fn(&mut D, T) + 'static,
{
    fn eval(&mut self) -> D {
        debug_assert!(self.ready());
        let mut combined = Default::default();
        let res = self
            .exchange
            .try_receive(self.worker_index, |x| (self.combine)(&mut combined, x));
        debug_assert!(res);

        combined
    }
}

impl<P> Circuit<P>
where
    P: Clone + 'static,
{
    /// Create an [`ExchangeSender`]/[`ExchangeReceiver`] operator pair.
    ///
    /// See [`ExchangeSender`] documentation for details and example usage.
    ///
    /// # Arguments
    ///
    /// * `runtime` - [`Runtime`](`crate::circuit::Runtime`) within which
    ///   operators are created.
    /// * `worker_index` - index of the current worker.
    /// * `partition` - partitioning logic that, for each element of the input
    ///   stream, returns an iterator with exactly `runtime.num_workers()`
    ///   values.
    /// * `combine` - re-assemble logic that combines values received from all
    ///   peers into a single output value.
    ///
    /// # Type arguments
    /// * `TI` - Type of values in the input stream consumed by
    ///   `ExchangeSender`.
    /// * `TO` - Type of values in the output stream produced by
    ///   `ExchangeReceiver`.
    /// * `TE` - Type of values sent across workers.
    /// * `PL` - Type of closure that splits a value of type `TI` into
    ///   `runtime.num_workers()` values of type `TE`.
    /// * `I` - Iterator returned by `PL`.
    /// * `CL` - Type of closure that folds `num_workers` values of type `TE`
    ///   into a value of type `TO`.
    pub fn new_exchange_operators<TI, TO, TE, PL, CL>(
        &self,
        runtime: &Runtime,
        worker_index: usize,
        location: OperatorLocation,
        partition: PL,
        combine: CL,
    ) -> (ExchangeSender<TI, TE, PL>, ExchangeReceiver<TE, CL>)
    where
        TO: Default + Clone,
        TE: Send + 'static,
        PL: FnMut(TI, &mut Vec<TE>) + 'static,
        CL: Fn(&mut TO, TE) + 'static,
    {
        let exchange_id = runtime.sequence_next(worker_index);
        let sender = ExchangeSender::new(runtime, worker_index, location, exchange_id, partition);
        let receiver = ExchangeReceiver::new(runtime, worker_index, location, exchange_id, combine);
        (sender, receiver)
    }
}

#[cfg(test)]
mod tests {
    use super::Exchange;
    use crate::{
        circuit::{
            schedule::{DynamicScheduler, Scheduler, StaticScheduler},
            Runtime,
        },
        operator::Generator,
        Circuit,
    };
    use std::thread::yield_now;

    // We decrease the number of rounds we do when we're running under miri,
    // otherwise it'll run forever
    const ROUNDS: usize = if cfg!(miri) { 128 } else { 2048 };

    // Create an exchange object with `WORKERS` concurrent senders/receivers.
    // Iterate for `ROUNDS` rounds with each sender sending value `N` to each
    // receiver in round number `N`.  Both senders and receivers may retry
    // sending/receiving multiple times, but in the end each receiver should get
    // all values in correct order.
    #[test]
    #[cfg_attr(miri, ignore)]
    fn test_exchange() {
        const WORKERS: usize = 16;

        Runtime::run(WORKERS, || {
            let current_worker = Runtime::worker_index();
            let exchange = Exchange::with_runtime(&Runtime::runtime().unwrap(), 0);

            let (send_unparker, recv_unparker) = Runtime::parker()
                .with(|parker| (parker.unparker().clone(), parker.unparker().clone()));
            exchange.register_sender_callback(current_worker, move || send_unparker.unpark());
            exchange.register_receiver_callback(current_worker, move || recv_unparker.unpark());

            for round in 0..ROUNDS {
                let output_data = vec![round; WORKERS];

                let mut output_iter = output_data.clone().into_iter();
                loop {
                    if exchange.try_send(current_worker, &mut output_iter) {
                        break;
                    }

                    yield_now();
                }

                let mut input_data = Vec::with_capacity(WORKERS);
                loop {
                    if exchange.try_receive(current_worker, |x| input_data.push(x)) {
                        break;
                    }

                    yield_now();
                }

                assert_eq!(input_data, output_data);
            }
        })
        .join()
        .unwrap();
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn test_exchange_operators_static() {
        test_exchange_operators::<StaticScheduler>();
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn test_exchange_operators_dynamic() {
        test_exchange_operators::<DynamicScheduler>();
    }

    // Create a circuit with `WORKERS` concurrent workers with the following
    // structure: `Generator - ExchangeSender -> ExchangeReceiver -> Inspect`.
    // `Generator` - yields sequential numbers 0, 1, 2, ...
    // `ExchangeSender` - sends each number to all peers.
    // `ExchangeReceiver` - combines all received numbers in a vector.
    // `Inspect` - validates the output of the receiver.
    fn test_exchange_operators<S>()
    where
        S: Scheduler + 'static,
    {
        fn do_test<S>(workers: usize)
        where
            S: Scheduler + 'static,
        {
            Runtime::run(workers, move || {
                let circuit = Circuit::build_with_scheduler::<_, _, S>(move |circuit| {
                    let mut n: usize = 0;
                    let source = circuit.add_source(Generator::new(move || {
                        let result = n;
                        n += 1;
                        result
                    }));

                    let (sender, receiver) = circuit.new_exchange_operators(
                        &Runtime::runtime().unwrap(),
                        Runtime::worker_index(),
                        None,
                        move |n, vals| {
                            for _ in 0..workers {
                                vals.push(n)
                            }
                        },
                        |v: &mut Vec<usize>, n| v.push(n),
                    );

                    let mut round = 0;
                    circuit
                        .add_exchange(sender, receiver, &source)
                        .inspect(move |v| {
                            assert_eq!(&vec![round; workers], v);
                            round += 1;
                        });
                })
                .unwrap()
                .0;

                for _ in 1..ROUNDS {
                    circuit.step().unwrap();
                }
            })
            .join()
            .unwrap();
        }

        do_test::<S>(1);
        do_test::<S>(16);
        do_test::<S>(32);
    }
}
