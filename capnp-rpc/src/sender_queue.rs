// Copyright (c) 2013-2016 Sandstorm Development Group, Inc. and contributors
// Licensed under the MIT License:
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

use capnp::capability::Promise;
use capnp::Error;
use futures::channel::oneshot;
use futures::{FutureExt, TryFutureExt};
use std::iter::Iterator;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

struct SenderQueueItem<In, Out>
where
    In: 'static,
    Out: 'static,
{
    input: In,
    out: oneshot::Sender<Out>,
    done: Arc<AtomicBool>,
}

/// A queue representing tasks that consume input of type `In` and produce output of
/// type `Out`.
pub struct SenderQueue<In, Out>(Vec<SenderQueueItem<In, Out>>)
where
    In: 'static,
    Out: 'static;

impl<In, Out> SenderQueue<In, Out>
where
    In: Send + Sync + 'static,
    Out: Send + Sync + 'static,
{
    pub fn new() -> Self {
        Self(Vec::new())
    }

    /// Pushes `value` to the queue, returning a promise that resolves after
    /// `value` is consumed on the other end of the queue. If the returned promised
    /// is dropped, then `value` is removed from the queue.
    pub fn push(&mut self, value: In) -> Promise<Out, Error> {
        let (tx, rx) = oneshot::channel();
        let done = Arc::new(AtomicBool::new(false));

        self.0.push(SenderQueueItem {
            input: value,
            out: tx,
            done: Arc::clone(&done),
        });

        Promise::from_future(
            rx.map_err(|_| Error::failed("SenderQueue canceled".into()))
                .map(move |out| {
                    done.store(true, Ordering::SeqCst);
                    out
                }),
        )
    }

    /// Pushes `values` to the queue.
    pub fn push_detach(&mut self, value: In) {
        let (tx, _rx) = oneshot::channel();
        let done = Arc::new(AtomicBool::new(false));

        self.0.push(SenderQueueItem {
            input: value,
            out: tx,
            done: Arc::clone(&done),
        });
    }

    pub fn drain(&mut self) -> impl Iterator<Item = (In, oneshot::Sender<Out>)> {
        std::mem::take(&mut self.0).into_iter().filter_map(|item| {
            if item.done.load(Ordering::SeqCst) {
                None
            } else {
                Some((item.input, item.out))
            }
        })
    }
}
