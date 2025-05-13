use std::{
    collections::{HashMap, VecDeque},
    future::Future,
    pin::Pin,
    task::{ready, Context, Poll},
};

use super::{Result, SubscribeError};
use crate::{
    groups::{
        mls_sync::GroupMessageProcessingError, scoped_client::ScopedGroupClient,
        summary::SyncSummary, MlsGroup,
    },
    intents::ProcessIntentError,
};
use futures::Stream;
use pin_project_lite::pin_project;
use tracing::Instrument;
use xmtp_api::GroupFilter;
use xmtp_common::types::GroupId;
use xmtp_common::{retry_async, FutureWrapper, Retry};
use xmtp_db::{group_message::StoredGroupMessage, refresh_state::EntityKind};
use xmtp_id::InboxIdRef;
use xmtp_proto::{
    api_client::{trait_impls::XmtpApi, XmtpMlsStreams},
    xmtp::mls::api::v1::{group_message, GroupMessage},
};

#[derive(thiserror::Error, Debug)]
pub enum MessageStreamError {
    #[error("received message for not subscribed group {id}", id = hex::encode(_0))]
    NotSubscribed(Vec<u8>),
    #[error("Invalid Payload")]
    InvalidPayload,
}

impl xmtp_common::RetryableError for MessageStreamError {
    fn is_retryable(&self) -> bool {
        use MessageStreamError::*;
        match self {
            NotSubscribed(_) | InvalidPayload => false,
        }
    }
}

pub fn extract_message_v1(message: GroupMessage) -> Option<group_message::V1> {
    match message.version {
        Some(group_message::Version::V1(value)) => Some(value),
        _ => None,
    }
}

pub fn extract_message_cursor(message: &GroupMessage) -> Option<u64> {
    match &message.version {
        Some(group_message::Version::V1(value)) => Some(value.id),
        _ => None,
    }
}

/// the position of this message in the backend topic
/// based only upon messages from the stream
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MessagePosition {
    /// current message
    cursor: Option<u64>,
}

impl MessagePosition {
    pub(super) fn set(&mut self, cursor: u64) {
        self.cursor = Some(cursor);
    }

    fn pos(&self) -> u64 {
        self.cursor.unwrap_or(0)
    }
}

impl std::fmt::Display for MessagePosition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.pos())
    }
}

impl From<u64> for MessagePosition {
    fn from(v: u64) -> MessagePosition {
        Self { cursor: Some(v) }
    }
}

pin_project! {
    pub struct StreamGroupMessages<'a, C, Subscription> {
        #[pin] inner: Subscription,
        #[pin] state: State<'a, Subscription>,
        client: &'a C,
        pub(super) group_list: HashMap<GroupId, MessagePosition>,
        add_queue: VecDeque<MlsGroup<C>>,
        returned: Vec<u64>,
        got: Vec<u64>
    }
}

pin_project! {
    #[project = ProjectState]
    #[derive(Default)]
    enum State<'a, Out> {
        /// State that indicates the stream is waiting on the next message from the network
        #[default]
        Waiting,
        /// state that indicates the stream is waiting on a IO/Network future to finish processing
        /// the current message before moving on to the next one
        Processing {
            #[pin] future: FutureWrapper<'a, Result<ProcessedMessage>>
        },
        Adding {
            #[pin] future: FutureWrapper<'a, Result<(Out, Vec<u8>, Option<u64>)>>
        }
    }
}

pub(super) type MessagesApiSubscription<'a, C> =
    <<C as ScopedGroupClient>::ApiClient as XmtpMlsStreams>::GroupMessageStream<'a>;

impl<'a, C> StreamGroupMessages<'a, C, MessagesApiSubscription<'a, C>>
where
    C: ScopedGroupClient + 'a,
    <C as ScopedGroupClient>::ApiClient: XmtpApi + XmtpMlsStreams + 'a,
{
    pub async fn new(client: &'a C, group_list: Vec<GroupId>) -> Result<Self> {
        tracing::debug!("setting up messages subscription");

        let mut group_list = group_list
            .into_iter()
            .map(|group_id| (group_id, 0u64))
            .collect::<HashMap<GroupId, u64>>();

        let cursors = group_list
            .keys()
            .map(|group| client.api().query_latest_group_message(group));

        let cursors = futures::future::join_all(cursors)
            .await
            .into_iter()
            .map(|r| r.map_err(SubscribeError::from))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        for message in cursors {
            let group_message::V1 {
                id: cursor,
                group_id,
                ..
            } = extract_message_v1(message).ok_or(MessageStreamError::InvalidPayload)?;
            group_list
                .entry(group_id.clone().into())
                .and_modify(|e| *e = cursor);
        }

        let filters: Vec<GroupFilter> = group_list
            .iter()
            .inspect(|(group_id, cursor)| {
                tracing::debug!(
                    "subscribed to group {} at {}",
                    xmtp_common::fmt::truncate_hex(hex::encode(group_id)),
                    cursor
                )
            })
            .map(|(group_id, cursor)| GroupFilter::new(group_id.to_vec(), Some(*cursor)))
            .collect();
        let subscription = client.api().subscribe_group_messages(filters).await?;

        Ok(Self {
            inner: subscription,
            client,
            state: Default::default(),
            group_list: group_list.into_iter().map(|(g, c)| (g, c.into())).collect(),
            got: Default::default(),
            returned: Default::default(),
            add_queue: Default::default(),
        })
    }

    /// Add a new group to this messages stream
    pub(super) fn add(mut self: Pin<&mut Self>, group: MlsGroup<C>) {
        if self.group_list.contains_key(group.group_id.as_slice()) {
            tracing::debug!("group {} already in stream", hex::encode(&group.group_id));
            return;
        }

        // if we're waiting, resolve it right away
        if let State::Waiting = self.state {
            self.resolve_group_additions(group);
        } else {
            tracing::debug!("stream busy, queuing group add");
            // any other state and the group must be added to queue
            let this = self.as_mut().project();
            this.add_queue.push_back(group);
        }
    }

    // re-subscribe to the stream with a new group
    async fn subscribe(
        client: &'a C,
        mut filters: Vec<GroupFilter>,
        new_group: Vec<u8>,
    ) -> Result<(MessagesApiSubscription<'a, C>, Vec<u8>, Option<u64>)> {
        // get the last synced cursor
        let last_cursor = {
            let provider = client.mls_provider();
            provider
                .db()
                .get_last_cursor_for_id(&new_group, EntityKind::Group)
        }?;

        match last_cursor {
            // we dont have messages for the group yet
            0 => {
                let stream = client.api().subscribe_group_messages(filters).await?;
                Ok((stream, new_group, Some(1)))
            }
            c => {
                // should we query for the latest message here instead?
                if let Some(new) = filters.iter_mut().find(|f| f.group_id == new_group) {
                    new.id_cursor = Some(c as u64);
                }
                let stream = client.api().subscribe_group_messages(filters).await?;
                Ok((stream, new_group, Some(c as u64)))
            }
        }
    }
}

impl<'a, C> Stream for StreamGroupMessages<'a, C, MessagesApiSubscription<'a, C>>
where
    C: ScopedGroupClient + 'a,
    <C as ScopedGroupClient>::ApiClient: XmtpApi + XmtpMlsStreams + 'a,
{
    type Item = Result<StoredGroupMessage>;

    #[tracing::instrument(level = "trace", skip_all)]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use ProjectState::*;
        let mut this = self.as_mut().project();
        let state = this.state.as_mut().project();
        match state {
            Waiting => {
                tracing::trace!("stream messages in waiting state");
                if let Some(group) = this.add_queue.pop_front() {
                    self.as_mut().resolve_group_additions(group);
                    return self.as_mut().poll_next(cx);
                }
                self.on_waiting(cx)
            }
            Processing { .. } => {
                tracing::trace!("stream messages in processing state");
                self.resolve_futures(cx)
            }
            Adding { future } => {
                tracing::trace!("stream messages in adding state");
                let (stream, group, cursor) = ready!(future.poll(cx))?;
                let this = self.as_mut();
                if let Some(c) = cursor {
                    this.set_cursor(group.as_slice(), c)
                };
                let mut this = self.as_mut().project();
                this.inner.set(stream);
                if let Some(cursor) = this.group_list.get(group.as_slice()) {
                    tracing::debug!(
                        "added group_id={} at cursor={} to messages stream",
                        hex::encode(&group),
                        cursor
                    );
                }
                this.state.as_mut().set(State::Waiting);
                self.poll_next(cx)
            }
        }
    }
}

impl<C, S> StreamGroupMessages<'_, C, S> {
    fn filters(&self) -> Vec<GroupFilter> {
        self.group_list
            .iter()
            .map(|(group_id, cursor)| GroupFilter::new(group_id.to_vec(), Some(cursor.pos())))
            .collect()
    }
}

impl<'a, C> StreamGroupMessages<'a, C, MessagesApiSubscription<'a, C>>
where
    C: ScopedGroupClient + 'a,
    <C as ScopedGroupClient>::ApiClient: XmtpApi + XmtpMlsStreams + 'a,
{
    fn on_waiting(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<<Self as Stream>::Item>> {
        let envelope = ready!(self.as_mut().next_message(cx));
        if envelope.is_none() {
            return Poll::Ready(None);
        }
        let mut envelope = envelope.expect("checked for none")?;
        // ensure we have not tried processing this message yet
        // if we have tried to process, replay messages up to the known cursor.
        let cursor = self
            .as_ref()
            .group_list
            .get(envelope.group_id.as_slice())
            .copied();
        if let Some(m) = cursor {
            if m > envelope.id.into() {
                tracing::debug!(
                    "current msg with group_id@[{}], has cursor@[{}]. skipping messages until cursor={m}",
                    xmtp_common::fmt::truncate_hex(hex::encode(
                        envelope.group_id.as_slice()
                    )),
                    envelope.id,
                );
                envelope = ready!(self.as_mut().skip(cx, envelope))?;
            }
            tracing::trace!(
                "group_id {} exists @cursor={m}, proceeding to process message @cursor={}",
                xmtp_common::fmt::truncate_hex(hex::encode(envelope.group_id.as_slice())),
                envelope.id
            );
        }
        let this = self.as_mut().project();
        let future = ProcessMessageFuture::new(*this.client, envelope)?;
        let future = future.process();
        let mut this = self.as_mut().project();
        this.state.set(State::Processing {
            future: FutureWrapper::new(future),
        });

        self.resolve_futures(cx)
    }

    /// Add the group to the group list
    /// and transition the stream to Adding state
    fn resolve_group_additions(mut self: Pin<&mut Self>, group: MlsGroup<C>) {
        tracing::debug!(
            inbox_id = self.client.inbox_id(),
            installation_id = %self.client.installation_id(),
            group_id = hex::encode(&group.group_id),
            "begin establishing new message stream to include group_id={}",
            hex::encode(&group.group_id)
        );
        let this = self.as_mut().project();
        this.group_list
            .insert(group.group_id.clone().into(), 1.into());
        let future = Self::subscribe(self.client, self.filters(), group.group_id);
        let mut this = self.as_mut().project();
        this.state.set(State::Adding {
            future: FutureWrapper::new(future),
        });
    }

    // iterative skip to avoid overflowing the stack
    fn skip(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        mut envelope: group_message::V1,
    ) -> Poll<Result<group_message::V1>> {
        // skip the messages
        while let Some(e) = ready!(self.as_mut().next_message(cx)) {
            let e = e?;
            if let Some(stream_cursor) =
                self.as_ref().group_list.get(e.group_id.as_slice()).copied()
            {
                if stream_cursor > e.id.into() {
                    tracing::debug!(
                        "skipping msg with group_id@[{}] and cursor@[{}]",
                        xmtp_common::fmt::truncate_hex(hex::encode(e.group_id.as_slice())),
                        e.id
                    );
                    continue;
                }
            } else {
                envelope = e;
                tracing::trace!("finished skipping");
                break;
            }
        }
        Poll::Ready(Ok(envelope))
    }

    fn next_message(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<group_message::V1>>> {
        let this = self.as_mut().project();
        if let Some(envelope) = ready!(this.inner.poll_next(cx)) {
            let envelope = envelope.map_err(|e| SubscribeError::BoxError(Box::new(e)))?;

            if let Some(msg) = extract_message_v1(envelope) {
                this.got.push(msg.id);
                tracing::trace!(
                    "got new message for group=[{}] @cursor=[{}] from network, total messages=[{}]",
                    xmtp_common::fmt::debug_hex(&msg.group_id),
                    msg.id,
                    this.got.len()
                );
                Poll::Ready(Some(Ok(msg)))
            } else {
                tracing::error!("bad message");
                // _NOTE_: This would happen if we receive a message
                // with a version not supported by the current client.
                // A version we don't know how to deserialize will return 'None'.
                // In this case the unreadable message would be skipped.
                self.next_message(cx)
            }
        } else {
            Poll::Ready(None)
        }
    }

    fn resolve_futures(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<<Self as Stream>::Item>> {
        use ProjectState::*;
        if let Processing { future } = self.as_mut().project().state.project() {
            let processed = ready!(future.poll(cx))?;
            let mut this = self.as_mut().project();
            if let Some(msg) = processed.message {
                this.state.set(State::Waiting);
                tracing::trace!(
                    "message processed, setting cursor to [{:?}] for group {}",
                    processed.next_message,
                    xmtp_common::fmt::truncate_hex(hex::encode(msg.group_id.as_slice()))
                );
                this.returned
                    .push(msg.sequence_id.map(|s| s as u64).unwrap_or(0u64));
                if let Some(c) = processed.next_message {
                    self.as_mut().set_cursor(msg.group_id.as_slice(), c);
                }
                tracing::trace!(
                    "returning new message for group=[{}] @cursor=[{:?}], total messages={}",
                    xmtp_common::fmt::debug_hex(msg.group_id.as_slice()),
                    msg.sequence_id,
                    self.returned.len()
                );
                tracing::error!(
                    "{}",
                    String::from_utf8_lossy(msg.decrypted_message_bytes.as_slice())
                );
                return Poll::Ready(Some(Ok(msg)));
            } else {
                this.state.set(State::Waiting);
                if let Some(cursor) = this.group_list.get_mut(processed.group_id.as_slice()) {
                    if let Some(c) = processed.next_message {
                        tracing::info!(
                            "no message could be processed, stream setting cursor to [{:?}] for group: {}",
                            processed.next_message,
                            xmtp_common::fmt::truncate_hex(hex::encode(processed.group_id.as_slice()))
                        );

                        cursor.set(c)
                    }
                }
                tracing::trace!(
                    "skipping message for group=[{}] @cursor=[{}]",
                    xmtp_common::fmt::debug_hex(&processed.group_id),
                    processed.tried_to_process
                );
                return self.poll_next(cx);
            }
        }
        Poll::Pending
    }

    fn set_cursor(mut self: Pin<&mut Self>, group_id: &[u8], new_cursor: u64) {
        let this = self.as_mut().project();
        if let Some(cursor) = this.group_list.get_mut(group_id) {
            cursor.set(new_cursor);
        }
    }
}

/// Future that processes a group message from the network
pub struct ProcessMessageFuture<Client> {
    client: Client,
    msg: group_message::V1,
}

// The processed message
pub struct ProcessedMessage {
    pub message: Option<StoredGroupMessage>,
    group_id: Vec<u8>,
    next_message: Option<u64>,
    tried_to_process: u64,
}

impl<C> ProcessMessageFuture<C>
where
    C: ScopedGroupClient,
{
    /// Create a new Future to process a GroupMessage.
    pub fn new(client: C, msg: group_message::V1) -> Result<ProcessMessageFuture<C>> {
        Ok(Self { client, msg })
    }

    fn inbox_id(&self) -> InboxIdRef<'_> {
        self.client.inbox_id()
    }

    /// process a message, returning the message from the database and the cursor of the message.
    /// There will always be a cursor returned.
    /// If a cursor is returned but a message is not, it means we tried processing up to `cursor`
    /// but were not able to get a message from it.
    #[tracing::instrument(skip_all)]
    pub(crate) async fn process(self) -> Result<ProcessedMessage> {
        let group_message::V1 {
            // the cursor ID is the position in the monolithic backend topic
            id: ref cursor_id,
            ref created_ns,
            ..
        } = self.msg;

        tracing::debug!(
            inbox_id = self.inbox_id(),
            group_id = hex::encode(&self.msg.group_id),
            cursor_id,
            "[{}]  is about to process streamed envelope for group {} cursor_id=[{}]",
            self.inbox_id(),
            xmtp_common::fmt::truncate_hex(hex::encode(&self.msg.group_id)),
            &cursor_id
        );

        let summary = if self.needs_to_sync(*cursor_id)? {
            self.process_stream_entry().await
        } else {
            tracing::debug!(
                "stream does not require sync; message should be available in database"
            );
            SyncSummary::single((&self.msg).into())
        };

        let conn = self.client.context_ref().db();
        // Load the message from the DB to handle cases where it may have been already processed in
        // another thread
        let new_message =
            conn.get_group_message_by_timestamp(&self.msg.group_id, *created_ns as i64)?;

        if let Some(msg) = new_message {
            tracing::debug!(
                "[{}] processed stream envelope [{}]",
                self.inbox_id(),
                &cursor_id
            );
            Ok(ProcessedMessage {
                message: Some(msg),
                next_message: Some(*cursor_id),
                group_id: self.msg.group_id,
                tried_to_process: self.msg.id,
            })
        } else {
            tracing::warn!(
                cursor_id,
                inbox_id = self.inbox_id(),
                group_id = hex::encode(&self.msg.group_id),
                "no message present in db for message @cursor=[{}] in group [{}]",
                &cursor_id,
                hex::encode(&self.msg.group_id),
            );
            let mut processed = summary.process;
            processed.new_messages.sort_by_key(|k| k.cursor);
            let next: Option<u64> = processed.new_messages.first().map(|f| f.cursor);
            // if we have no new messages, set the cursor to the latest total message we processed
            // or, if we did not process anything, set it back to 0.
            let next = next.or(processed.last());
            Ok(ProcessedMessage {
                message: None,
                next_message: next,
                group_id: self.msg.group_id,
                tried_to_process: self.msg.id,
            })
        }
    }

    /// stream processing function
    async fn process_stream_entry(&self) -> SyncSummary {
        use SubscribeError::*;
        let process_result = retry_async!(
            Retry::default(),
            (async {
                let (group, _) = MlsGroup::new_validated(&self.client, self.msg.group_id.clone())?;
                let epoch = group.epoch().await?;

                tracing::debug!(
                    inbox_id = self.inbox_id(),
                    group_id = hex::encode(&self.msg.group_id),
                    cursor_id = self.msg.id,
                    epoch = epoch,
                    "epoch={} for [{}] in process_stream_entry()",
                    epoch,
                    self.inbox_id(),
                );
                group
                    .process_message(&self.msg, false)
                    .instrument(tracing::debug_span!("process_message"))
                    .await
                    // NOTE: We want to make sure we retry an error in process_message
                    .map_err(SubscribeError::ReceiveGroup)
            })
        );

        match process_result {
            Err(ReceiveGroup(GroupMessageProcessingError::MessageAlreadyProcessed(msg)))
            | Err(ReceiveGroup(GroupMessageProcessingError::ProcessIntent(
                ProcessIntentError::MessageAlreadyProcessed(msg),
            ))) => {
                tracing::debug!("message {msg:?} already processed");
                SyncSummary::single(msg)
            }
            Err(ReceiveGroup(e)) => self.attempt_message_recovery(e).await,
            Err(e) => {
                // This should never occur because we map the error to `ReceiveGroup`
                // But still exists defensively
                tracing::error!(
                    inbox_id = self.client.inbox_id(),
                    group_id = hex::encode(&self.msg.group_id),
                    cursor_id = self.msg.id,
                    err = e.to_string(),
                    "process stream entry {:?}",
                    e
                );
                SyncSummary::default()
            }
            Ok(msg) => {
                tracing::trace!(
                    cursor_id = self.msg.id,
                    inbox_id = self.inbox_id(),
                    group_id = hex::encode(&self.msg.group_id),
                    "message process in stream success, synced single msg @cursor={},group_id={}",
                    msg.cursor,
                    xmtp_common::fmt::truncate_hex(hex::encode(&msg.group_id))
                );
                SyncSummary::single(msg)
            }
        }
    }

    /// Checks if a message has already been processed through a sync
    fn needs_to_sync(&self, current_msg_cursor: u64) -> Result<bool> {
        let last_synced_id = self
            .client
            .db()
            .get_last_cursor_for_id(&self.msg.group_id, EntityKind::Group)?;
        Ok(last_synced_id < current_msg_cursor as i64)
    }

    /// Attempt a recovery sync if a group message failed to process
    async fn attempt_message_recovery(&self, e: impl std::error::Error) -> SyncSummary {
        let group = MlsGroup::new(
            &self.client,
            self.msg.group_id.clone(),
            None,
            self.msg.created_ns as i64,
        );
        let epoch = group.epoch().await.unwrap_or(0);
        tracing::debug!(
            inbox_id = self.client.inbox_id(),
            group_id = hex::encode(&self.msg.group_id),
            cursor_id = self.msg.id,
            epoch = epoch,
            "processing streamed message @cursor=[{}] failed with [{e}], attempting recovery sync for group {} in epoch {}",
            self.msg.id,
            xmtp_common::fmt::debug_hex(&self.msg.group_id),
            epoch
        );
        // Swallow errors here, since another process may have successfully saved the message
        // to the DB
        let sync = group
            .sync_with_conn()
            .instrument(tracing::debug_span!("sync_with_conn"))
            .await;
        match sync {
            Ok(summary) => {
                let epoch = group.epoch().await.unwrap_or(0);
                tracing::debug!(
                    inbox_id = self.client.inbox_id(),
                    group_id = hex::encode(&self.msg.group_id),
                    cursor_id = self.msg.id,
                    "recovery sync triggered by streamed message successful, epoch = {} for group = {}",
                    epoch,
                    xmtp_common::fmt::truncate_hex(hex::encode(&self.msg.group_id))
                );
                tracing::debug!("{summary}");
                summary
            }
            Err(summary) => {
                tracing::warn!(
                    inbox_id = self.client.inbox_id(),
                    group_id = hex::encode(&self.msg.group_id),
                    cursor_id = self.msg.id,
                    "recovery sync triggered by streamed message failed",
                );
                tracing::warn!("{summary}");
                summary
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::stream::StreamExt;

    use crate::assert_msg;
    use crate::{builder::ClientBuilder, groups::GroupMetadataOptions};
    use xmtp_cryptography::utils::generate_local_wallet;

    #[rstest::rstest]
    #[xmtp_common::test]
    #[timeout(std::time::Duration::from_secs(5))]
    async fn test_stream_messages() {
        let alice = Arc::new(ClientBuilder::new_test_client(&generate_local_wallet()).await);
        let bob = ClientBuilder::new_test_client(&generate_local_wallet()).await;

        let alice_group = alice
            .create_group(None, GroupMetadataOptions::default())
            .unwrap();
        tracing::info!("Group Id = [{}]", hex::encode(&alice_group.group_id));

        alice_group
            .add_members_by_inbox_id(&[bob.inbox_id()])
            .await
            .unwrap();
        let bob_groups = bob.sync_welcomes().await.unwrap();
        let bob_group = bob_groups.first().unwrap();
        alice_group.sync().await.unwrap();

        let stream = alice_group.stream().await.unwrap();
        futures::pin_mut!(stream);
        bob_group.send_message(b"hello").await.unwrap();

        // group updated msg/bob is added
        // assert_msg_exists!(stream);
        assert_msg!(stream, "hello");

        bob_group.send_message(b"hello2").await.unwrap();
        assert_msg!(stream, "hello2");
    }
}
