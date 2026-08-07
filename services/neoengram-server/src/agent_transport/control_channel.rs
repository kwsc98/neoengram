use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use neoengram_protocol::{AgentId, SessionGeneration, SessionId};
use tokio::sync::{watch, Mutex, OwnedMutexGuard};

#[derive(Clone, Default)]
pub(super) struct LiveAgentChannels {
    inner: Arc<LiveAgentChannelsInner>,
}

#[derive(Default)]
struct LiveAgentChannelsInner {
    next_registration: AtomicU64,
    state: Mutex<LiveAgentChannelsState>,
}

#[derive(Default)]
struct LiveAgentChannelsState {
    active: BTreeMap<AgentId, LiveAgentChannel>,
    gates: BTreeMap<AgentId, Arc<Mutex<()>>>,
}

struct LiveAgentChannel {
    registration_id: u64,
    session_id: SessionId,
    session_generation: SessionGeneration,
    replaced: watch::Sender<bool>,
}

pub(super) struct LiveAgentChannelRegistration {
    pub(super) registration_id: u64,
    pub(super) replaced: watch::Receiver<bool>,
}

/// Serializes a session transition or one upstream frame application for a single Agent.
pub(super) struct LiveAgentChannelFence {
    agent_id: AgentId,
    _guard: OwnedMutexGuard<()>,
}

impl LiveAgentChannels {
    pub(super) async fn acquire_fence(&self, agent_id: AgentId) -> LiveAgentChannelFence {
        let gate = {
            let mut state = self.inner.state.lock().await;
            state
                .gates
                .entry(agent_id.clone())
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .clone()
        };
        LiveAgentChannelFence {
            agent_id,
            _guard: gate.lock_owned().await,
        }
    }

    #[cfg(test)]
    pub(super) async fn register(
        &self,
        agent_id: AgentId,
        session_id: SessionId,
        session_generation: SessionGeneration,
    ) -> LiveAgentChannelRegistration {
        let fence = self.acquire_fence(agent_id).await;
        self.register_fenced(&fence, session_id, session_generation)
            .await
    }

    pub(super) async fn register_fenced(
        &self,
        fence: &LiveAgentChannelFence,
        session_id: SessionId,
        session_generation: SessionGeneration,
    ) -> LiveAgentChannelRegistration {
        let registration_id = self
            .inner
            .next_registration
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        let (replaced, receiver) = watch::channel(false);
        let previous = self.inner.state.lock().await.active.insert(
            fence.agent_id.clone(),
            LiveAgentChannel {
                registration_id,
                session_id,
                session_generation,
                replaced,
            },
        );
        if let Some(previous) = previous {
            tracing::debug!(
                session_id = %previous.session_id,
                session_generation = previous.session_generation.get(),
                "Replacing the Agent's live reverse control channel"
            );
            let _ = previous.replaced.send(true);
        }
        LiveAgentChannelRegistration {
            registration_id,
            replaced: receiver,
        }
    }

    pub(super) async fn enter_current(
        &self,
        agent_id: &AgentId,
        registration_id: u64,
    ) -> Option<LiveAgentChannelFence> {
        let gate = {
            let state = self.inner.state.lock().await;
            if !state
                .active
                .get(agent_id)
                .is_some_and(|channel| channel.registration_id == registration_id)
            {
                return None;
            }
            state.gates.get(agent_id)?.clone()
        };
        let guard = gate.lock_owned().await;
        let is_current = self
            .inner
            .state
            .lock()
            .await
            .active
            .get(agent_id)
            .is_some_and(|channel| channel.registration_id == registration_id);
        is_current.then(|| LiveAgentChannelFence {
            agent_id: agent_id.clone(),
            _guard: guard,
        })
    }

    pub(super) async fn unregister(&self, agent_id: &AgentId, registration_id: u64) {
        let mut state = self.inner.state.lock().await;
        if state
            .active
            .get(agent_id)
            .is_some_and(|channel| channel.registration_id == registration_id)
        {
            state.active.remove(agent_id);
        }
    }

    #[cfg(test)]
    async fn current(&self, agent_id: &AgentId) -> Option<(SessionId, SessionGeneration)> {
        self.inner
            .state
            .lock()
            .await
            .active
            .get(agent_id)
            .map(|channel| (channel.session_id.clone(), channel.session_generation))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn reconnect_replaces_live_channel_without_stale_unregister_removing_it() {
        let channels = LiveAgentChannels::default();
        let agent_id = AgentId::new("agent-a").unwrap();
        let first = channels
            .register(
                agent_id.clone(),
                SessionId::new("session-a").unwrap(),
                SessionGeneration::new(1),
            )
            .await;
        let mut first_replaced = first.replaced.clone();
        let second = channels
            .register(
                agent_id.clone(),
                SessionId::new("session-b").unwrap(),
                SessionGeneration::new(2),
            )
            .await;

        first_replaced.changed().await.unwrap();
        assert!(*first_replaced.borrow());
        channels.unregister(&agent_id, first.registration_id).await;
        assert_eq!(
            channels.current(&agent_id).await,
            Some((
                SessionId::new("session-b").unwrap(),
                SessionGeneration::new(2)
            ))
        );

        channels.unregister(&agent_id, second.registration_id).await;
        assert_eq!(channels.current(&agent_id).await, None);
    }

    #[tokio::test]
    async fn replacement_waits_for_in_flight_frame_and_fences_the_old_registration() {
        let channels = LiveAgentChannels::default();
        let agent_id = AgentId::new("agent-a").unwrap();
        let first = channels
            .register(
                agent_id.clone(),
                SessionId::new("session-a").unwrap(),
                SessionGeneration::new(1),
            )
            .await;
        let in_flight = channels
            .enter_current(&agent_id, first.registration_id)
            .await
            .unwrap();

        let replacement_channels = channels.clone();
        let replacement_agent_id = agent_id.clone();
        let mut replacement = tokio::spawn(async move {
            replacement_channels
                .register(
                    replacement_agent_id,
                    SessionId::new("session-b").unwrap(),
                    SessionGeneration::new(2),
                )
                .await
        });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), &mut replacement)
                .await
                .is_err()
        );

        drop(in_flight);
        let second = replacement.await.unwrap();
        assert!(channels
            .enter_current(&agent_id, first.registration_id)
            .await
            .is_none());
        assert!(channels
            .enter_current(&agent_id, second.registration_id)
            .await
            .is_some());
    }
}
