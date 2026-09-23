# Session lifecycle and rotate

Hermes `/new` starts a new host session. On gateway surfaces that supply the outgoing session ID to the observer hook, Hermes-LCM clears that conversation's summary carry pointer and frontier and deletes its current/finalized summary nodes at every depth before the next inbound turn. Current Hermes CLI emits a finalize hook with the outgoing ID and then a reset hook with the new ID; LCM pairs those same-thread events only after the host `state.db` confirms an actual `new_session` rotation. A delayed finalization of the old session does not rearm the pointer. Compression-initiated session rollover is separate and still carries eligible summaries.

The operation preserves historical raw rows in `lcm.db` for explicit, bounded recall. It deletes the outgoing conversation summary nodes; other conversations are untouched. Older CLI hosts without both hook events or a verifiable session database retain their existing reset behavior.

## `/lcm rotate`

`/lcm rotate` is different from `/new`:

- it keeps the current `session_id` and `conversation_id`;
- preview is read-only;
- apply creates/updates the rolling rotate backup first;
- it preserves the configured fresh tail;
- it advances the lifecycle frontier past older raw messages so bootstrap does not replay them into active context;
- it does not delete raw source rows or call a summarization model.

Run normal compaction before rotate when older material must be represented in summary nodes. Even without a summary, pre-tail raw rows remain recoverable through `lcm_load_session` and `lcm_expand`.

Rotate refuses ignored or stateless sessions. Repeating an already-satisfied rotate reports a no-op and preserves the previous known-good rolling backup.

Use a separate session when the user wants a new active conversational boundary. Use rotate when the problem is active transcript/frontier size without changing identity.
