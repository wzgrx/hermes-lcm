# Session lifecycle and rotate

Hermes `/new` starts a new host session. On gateway surfaces that supply the outgoing session ID to the observer hook, Hermes-LCM clears that conversation's summary carry pointer and frontier before the next inbound turn. A delayed finalization of the old session does not rearm the pointer. Compression-initiated session rollover is separate and still carries eligible summaries.

The operation preserves historical raw rows and summary nodes in `lcm.db` for explicit, bounded recall; it only removes them from automatic new-session inheritance. Other conversations are untouched. CLI hosts that omit the outgoing ID from the reset hook retain their existing reset behavior; inspect the host hook payload before promising the same carry fence there.

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
