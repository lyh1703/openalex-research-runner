# OpenAlex Research 002 — Stage 13 In-Progress Checkpoint

Persistent baseline:
- Stages 0–12: COMPLETE
- Stage 10/11 derived QA: PASS
- Stage 12 citation graph: COMPLETE
- Stage 10–12 authoritative run: 36585853580

Stage 13:
- Goal: Author / Institution / Country Network
- Active run: 36604573610 (Run #3)
- Trigger head: 82ba0f87d9a627739dd575279e331a48f04536fe
- Frozen contract: STAGE13_NETWORK_CONTRACT.md
- 16 balanced shards planned over 2,040 frozen Works files
- One-file pilot precedes full fan-out

Recovery history:
1. Run #1 failed because the pilot selected a tiny 2-work tail partition; no data corruption.
2. Run #2 used a representative 298,638-work source and proved country/author logic, but exposed that top-level `work.institutions` is not reliable as the historical institution authority in this snapshot path.
3. Stage 13 was minimally patched to derive institution activity/pairs from `work.authorships[].institutions[]`, consistent with the Stage 7 frozen relationship contract.
4. Run #3 is the first run after this authority fix.

Do not start a duplicate Stage 13 workflow while Run 36604573610 is queued/in_progress.
If it fails, inspect only the failing step and patch from this checkpoint.
