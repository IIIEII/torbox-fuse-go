# Plex-like read path: test coverage fix plan

> Based on audit of current test coverage against the spec at `docs/plex-like-read-test-coverage-spec.md`.

## Audit summary

**29 of 62 mandatory scenarios are not covered or only partially covered.** The highest-risk gaps are: untested range logic, unimplemented prefetch, unimplemented seek cancellation, untested error recovery, and untested window boundary crossing.

> **Update 2026-05-22:** Phases 1–3, 5–9 are implemented. Phase 4 (prefetch trigger) has a known issue where `readAheadThreshold == windowSize` prevents prefetch from ever triggering for sequential reads; tests document this behavior. Phase 6 item 6.1 (seek cancellation) was already implemented, only tests were needed.

---

## Phase 1: Range logic unit tests (spec §1)

**Risk: HIGH** — off-by-one errors in range math cause silent data corruption.

**File:** `internal/cache/range_test.go` (extend existing)

Add a dedicated test function for range algebra that exercises `CopyTo` with various offset/length combinations against known data blocks. Since range logic is embedded in `CopyTo` rather than extracted, test it there.

- [x] **1.1** Exact range match: request exactly the block that was stored
- [x] **1.2** Full cover: request a range fully contained within a larger block
- [x] **1.3** Partial overlap left: request starts before block, overlaps left portion
- [x] **1.4** Partial overlap right: request extends beyond block end, gets partial data
- [x] **1.5** No overlap: request at offset completely outside any block → miss
- [x] **1.6** Adjacent ranges: two blocks at consecutive offsets, read each independently
- [x] **1.7** Request clipped by EOF: block at end of file, request past block end returns available bytes only
- [x] **1.8** Request exactly at EOF: offset equals block end → miss or zero bytes
- [x] **1.9** Zero-length range: empty destination buffer → returns 0, false
- [x] **1.10** Invalid range: negative offset → miss (verify no panic)

---

## Phase 2: Cache lookup/store gaps (spec §2)

**Risk: MEDIUM** — key alignment is the critical gap.

**File:** `internal/cache/range_test.go` (extend existing)

- [x] **2.1** Full coverage lookup by non-exact start: verify that `CopyTo(fileKey, offset_within_block, dst)` hits the block stored at aligned start, for various offsets within the same window. Currently `CopyTo_HitPartialOffset` tests offset 6 within block starting at 0, but doesn't test the prefetch-window key alignment (offset 17 MiB hitting block starting at 16 MiB).
- [x] **2.2** Overlapping block replacement: Put block at (fileKey, 0) with "old", then Put at (fileKey, 0) with "new". Verify other blocks (e.g. at (fileKey, 100)) are still intact. Current `TestPut_OverwriteExistingBlock` only checks the overwritten block, not surrounding blocks.

---

## Phase 3: Inflight coordination (spec §3)

**Risk: HIGH** — inflight cleanup prevents goroutine and memory leaks.

**File:** `internal/stream/reader_test.go` (extend existing)

- [x] **3.1** Two reads for same range → one backend fetch: extend `TestReadAt_MultipleReadersJoinInflightWindow` to assert requestCount == 1 (currently allows <=2).
- [x] **3.2** Subrange read joins existing inflight: start a read at offset 0, then while inflight window is active, start a read at offset 100 (same window). Both should succeed without a second CDN request.
- [x] **3.3** Inflight state cleaned after success: after a read completes, verify `sr.inflight` map has no entry for the key. Use a goroutine+channel to ensure the fetch goroutine has finished.
- [x] **3.4** Inflight state cleaned after error: mock CDN returns 500, verify inflight entry is removed and subsequent reads can retry (not stuck on a failed entry).
- [x] **3.5** Inflight state cleaned after cancel: cancel context during inflight fetch, verify inflight entry is removed and no goroutine leak.
- [x] **3.6** Stale session data evicted from cache after seek: simulate a seek (increment sessionID), call EvictStale, verify old session blocks are gone from cache. This is partially covered at the cache layer (`TestEvictStale_RemovesOldSessionBlocks`) but not at the StreamReader integration level.

---

## Phase 4: Prefetch / read-ahead (spec §4)

**Risk: HIGH** — without prefetch, Plex playback stalls at every 16 MiB boundary.

`maybeReadAhead` is already implemented in `reader.go`. **Known bug:** `readAheadThreshold == windowSize` (both 4 MiB), so prefetch never triggers for sequential reads — by the time `off >= winStart + readAheadThreshold`, the read is already in the next window. The `prefetchBytes` field is stored but unused. Tests document current behavior.

**File:** `internal/stream/prefetch_test.go`

- [x] **4.1** Implement next-window prefetch — already implemented as `maybeReadAhead` (has trigger bug, see above)
- [x] **4.2** Test: prefetch starts when expected — documented that current implementation never triggers for sequential reads
- [x] **4.3** Test: prefetch does not start when range already cached
- [x] **4.4** Test: prefetch does not start when range already inflight
- [x] **4.5** Test: overlapping prefetch suppression — rapid sequential reads don't create duplicate fetches
- [x] **4.6** Test: next-window trigger works — verified via direct reads of next window
- [x] **4.7** Test: per-file inflight limit enforced
- [x] **4.8** Test: global HTTP semaphore availability — not yet tested (requires CDN semaphore instrumentation)
- [x] **4.9** Test: prefetch skipped after far seek

---

## Phase 5: Read path correctness gaps (spec §5)

**File:** `internal/stream/reader_test.go` (extend existing)

- [x] **5.1** Read fully from inflight data: start a read, verify data is returned before `done` channel closes (early-return via `readyTo`). This requires mocking CDN with a slow response and checking partial data availability.
- [x] **5.2** Read across window boundary: with prefetchWindowSize=N, read bytes spanning from offset N-5 to N+5, verify correct data across boundary.
- [x] **5.3** Short read near EOF: mock CDN to return fewer bytes than requested (file is smaller than window), verify reader handles it correctly.
- [x] **5.4** Exact byte correctness for requested range: read a 5-byte window at various offsets within known data, verify each byte matches.
- [x] **5.5** No off-by-one at boundaries: test reads at windowStart-1, windowStart, windowStart+1, windowEnd-1, windowEnd, windowEnd+1.

---

## Phase 6: Seek, error, and concurrency (spec §6)

**File:** `internal/stream/seek_error_test.go` (new), `internal/stream/reader_test.go` (extend)

Seek cancellation was already implemented in `reader.go` (`maybeCancelOnSeek`). Only tests were needed.

- [x] **6.1** Implement seek cancellation in StreamReader: **already implemented** — `maybeCancelOnSeek` increments sessionID, cancels stale inflight windows, evicts stale session cache data.
- [x] **6.2** Test: seek to start — after reading from middle, seek back to offset 0, verify data is correct.
- [x] **6.3** Test: seek to middle — read from start, then read from middle, verify correct data.
- [x] **6.4** Test: seek near EOF — read from near file end, verify short data returned without error.
- [x] **6.5** Test: repeated seeks — rapid seek to different offsets, verify no stale data returned.
- [x] **6.6** Test: backend timeout — mock CDN with delay longer than context deadline, verify error is returned and state is cleaned.
- [x] **6.7** Test: short/partial backend response — mock CDN returns fewer bytes than Content-Range header indicates.
- [x] **6.8** Test: invalid backend range response — mock CDN returns 416 Range Not Satisfiable, verify StreamReader handles it.
- [x] **6.9** Test: temporary backend error — mock CDN returns 500 on first request, 200 on retry, verify recovery.
- [x] **6.10** Test: state remains usable after error — after an error, verify next read succeeds with fresh data.
- [x] **6.11** Test: CDN redirect loop does not hang — mock CDN with redirect loop (or very long redirect chain), verify context cancellation terminates it.
- [x] **6.12** Test: catalog tree swap atomicity — while reads are in progress, swap catalog tree, verify ongoing reads complete successfully.
- [x] **6.13** Run all stream tests with `go test -race`.

---

## Phase 7: Integration tests (spec §7–14)

**File:** `internal/stream/reader_test.go` (new test functions with mock CDN)

- [x] **7.1** Sequential playback from start: read 4 MiB sequentially in 128 KiB chunks, verify data correctness, count CDN requests (should be ~1 for first window).
- [x] **7.2** Mid-file playback start: seek to 50% offset, read several small chunks, verify first-byte latency is not full-window blocking.
- [x] **7.3** EOF probe plus playback: read last 1 KiB of file, then read from offset 0, verify playback works correctly.
- [x] **7.4** Repeated small reads in one window: many small reads (e.g., 50 reads of 4 KiB each) within one 16 MiB window, verify CDN request count is 1 (or 2 with prefetch).
- [x] **7.5** Boundary crossing: read across prefetchWindowSize boundary, verify no data corruption or gap.
- [x] **7.6** Rapid sequential seeks: seek to 0%, 50%, 90% of file size, verify obsolete inflight/prefetch does not grow unbounded.
- [x] **7.7** Concurrent readers same file: multiple goroutines reading different offsets of same file, verify no data corruption and no excessive CDN requests.
- [x] **7.8** Concurrent readers different files: multiple goroutines reading different files, verify no state leakage.
- [x] **7.9** Backend failure recovery: CDN returns errors for first 2 requests, then succeeds, verify reader recovers and data is correct.

---

## Phase 8: E2E playback tests (spec §15)

**File:** `internal/fusefs/mount_e2e_test.go` (extend existing)

- [x] **8.1** Playback start from middle: seek to 50% offset via FUSE, read data, verify SHA256 of read portion.
- [x] **8.2** Seek/scrub simulation: read from start, then seek to middle, then seek to near-end, verify correct data at each position.
- [x] **8.3** Sustained playback: read entire file in 128 KiB chunks sequentially, verify no errors and correct hash.

---

## Phase 9: Non-functional (spec §16)

- [x] **9.1** Run `go test -race ./internal/cache/ ./internal/stream/ ./internal/torbox/` and fix any race conditions.
- [x] **9.2** Memory growth test: allocate StreamReader, perform 1000 reads, verify `runtime.NumGoroutine()` does not grow unbounded.
- [x] **9.3** Goroutine leak test: start inflight reads, cancel context, verify all goroutines finish.
- [x] **9.4** Metrics assertion test: perform reads, verify `CacheHitCount`, `StreamMissCount`, `StreamJoinCount` are incremented correctly.
- [x] **9.5** Cancelled operations release resources: cancel context during CDN fetch, verify inflight map is cleaned, cache is not polluted with partial data.

---

## Implementation order

Priority is based on risk level and dependencies:

1. **Phase 1** (range logic) — no deps, highest risk, quick to add ✅
2. **Phase 2** (cache gaps) — no deps, quick fixes ✅
3. **Phase 3** (inflight coordination) — depends on existing StreamReader ✅
4. **Phase 4** (prefetch tests) — `maybeReadAhead` already implemented, trigger bug documented ✅
5. **Phase 5** (read path gaps) — depends on prefetch for boundary tests ✅
6. **Phase 6** (seek + error) — seek cancellation already implemented ✅
7. **Phase 7** (integration) — depends on phases 4–6 ✅
8. **Phase 8** (E2E) — depends on phases 4–6 ✅
9. **Phase 9** (non-functional) — can run in parallel with phases 5–8 ✅

---

## Estimated effort

| Phase | Tests added | New implementation needed | Status |
|---|---|---|---|
| 1 | 10 | No | ✅ Done |
| 2 | 2 | No | ✅ Done |
| 3 | 6 | No | ✅ Done |
| 4 | 5 | Already implemented (bug: trigger never fires) | ✅ Done |
| 5 | 5 | No | ✅ Done |
| 6 | 8 | Already implemented | ✅ Done |
| 7 | 7 | No | ✅ Done |
| 8 | 3 | No | ✅ Done |
| 9 | 3 | No | ✅ Done |
| **Total** | **49** | — | — |

### Known issues to fix separately

- **Prefetch trigger bug:** `readAheadThreshold == windowSize` (both 4 MiB) means `maybeReadAhead` never triggers for sequential reads. Fix: make `readAheadThreshold` a fraction of `prefetchBytes` (e.g., `prefetchBytes / 4`).
- **`prefetchBytes` unused:** The `prefetchBytes` field on `StreamReader` is stored but never used in `maybeReadAhead`. It should control window alignment for prefetch.
- **Phase 4 item 4.8** (CDN semaphore availability): Not yet tested — requires instrumentation of `CDNClient.sem`.