# 📊 DETAILED LOG ANALYSIS: Container App Data Ingestion

**Date**: April 4, 2026  
**Version Deployed**: `v2.0.6`  
**Environment**: Azure Container Apps  

---

## Executive Summary

**Critical Finding**: Version v2.0.6 has a **single-retry limitation** on 429 errors. When embedding generation fails due to rate limiting, the `get_embeddings()` function retries only **ONCE** and then raises the exception. This caused **repeated re-indexing loops** where the same document kept failing and being retried across multiple indexer runs.

**All 6 files were eventually indexed successfully**, but the WV_West Virginia file took **~3 hours** due to repeated failures and re-processing cycles.

---

## 🔴 The Root Cause: v2.0.6 Single-Retry Bug

### What Happens in v2.0.6

```python
# v2.0.6 get_embeddings() - PROBLEMATIC CODE
def get_embeddings(self, text: str, retry_after: bool = True) -> list:
    try:
        resp = self.client.embeddings.create(...)
        return resp.data[0].embedding

    except openai.RateLimitError as e:
        retry_hdr = getattr(e, "headers", {}).get("retry-after")
        if retry_after and retry_hdr:  # Only if header exists
            wait = float(retry_hdr)
            time.sleep(wait)
            return self.get_embeddings(text, retry_after=False)  # ⚠️ ONLY ONE RETRY!
        logging.error(f"RateLimitError in get_embeddings: {e}")
        raise  # ❌ Fails after single retry
```

### The Problem

1. **Single retry only**: `retry_after=False` on the second call means NO MORE RETRIES
2. **No exponential backoff**: Waits exactly what the server says, no buffer
3. **No jitter**: Multiple concurrent requests all retry at the same time, causing more 429s
4. **Header dependency**: If `retry-after` header is missing, doesn't retry at all

---

## 📁 File-by-File Analysis

### ✅ 1. GA_ABD_Application_Processing.pdf
| Metric | Value |
|--------|-------|
| Start Time | 2026-04-03 19:40:00 |
| Document Intelligence Calls | **1** |
| Chunks Generated | **331** |
| Completion Time | 19:43:45 |
| Total Duration | **224 seconds (3.7 min)** |
| Errors | 0 |

### ✅ 2. GA_ABD_Financial_Responsibility.pdf
| Metric | Value |
|--------|-------|
| Start Time | 2026-04-03 19:43:49 |
| Document Intelligence Calls | **2** |
| Chunks Generated | **331** |
| Completion Time | 19:48:44 |
| Total Duration | **183 seconds (3 min)** |
| Errors | 0 |

### ✅ 3. GA_ABD_Medicaid_Resources.pdf
| Metric | Value |
|--------|-------|
| Start Time | 2026-04-03 19:48:48 |
| Document Intelligence Calls | **2** |
| Chunks Generated | **331** |
| Completion Time | 19:53:37 |
| Total Duration | **209 seconds (3.5 min)** |
| Errors | 0 |

### ✅ 4. GA_Medicaid_Renewals.pdf
| Metric | Value |
|--------|-------|
| Start Time | 2026-04-03 19:53:42 |
| Document Intelligence Calls | **4** |
| Chunks Generated | **331** |
| Completion Time | 19:58:57 |
| Total Duration | **150 seconds (2.5 min)** |
| Errors | 0 |

### ✅ 5. PA_Resources_Chapter_178.pdf (smallest file)
| Metric | Value |
|--------|-------|
| Start Time | 2026-04-03 19:59:01 |
| Document Intelligence Calls | **2** |
| Chunks Generated | **25** |
| Completion Time | 19:59:48 |
| Total Duration | **22 seconds** |
| Errors | 0 |

### ⚠️ 6. WV_West Virginia_Binder4 - Effective 1-1-26.pdf (THE PROBLEM FILE)
| Metric | Value |
|--------|-------|
| Start Time | 2026-04-03 19:59:49 |
| Document Intelligence Calls | **56 (!)** |
| Failed Attempts | **2** (with 0 chunks, 1 error each) |
| Final Chunks Generated | **792** |
| Final Completion Time | 23:01:31 |
| **Total Duration** | **~3 HOURS** |
| 429 Errors Logged | **33** |

---

## 🔍 Deep Dive: What Happened to the WV File

### Timeline of the WV File Processing

```
19:59:49 - First attempt started
         Document Intelligence begins analyzing (~33 MB base64 payload)
         
20:xx:xx → 21:xx:xx - Multiple Document Intelligence retries
         56 POST -> 202 calls made (document analysis submissions)
         
21:35:02 → 21:36:14 - First major 429 error cluster
         33 rate limit errors in quick succession
         v2.0.6 retry logic fails after single retry
         
22:48:55 - First complete failure
         "Finished chunking in 250.77 seconds. 0 chunks. 1 errors."
         
         ERROR: RateLimitError in get_embeddings: Error code: 429 - 
         {'error': {'code': 'RateLimitReached', 'message': 'Your requests 
         to text-embedding for text-embedding-3-large in Sweden Central 
         have exceeded the call rate limit... Please retry after 1 second.'}}

22:57:52 - Second complete failure  
         "Finished chunking in 214.65 seconds. 0 chunks. 1 errors."
         
         Same 429 error - single retry exhausted

23:01:31 - FINALLY SUCCESS!
         "Finished chunking in 390.19 seconds. 792 chunks. 0 errors."
```

### Why 56 Document Intelligence Calls?

The WV file is very large (~33 MB). Each time the file failed during **embedding generation**, the indexer marked it as a failed candidate. On the next cron cycle (every 5 minutes), the indexer:

1. Detected the file as still needing processing
2. Restarted from scratch (including Document Intelligence analysis)
3. Sent a **new** POST to Document Intelligence
4. Eventually hit 429 on embeddings again
5. Repeated...

This caused **56 Document Intelligence API calls** for a single file - wasting both time and API quota.

### The Actual Error

```json
{
  "error": {
    "code": "RateLimitReached",
    "message": "Your requests to text-embedding for text-embedding-3-large in Sweden Central 
               have exceeded the call rate limit for your current AIServices S0 pricing tier. 
               This request was for Embeddings_Create under Azure OpenAI API version 2024-10-21. 
               Please retry after 1 second."
  }
}
```

**The irony**: The server said "retry after 1 second" - but v2.0.6 code only retries **once**. After that single retry, if it hits another 429, the entire document processing fails.

---

## 📈 How Much Better Would v2.2.1 Be?

### v2.0.6 (Your Version) vs v2.2.1 (Fixed Version)

| Feature | v2.0.6 | v2.2.1 |
|---------|--------|--------|
| Max Retry Attempts | **1** | **20** |
| Exponential Backoff | ❌ No | ✅ Yes (1s → 60s) |
| Jitter | ❌ No | ✅ Yes (0.5s random) |
| Respects Retry-After Header | Partially | ✅ Fully |
| Handles Missing Header | ❌ Fails | ✅ Uses backoff |
| Logging | Basic | ✅ Detailed retry logging |

### v2.2.1 Retry Logic

```python
# v2.2.1 get_embeddings() - ROBUST RETRY
def get_embeddings(self, text: str, retry_after: bool = True) -> list:
    attempt = 0
    while True:
        try:
            resp = self.client.embeddings.create(...)
            return resp.data[0].embedding

        except openai.RateLimitError as e:
            if not retry_after or attempt >= self.retry_max_attempts:  # 20 attempts!
                raise
            ra = self._extract_retry_after_seconds(e)
            self._sleep_for_retry(attempt=attempt, retry_after=ra, op_name="embeddings")
            attempt += 1
            continue  # ✅ Keep trying!
```

### Estimated Time Savings with v2.2.1

| Metric | v2.0.6 (Actual) | v2.2.1 (Estimated) |
|--------|-----------------|-------------------|
| WV File Processing Time | ~3 hours | ~10-15 minutes |
| Document Intelligence Calls | 56 | 1-2 |
| 429 Errors Before Success | 33+ | 0-5 (handled internally) |
| Failed Indexer Runs | Multiple | 0 |

**With v2.2.1, the WV file would have succeeded on the first attempt**, taking approximately:
- ~2 min for Document Intelligence analysis
- ~5-10 min for embedding generation (with internal retries on 429s)
- Total: **~10-15 minutes** instead of 3 hours

---

## 💡 What About v2.1.0?

**v2.1.0 does NOT fix the retry issue.** The changes between v2.0.6 and v2.1.0 are:

- SharePoint Lists integration
- Blob indexing fix
- Minor configuration changes

The embedding retry logic in v2.1.0 is **identical** to v2.0.6 - still only single retry.

**The fix was introduced in v2.2.1.**

---

## 📊 Summary Statistics

| File | Doc Int Calls | Chunks | Processing Time | Status |
|------|---------------|--------|-----------------|--------|
| GA_ABD_Application_Processing | 1 | 331 | 3.7 min | ✅ Success |
| GA_ABD_Financial_Responsibility | 2 | 331 | 3 min | ✅ Success |
| GA_ABD_Medicaid_Resources | 2 | 331 | 3.5 min | ✅ Success |
| GA_Medicaid_Renewals | 4 | 331 | 2.5 min | ✅ Success |
| PA_Resources_Chapter_178 | 2 | 25 | 22 sec | ✅ Success |
| WV_West Virginia_Binder4 | **56** | 792 | **~3 hours** | ✅ Success (eventually) |

**Total Chunks Indexed**: 2,141

---

## 🎯 Recommendations

### Immediate Actions

1. **Upgrade to v2.2.1 or later** - This fixes the retry logic
2. **Keep INDEXER_MAX_CONCURRENCY=1** - Good for stability with your current TPM
3. **Keep the current memory (4Gi)** - Sufficient for large documents

### Why Your Configuration Was Correct

Your settings were actually quite good:
- **2 CPU / 4 Gi memory**: Prevents OOM crashes
- **INDEXER_MAX_CONCURRENCY=1**: Reduces burst TPM demand
- **300K TPM embedding**: Sufficient quota

The only issue was the **v2.0.6 single-retry bug** - your settings couldn't compensate for that code limitation.

### After Upgrading to v2.2.1+

You can consider:
- Increasing `INDEXER_MAX_CONCURRENCY` to 2-4 for faster processing
- The robust retry logic will handle 429s gracefully
- Large files like WV_West Virginia will process in ~10-15 min instead of 3 hours

---

## 🔑 Key Takeaways

1. **The 40+ Document Intelligence calls were NOT normal** - They were caused by the single-retry bug forcing repeated re-processing
2. **The actual error was 429 on embeddings**, not Document Intelligence
3. **v2.1.0 would NOT have helped** - Same retry bug exists
4. **v2.2.1 fixes the issue** with robust 20-attempt retry logic with exponential backoff
5. **All files were successfully indexed** - The system eventually recovered, but took much longer than necessary

---

## Appendix: Version Comparison

### Changes v2.0.6 → v2.1.0
- SharePoint Lists integration
- Blob indexing fix
- **No retry logic changes**

### Changes v2.1.0 → v2.2.0
- RBAC support

### Changes v2.2.0 → v2.2.1
- **Fix Excel ingestion**
- **Robust retry logic with 20 attempts + exponential backoff** ← This is what you need!

---

*Analysis generated on April 4, 2026*
