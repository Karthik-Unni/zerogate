# 🛑 Strict Maintainer Review Checklist

Before opening this Pull Request, you must check and confirm every single requirement below. Non-compliant PRs will be closed immediately without review.

- [ ] **Issue Alignment:** I have commented on an open tracking Issue, and the maintainers have explicitly approved and assigned this work to me. (Link to Issue: #)
- [ ] **Natively Async:** Every added network, database, or cache operation is strictly asynchronous.
- [ ] **No Vendor SDK Bloat:** This PR does not modify `requirements.txt` to add proprietary cloud packages (`runpod`, `boto3`, etc.).
- [ ] **Centralized Configuration:** No new variables have been injected into `.env`. All presets are isolated within `configs.py`.
- [ ] **Local Verification Logs:** I have attached passing local simulation traces (`ZEROGATE_MOCK=True`) below.

## Summary of Changes

Provide a clean summary of the technical subtraction, isolation, or feature addition.

## Local Simulation Logs / Terminal Output

```text
// Paste your passing local worker daemon terminal print streams here
```
