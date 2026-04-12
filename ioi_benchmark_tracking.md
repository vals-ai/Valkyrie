# IOI Benchmark Runs - 2026-03-25

## Run Summary

| # | Model | Dataset | Run ID | Status | Progress | Last Updated |
|---|-------|---------|--------|--------|----------|--------------|
| 1 | openai/gpt-5.4-2026-03-05 | ioi2024 | `5c9aad28` | COMPLETE | 6/6 (47.83%) | 11:32 PDT |
| 2 | openai/gpt-5.4-2026-03-05 | ioi2025 | `7f8724f6` | COMPLETE | 6/6 (64.33%) | 12:56 PDT |
| 3 | google/gemini-3-flash-preview | ioi2024 | `235a0079` | COMPLETE | 6/6 (36.67%) | 11:02 PDT |
| 4 | google/gemini-3-flash-preview | ioi2025 | `8426a2de` | COMPLETE | 6/6 (42.50%, 1 err) | 12:14 PDT |
| 5 | grok/grok-4.20-0309-reasoning | ioi2024 | `a91dfb2c` | STOPPED | 0/6 | 10:44 PDT |
| 6 | grok/grok-4.20-0309-reasoning | ioi2025 | `5095e28d` | STOPPED | 0/6 | 10:44 PDT |
| 7 | anthropic/claude-opus-4-5-thinking | ioi2024 | `d2be47ee` | COMPLETE | 6/6 (25.00%) | 11:59 PDT |
| 8 | anthropic/claude-opus-4-5-thinking | ioi2025 | `ea0f5f41` | COMPLETE | 6/6 (2 err, 25.33%) | 11:26 PDT |
| 9 | zai/glm-5-thinking | ioi2024 | `b4e34533` | COMPLETE (retried) | 6/6 (15.67%, 0 err) | 14:35 PDT |
| 10 | zai/glm-5-thinking | ioi2025 | `abb75365` | COMPLETE (retried) | 6/6 (14.83%, 1 err) | 14:20 PDT |

## Full Run IDs

- gpt-5.4 / ioi2024: `5c9aad28-43f0-45db-94a2-7f5098449074`
- gpt-5.4 / ioi2025: `7f8724f6-4a17-48b8-bea6-2a53c185c360`
- gemini-3-flash / ioi2024: `235a0079-a18d-4114-86fe-6bc6446674c7`
- gemini-3-flash / ioi2025: `8426a2de-9ea7-4ffc-8d01-bc7fd42b6abb`
- grok-4.20 / ioi2024: `a91dfb2c-bb78-455b-9817-85348b505b58`
- grok-4.20 / ioi2025: `5095e28d-706e-4442-bb19-9b86f0f0088c`
- claude-opus-4-5-thinking / ioi2024: `d2be47ee-cdf4-44f6-bd47-418b7e9cb8f7`
- claude-opus-4-5-thinking / ioi2025: `ea0f5f41-4ce1-47b0-a6ad-77fb77f5dc92`
- glm-5-thinking / ioi2024: `b4e34533-9b54-4e10-a498-e234e367133d`
- glm-5-thinking / ioi2025: `abb75365-a886-4807-994f-4b43b0967f98`

## Monitoring Log

### 10:39 PDT - Runs Launched
All 10 runs started successfully. Each run has 6 tasks with concurrency 6.

### 10:42 PDT - CloudWatch Log Check
All 10 runs confirmed healthy in CloudWatch. Dependencies installing correctly (uv, model-proxy, ioi-agent). No errors detected. Gemini-3-flash leading at 1/6 on both datasets. All other runs at 0/6 (still bootstrapping).

### 10:44 PDT - Grok Runs Stopped
Both grok/grok-4.20-0309-reasoning runs force stopped per user request.

### 10:47 PDT - Monitoring Cycle 1
- gemini-3-flash: 5/6 on both datasets (fastest)
- gpt-5.4: 1/6 on ioi2024, 0/6 on ioi2025
- claude-opus-4-5-thinking: 0/6 on both
- glm-5-thinking: 0/6 on both

### 10:50 PDT - Monitoring Cycle 2
No changes from previous cycle. All runs still in progress, no errors.

### 10:53 PDT - Monitoring Cycle 3
- gemini-3-flash: 5/6 on both (last task still running)
- gpt-5.4: 1/6 on both datasets now
- claude-opus-4-5-thinking: 0/6 on both (IOI tasks are long-running)
- glm-5-thinking: 0/6 on both

### 10:56 PDT - Monitoring Cycle 4
No changes. All runs stable, no errors.

### 10:59 PDT - Monitoring Cycle 5
- gemini-3-flash / ioi2024: COMPLETE (6/6)
- gemini-3-flash / ioi2025: 5/6 (last task still running)
- gpt-5.4: 1/6 on both
- claude-opus-4-5-thinking: 0/6 on both
- glm-5-thinking: 0/6 on both

### 11:02 PDT - Monitoring Cycle 6 + Results Download
**gemini-3-flash / ioi2024 results:**
- Platform Run ID: `17c72afe-0591-4390-bf94-79f7e9c58f21`
- IOI Accuracy: **36.67%**
- Average Cost: $0.28/task
- Average Duration: 603.9s

### 11:05 PDT - Monitoring Cycle 7
No changes. gemini ioi2025 still on last task. glm-5 still at 0/6 on both.

### 11:08 PDT - Monitoring Cycle 8
- claude-opus-4-5 / ioi2025: 0/6 -> 1/6
- glm-5 / ioi2024: 0/6 -> 1/6
- All other active runs steady. gemini ioi2025 still 5/6.

### 11:11 PDT - Monitoring Cycle 9
- gpt-5.4 / ioi2024: 1/6 -> 2/6
- claude-opus-4-5 / ioi2024: 1/6 -> 2/6
- claude-opus-4-5 / ioi2025: 1/6 -> 3/6 (fast progress)
- gemini ioi2025 still 5/6 on that stubborn last task

### 11:14 PDT - Monitoring Cycle 10
- claude-opus-4-5 / ioi2024: 2/6 -> 4/6
- claude-opus-4-5 / ioi2025: 3/6 -> 5/6 (1 error: task `2025/migrations` hit 200k token context limit -- MaxContextWindowExceededError, 201283 > 200000)
- gemini ioi2025 still 5/6

### 11:17 PDT - Monitoring Cycle 11
- gpt-5.4 / ioi2024: 2/6 -> 5/6 (big jump)
- gpt-5.4 / ioi2025: 1/6 -> 2/6
- claude-opus-4-5 / ioi2024: 4/6 -> 5/6
- claude-opus-4-5 / ioi2025: 5/6 (1 err, 1 remaining)
- gemini ioi2025: still 5/6
- glm-5: 1/6 on ioi2024, 0/6 on ioi2025

### 11:20 PDT - Monitoring Cycle 12
No changes from previous cycle.

### 11:23 PDT - Monitoring Cycle 13
- gpt-5.4 / ioi2025: 2/6 -> 3/6
- 4 runs stuck on last task (gpt-5.4 ioi2024, gemini ioi2025, claude-opus ioi2024/ioi2025)
- glm-5 still slow: 1/6 and 0/6

### 11:26 PDT - Monitoring Cycle 14
- claude-opus-4-5 / ioi2025: COMPLETE (6/6, 2 errors)
  - Platform Run ID: `43656ab4-328b-44ea-bef5-d69672788084`
  - IOI Accuracy: **25.33%**
  - Average Cost: $4.86/task
  - Average Duration: 1630.6s
  - 2 tasks errored (context window exceeded)
- gpt-5.4 ioi2024 still 5/6, gemini ioi2025 still 5/6, claude-opus ioi2024 still 5/6

### 11:32 PDT - Monitoring Cycle 16
- gpt-5.4 / ioi2024: COMPLETE (6/6)
  - Platform Run ID: `49eb5566-72af-4783-a9ae-912eb12a4a8d`
  - IOI Accuracy: **47.83%**
  - Average Cost: $2.84/task
  - Average Duration: 2611.5s
- gpt-5.4 / ioi2025: 4/6 -> 5/6
- glm-5 / ioi2025: 0/6 -> 1/6

### 11:41 - 11:56 PDT - Monitoring Cycles 17-24
- glm-5 ioi2024: 1/6 -> 2/6, glm-5 ioi2025: 0/6 -> 3/6
- Three runs (gpt-5.4 ioi2025, gemini ioi2025, claude-opus ioi2024) stuck at 5/6 on their last task for 20+ minutes -- likely the hardest IOI problem in each set

### 11:59 PDT - Monitoring Cycle 25
- claude-opus-4-5 / ioi2024: COMPLETE (6/6, no errors)
  - Platform Run ID: `d7ffb442-8a8c-485d-893f-6b92a83f4022`
  - IOI Accuracy: **25.00%**
  - Average Cost: $7.01/task
  - Average Duration: 3085.0s
- gpt-5.4 ioi2025 and gemini ioi2025 still at 5/6

### 12:14 PDT - Monitoring Cycle 30
- gemini-3-flash / ioi2025: COMPLETE (6/6, 1 error)
  - Platform Run ID: `8aadb422-f5eb-4ef7-b077-82e9e2008433`
  - IOI Accuracy: **42.50%**
  - Average Cost: $0.26/task
  - Average Duration: 433.2s
- gpt-5.4 ioi2025 still 5/6
- glm-5 ioi2024: 2/6, glm-5 ioi2025: 5/6 (2 err)

### Error Summary (all runs)
All errors are **context window exceeded** failures on the hardest/longest problems:
| Run | Task | Error |
|-----|------|-------|
| claude-opus-4-5 / ioi2025 | `2025/migrations` | prompt 201,283 > 200,000 token limit |
| gemini-3-flash / ioi2025 | `2025/migrations` | Empty response (context overflow) |
| glm-5 / ioi2024 | `2024/message` | `model_context_window_exceeded` |
| glm-5 / ioi2024 | `2024/sphinx` | `model_context_window_exceeded` |
| glm-5 / ioi2025 | `2025/migrations` | `model_context_window_exceeded` |
| glm-5 / ioi2025 | `2025/worldmap` | `model_context_window_exceeded` |

`2025/migrations` is the worst offender -- failed on 3 different models.

### 12:41 PDT - Monitoring Cycle 42
- glm-5 / ioi2024: COMPLETE (6/6, 4 errors -- all context window)
  - Platform Run ID: `aa998429-075c-43b1-8236-09858256ba8e`
  - IOI Accuracy: **11.83%** (only 2 of 6 tasks succeeded)
  - Failed tasks: `2024/message`, `2024/sphinx`, `2024/hieroglyphs`, `2024/tree`
  - Average Cost: $0.41/task, Average Duration: 1158.4s
- All errors across all runs are context window related -- nothing to retry

### 14:35 PDT - GLM-5 Retries Complete
Retried all errored tasks. Context window errors were non-deterministic (reasoning token usage varies per attempt).
- glm-5 / ioi2024: 11.83% -> **15.67%** (all 4 errors recovered, 0 errors now)
  - New Platform Run ID: `7a624a89-3e78-4153-8f29-1487078fed78`
- glm-5 / ioi2025: 11.50% -> **14.83%** (1 of 2 errors recovered, 1 error remains)
  - New Platform Run ID: `b586c52b-23a5-4d1e-bb9e-2c33df95916c`
- GLM-5 average: 15.25% (vs 22.00% reference -- closer but still below)

### 12:56 PDT - Monitoring Cycle ~47
- gpt-5.4 / ioi2025: COMPLETE (6/6, no errors)
  - Platform Run ID: `2e203e1f-e53b-40e2-9dd8-f17332b4490b`
  - IOI Accuracy: **64.33%** (highest score across all runs)
  - Average Cost: $3.55/task
  - Average Duration: 4106.6s
- Only glm-5 ioi2025 remaining (5/6, 2 err)

### 13:09 PDT - ALL RUNS COMPLETE
- glm-5 / ioi2025: COMPLETE (6/6, 2 errors)
  - Platform Run ID: `86ec2f8e-2f8d-4d6b-83af-8037666a522d`
  - IOI Accuracy: **11.50%**
  - Average Cost: $1.03/task
  - Average Duration: 4402.8s

## Final Results Summary

| Model | ioi2024 Accuracy | ioi2024 Platform Run ID | ioi2025 Accuracy | ioi2025 Platform Run ID |
|-------|-----------------|------------------------|-----------------|------------------------|
| openai/gpt-5.4-2026-03-05 | **47.83%** | `49eb5566-72af-4783-a9ae-912eb12a4a8d` | **64.33%** | `2e203e1f-e53b-40e2-9dd8-f17332b4490b` |
| google/gemini-3-flash-preview | **36.67%** | `17c72afe-0591-4390-bf94-79f7e9c58f21` | **42.50%** | `8aadb422-f5eb-4ef7-b077-82e9e2008433` |
| anthropic/claude-opus-4-5-thinking | **25.00%** | `d7ffb442-8a8c-485d-893f-6b92a83f4022` | **25.33%** | `43656ab4-328b-44ea-bef5-d69672788084` |
| zai/glm-5-thinking | **15.67%** (retry) | `7a624a89-3e78-4153-8f29-1487078fed78` | **14.83%** (retry) | `b586c52b-23a5-4d1e-bb9e-2c33df95916c` |
| grok/grok-4.20-0309-reasoning | STOPPED | - | STOPPED | - |
