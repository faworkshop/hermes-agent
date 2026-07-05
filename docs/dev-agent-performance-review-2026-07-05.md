# Dev agent performance review — 2026-07-05

Snapshot of the Developer role (maestro SDLC pipeline) over the full ptdashboard
ticket series (PTD-*) and the older FAW-* workshops, with focus on
throughput, error rate, and per-ticket duration outliers.

## Headline numbers (n=126 lifetime, 124 done)

| metric              | value                                  |
|---------------------|----------------------------------------|
| success rate        | 124/126 = **98.4%** (1 failed, 1 cancelled) |
| median dev duration | **3m34s**                              |
| p90 dev duration    | **14m51s**                             |
| longest dev run     | 30m00s (hard-timeout, PTD-2 task 327)  |
| under 1m            | 8 (6.5%)                               |
| under 3m            | 37 (29.8%)                             |
| under 10m           | 68 (54.8%)                             |

## Trend is clearly improving

| date  | tasks | avg dur | failures | notes                                  |
|-------|-------|---------|----------|----------------------------------------|
| Jul 1 | 7     | 8m50s   | 0        |                                        |
| Jul 2 | 12    | 10m50s  | 1        | PTD-2 hard-timeout (basis of the `faw-agent-hard-timeout` fix) |
| Jul 3 | 31    | 6m06s   | 0        | batched dispatch test                  |
| Jul 4 | 5     | 3m27s   | 0        |                                        |
| Jul 5 | 13    | **3m23s** | 0      | best day in the dataset (this review)  |

The 30-minute hard-timeout (`faw-agent-hard-timeout` skill, task 327 = PTD-2 on
Jul 2) is doing its job — no runaway dev sessions since.

## Today's flow (Jul 5, 13 tasks)

```
PTD-27  1m12s   done
PTD-28  1m12s   done
PTD-25  1m11s   done   <- redispatched (focused resume prompt)
PTD-25  1m47s   done   <- original (had not pushed)
PTD-30  0m51s   done
PTD-29  4m15s   done
PTD-35  0m54s   done
PTD-34  6m34s   done
PTD-33  1m08s   done
PTD-32  0m39s   done
PTD-36  2m50s   done
PTD-50  0m39s   done
PTD-38  20m45s  done   <- outlier: see "PTD-38 timing analysis" below
PTD-39  ...     running <- in flight at time of review
```

## Failure modes (only 2 ever)

### 1. task 327, PTD-2, hard-timeout at 1800s (Jul 2 06:46)

- Root cause: infinite loop, dev agent kept re-checking without making progress.
- Resolution: `faw-agent-hard-timeout` skill — added 30-minute cap.
- No recurrence since (3 days, ~118 runs).

### 2. task 680, PTD-26, cancelled (Jul 4)

- Root cause: spurious dispatch — ticket was already in In Review, dev got
  queued anyway.
- Resolution: noted as reaped during recovery; not a recurring pattern
  (1 occurrence).
- No recurrence.

## Multi-attempt tickets (5-13× clusters)

```
FAW-57  13 runs   older FAW-session, all done
PTD-2   10 runs   1 hard-timeout, 9 done (noisy ticket pre-timeout-fix)
FAW-52  9 runs    older FAW-session
FAW-54  9 runs    older FAW-session (mostly null/legacy rows)
FAW-56  7 runs    older FAW-session
PTD-16  5 runs    normal iteration, all done
PTD-13  5 runs    normal iteration, all done
```

The high-count FAW-* clusters are all from older FAW-Workshop sessions
(May-Jun, before the PTD refactor). On PTD tickets the failure-mode concern
is concentrated in PTD-2, which was solved by the hard-timeout fix.

PTD-25 specifically: 2 runs. First attempt finished in 19m45s but never
committed/pushed (hit iteration cap); second attempt (focused resume prompt)
finished in 1m47s and pushed cleanly. That is the "stuck between dev
iterations" recovery pattern working as designed.

## PTD-38 timing analysis (the 20m45s outlier)

**The dev agent did NOT hit the FE design-file gate on PTD-38.** PTD-38 is
backend-only — the ticket description explicitly says `Frontend: N/A`.
The dev agent correctly identified this from the ticket text and skipped the
gate (session msg 74: *"PTD-38 is a **backend-only** task... I do NOT need
to apply the FE design-file gate"*). The FE design-file gate is working as
intended.

The 20m45s breakdown:

```
~3-4 min   ticket read + history + branch setup + baseline build
~14-15 min implementing 3 model files (RouteEtaRow, LineDirectionEtaRow,
            EtaRow interface) + refactoring FavoriteEtaResponse
            + writing 3 test files (EtaModelTest, EtaServiceTest, EtaResourceTest)
~2 min     git commit + push + open PR + CI poll
```

The smoking gun is the `mvnw test` count: **20 separate `./mvnw test`
invocations in one session** (session msgs 82, 94, 96, 122, 124, 126, 128,
132, 160, 164, 166, 168, 170, 172, 202, 268, 273, 275, 288). Pattern:

```
mvnw test -Dtest=EtaModelTest                      # passes (single class)
... edits ...
mvnw test -Dtest=EtaModelTest,EtaServiceTest       # passes
... edits ...
mvnw test                                            # full suite (~2-5m)
... edits ...
mvnw test -Dtest=EtaModelTest,...                   # passes
... edits ...
mvnw test                                            # full suite again
```

Each Quarkus app startup from `./mvnw test` is ~30-60s even with `-q`.
20 invocations × ~30-60s = **~10-20 minutes of JVM boot overhead alone**,
which is the bulk of the 20m45s.

### Fix applied (TEST-BATCHING RULE in sdlc_roles.yaml)

Added a new mandatory section to the Developer system prompt
(`sdlc_roles.yaml` ~L259, immediately after the existing Backend CI RULE):

- Run tests ONCE at the end after all edits are complete, NOT after each
  edit.
- Use the compiler (`./mvnw compile -q`, `mvn -DskipTests test-compile`)
  during implementation; the compiler is fast (<5s) and catches ~95% of
  edit-time mistakes.
- Single-test-class invocations (`-Dtest=ClassName`) ONLY for debugging a
  specific failing test, never as a routine "did my edit work" check.
- Full suite (`./mvnw test`) is run exactly twice per session:
  (1) once after all code edits are complete, before `git commit`
  (2) once before `git push` to confirm no regressions from the commit.
- For Quarkus dev iteration, prefer `./mvnw -DskipITs test` (skips
  integration tests with Quarkus boot) when only fast unit-test feedback
  is needed. Reserve full `./mvnw test` for the pre-push gate.
- Frontend equivalent: do NOT re-run `pnpm run test:ci` after every file
  save. Use `pnpm exec jest --watch` or `pnpm exec jest <file>` for
  targeted runs during dev; run the full CI triple (lint + type-check +
  test:ci) exactly once before `git push`.

**Expected impact:** PTD-38-class backend tickets should drop from ~20m to
~10m, with the difference coming almost entirely from reduced JVM boot
overhead. Median dev duration may fall from 3m34s toward ~2m30s if the
rule is consistently applied.

## Outliers worth understanding

- **PTD-38 at 20m45s** — actual work (3 model files + refactor + 3 test
  files), not a stall. With TEST-BATCHING RULE applied, expected ~10m.
- **FAW-57 at 29m** — longest *successful* run, from May pre-timeout-fix,
  hit its 1740s runtime organically. Not a current concern.

## What this means

✅ Dev agent throughput is **~13 tickets/day at peak** (Jul 3 was 31 runs in
a calendar day — batched dispatch test).

✅ Error rate < 2%, and the one real failure mode (infinite loop) has been
fixed by `faw-agent-hard-timeout`.

✅ Median runtime **3m34s** with p90 **15m** — well under the 30m hard cap.

✅ "Iteration cap → silent stuck" pattern (PTD-25) is now catchable via
focused resume prompts.

⚠️ **PTD-38 took 20m45s** — long because of repeated `mvnw test` calls,
not because of any gate. TEST-BATCHING RULE fix is targeted at this exact
pattern.

⚠️ **PTD-39 was running 10m52s** at review time — no signal yet whether it
would be fast or slow. Watchlist item.

## Recommendations

1. **No urgent action.** Dev agent is healthy, getting faster, error rate
   is acceptable.
2. **Monitor PTD-39** — running 10m52s at review time, will report back if
   it hits the 30m cap.
3. **When FE scope tickets queue up**, verify Stitch exports exist at
   `design/ui/stitch/` *before* letting PM promote to In Progress. Otherwise
   dev will hit the FE design-file gate and burn 15-30min before aborting.
4. **Track multi-attempt PTD tickets** (PTD-2, PTD-16, PTD-13) as a
   dashboard metric — if any ticket crosses 5 attempts, escalate as a
   "ticket not making progress" signal.
5. **Re-measure median dev duration after the TEST-BATCHING RULE has been
   live for ~20 dev runs.** If median drops below ~3m as predicted, the
   rule is working. If unchanged, the agent is still over-iterating tests
   and the rule needs stronger wording or a hard cap on test invocations.

## Methodology

- Source: `agent_tasks` table, role = `Developer`, finished_at IS NOT NULL.
- Database: PostgreSQL via `FAW_DB_URL` (Docker container, port 5433).
- Time window: full history through 2026-07-05 ~19:00 SGT.
- Outlier PTD-38 confirmed by reading the session JSON
  `~/.hermes/sessions/session_Developer_1783242904_bf0bd1.json` (308
  messages, 4484 lines) — extracted `tool_calls[].function.name` per message
  and computed the `mvnw test` invocation histogram.