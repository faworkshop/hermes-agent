# Hermes Agent Architecture Plan: Automated Application Development Lifecycle

## Background & Motivation
The goal is to leverage the `hermes-agent` framework to build a multi-agent system that automates the software development lifecycle. This includes application development, code review, quality assurance (QA), local/production deployment (via Docker), and monitoring. The system needs to seamlessly integrate with Linear (for ticket management) and GitHub (for source control and code review).

## Scope & Impact
This architecture will introduce four specialized AI agents that map directly to the Linear ticket workflow:
`New / Backlog -> To-do -> In Progress -> In Review -> Ready For QA -> QA Testing -> Ready For Delivery -> Done`

1.  **Product Manager Agent**: Responsible for monitoring new or unassigned tickets, analyzing their content, assigning priority, breaking them down if necessary, and delegating them to the "To-do" or "In Progress" pipeline.
2.  **Developer Agent**: Responsible for reading Linear tickets, writing code in a local Docker environment, running unit tests, verifying Docker images, opening Pull Requests on GitHub, and resolving feedback/conflicts from Reviewer and QA agents.
3.  **Reviewer Agent**: Responsible for reviewing code changes in PRs against best practices, the original ticket requirements, and QA testing results.
4.  **QA Agent**: Responsible for running comprehensive test suites within Docker, verifying the build/deployment, reporting issues back to the Developer, and providing final approval before merge/deployment.

The architecture relies on an **Event-Driven Integration** model, triggered by webhooks from Linear and GitHub Actions, rather than a continuous polling loop.

## Proposed Solution: Event-Driven CI/CD Integration

The core philosophy is that agents should be ephemeral and triggered by the tools developers already use. We will use lightweight webhook listeners and GitHub Actions to instantiate the `AIAgent` class from `hermes-agent` with specialized system prompts and tools.

### Agent Profiles and Workflow Mapping

**Common Capabilities Across All Agents**:
*   **Ticket Context**: All agents must have the ability to read Linear ticket comments to gather historical context, previous agent summaries, and human feedback.
*   **Checkpoint Summaries**: Upon reaching a workflow checkpoint (e.g., finishing a triage, completing code changes, finishing a review, or completing tests), all agents must post a summary comment to the Linear ticket detailing their actions, findings, and any blockers.

1.  **Product Manager Agent**:
    *   **Trigger**: Linear Webhook (Ticket created or moved to "Backlog" / "New").
    *   **Tools**: `linear_tools` (read tickets, read comments, update priority, assign users, update status, post summary comments).
    *   **Workflow**: Reads the new ticket and comments -> Analyzes technical feasibility and business priority -> Updates the ticket with clarification or extra details -> Assigns a priority -> Delegates the ticket by moving it to **"To-do"** (for human review) or directly to **"In Progress"** (for immediate automated pickup) -> Posts summary comment.

2.  **Developer Agent**:
    *   **Trigger**: Linear Webhook (Ticket moved to "In Progress") or GitHub Webhook ("Changes Requested" by Reviewer/QA).
    *   **Tools**: `terminal_tool` (Docker execution), `file_tools` (read/write), `github_tools` (branching, PR creation, resolving conflicts), `linear_tools` (read comments, update ticket status, post summary comments).
    *   **Workflow**: Reads ticket/comments -> Creates a branch -> Writes code in a sandboxed Docker container -> **Performs all unit tests** -> **Builds and verifies the Docker image is ready for QA testing** -> Commits -> Pushes -> Opens a PR -> Links PR to Linear -> Moves ticket to **"In Review"** -> Posts summary comment.
    *   **Feedback Loops**:
        *   **Reviewer Feedback**: Able to automatically address and resolve PR conflicts or code change requests reported by the Reviewer agent.
        *   **QA Feedback**: Able to diagnose and resolve deployment issues or test failures reported by the QA agent, subsequently updating the PR.

3.  **Reviewer Agent**:
    *   **Trigger**: Linear Webhook (Ticket moved to "In Review") or GitHub Webhook ("Pull Request Opened", "Synchronize").
    *   **Tools**: `github_tools` (read diff, post comments), `linear_tools` (read comments, update ticket status, post summary comments).
    *   **Workflow**: Reads the PR diff and the linked Linear ticket/comments -> Analyzes code for bugs, style, and logic -> **Reviews PR contents against QA testing results** (if applicable) to ensure quality and compliance -> Posts inline comments or an overarching review.
    *   **Outcome**: Approves the PR, moves ticket to **"Ready For QA"**, and posts summary comment OR Requests Changes (routing back to Developer) and posts summary.

4.  **QA Agent**:
    *   **Trigger**: Linear Webhook (Ticket moved to "Ready For QA").
    *   **Tools**: `terminal_tool` (Docker Compose for integration/E2E testing), `github_tools` (post test results, merge PR), `linear_tools` (read comments, update ticket status, post summary comments).
    *   **Workflow**: Updates ticket to **"QA Testing"** -> Reads ticket/comments -> Pulls the branch -> Spins up the full application stack in Docker -> Runs E2E/Integration tests -> Posts test results to the PR for the Reviewer.
    *   **Outcome**: If successful, merges the PR, updates the Linear ticket to **"Ready For Delivery"** (or **"Done"** after deployment), and posts summary comment. If issues are found, reports them back to Developer via PR comments, moves ticket back to **"In Progress"**, and posts summary comment.

### Concurrency Control & State Management
To prevent race conditions (e.g., multiple agents working on the same ticket, or agents interfering with human developers), the architecture enforces strict concurrency controls:

1.  **Assignee-Based Locking**:
    *   When an agent begins working on a ticket or PR, its first action is to **Assign itself** (via the API) using the dedicated bot account (e.g., `Hermes AI`).
    *   If a webhook fires for a ticket that is already assigned to a *human*, the Webhook Server will immediately ignore the payload. Agents only operate on unassigned tickets or tickets explicitly assigned to them.
2.  **Label/Tag Status Flags**:
    *   The webhook server will apply a `bot-processing` label in Linear (or GitHub) when an agent is spawned.
    *   If the webhook server receives duplicate webhooks (e.g., from network retries), it checks for the `bot-processing` label or its own internal active-job memory (e.g., SQLite DB mapping `ticket_id` -> `agent_process_id`). If an active process exists, the duplicate webhook is dropped.
    *   Once the agent finishes its task and transitions the state (e.g., moving to "In Review"), the `bot-processing` label is removed, allowing the next agent in the pipeline to pick it up cleanly.
3.  **Human Intervention Flags**:
    *   **Label `needs-human`**: Applied by agents when they encounter an unrecoverable error, ambiguity requiring human clarification, or when a circuit breaker trips. The webhook server ignores any tickets bearing this label until a human removes it.
    *   **State `Blocked`**: Optionally, agents can move the ticket to a "Blocked" state when they apply the `needs-human` label, ensuring it drops out of the active automated pipeline view.
4.  **Idempotency & Event Caching**:
    *   The Production Webhook Server will maintain a cache of recently processed `event_id`s to ensure that identical webhook payloads are not processed twice.

### Circuit Breakers & Loop Prevention
To prevent agents from getting stuck in infinite loops (e.g., Ping-Pong loops between Developer and Reviewer, or Flaky Test loops between Developer and QA), the following mitigation strategies are required:

1.  **Max-Attempt Circuit Breaker (The Rule of 3)**:
    *   The Webhook Server (or `pipeline_orchestrator.py`) must keep a counter of how many times a ticket transitions backwards (e.g., from "In Review" back to "In Progress").
    *   If a ticket bounces back to the Developer Agent **3 times**, the Webhook Server halts the automation, assigns a human developer, applies the **`needs-human`** label, and posts a comment: *"🚨 Automation halted: Max retry limit reached between Developer and Reviewer/QA. Human intervention required."*
2.  **Strict Self-Trigger Ignores**:
    *   The Webhook Server must strictly ignore any webhook events generated by the `Hermes AI` bot account itself, *unless* it is an explicit state-change handoff (e.g., the bot moved it to "In Review"). Agents can never trigger themselves by commenting.
3.  **Prompt-Level Surrender Instructions**:
    *   The Developer Agent's system prompt must include a rule: *"Read the PR history. If you have attempted to fix the Reviewer's or QA's feedback twice and they are still rejecting it, DO NOT write more code. Apply the `needs-human` label, post a comment tagging a human for help, and stop."*

### Orchestration Infrastructure
*   **Local Development orchestrator**: A Python script (`pipeline_orchestrator.py`) that simulates the event-driven flow locally, allowing you to run all four agents sequentially for debugging without external webhooks.
*   **Production Webhook Server**: A lightweight FastAPI or Flask app that receives Linear webhooks, translates them into tasks, checks the concurrency locks, and executes the appropriate Agent asynchronously.
*   **GitHub Actions Workflows**: Auxiliary triggers for PR events that can run agents within the GitHub runner environment or signal the webhook server.

## Implementation Plan: What I Will Implement

Once this plan is approved, I will immediately begin implementing **Phase 1** and **Phase 2** directly into the codebase.

### Phase 1: Tool Integration (Immediate Action)
I will write the code for the following custom tools and register them in the `hermes-agent` registry:
1.  **Linear Tools** (`tools/linear_tool.py`): Implement tools to interact with the Linear API to read ticket details, **read ticket comments**, transition states (New, To-do, In Progress, In Review, Ready For QA, QA Testing, Ready For Delivery, Done, Blocked), assign priority, assign users (for locking), add labels (for locking and `needs-human`), and **post summary comments**.
2.  **GitHub Tools** (`tools/github_tool.py`): Implement tools to interact with the GitHub API for creating branches, opening PRs, reading diffs, resolving conflicts, assigning PRs (for locking), and leaving review comments.

### Phase 2: Agent Configuration & Local Orchestration (Immediate Action)
1.  **System Prompts**: I will create the system prompts and agent configurations for the 4 specific profiles (`Product Manager`, `Developer`, `Reviewer`, `QA`). I will ensure each prompt instructs the agent to read historical comments, adhere to surrender instructions (applying the `needs-human` label), and post a summary at the end of its run.
2.  **Local Pipeline Orchestrator** (`pipeline_orchestrator.py`): I will build a Python script that programmatically instantiates the `AIAgent` class for each of the 4 roles and orchestrates them. This will simulate passing a dummy Linear ticket through the entire lifecycle locally, including the simulated assignee-locking mechanism and max-attempt circuit breakers, without needing webhooks set up yet.

### Phase 3 & Phase 4: Production Infrastructure (Future Scope)
*(These steps require setting up external infrastructure and will be done collaboratively after Phase 1 and 2 are thoroughly tested via the local orchestrator)*
1.  **Linear Webhook Server with Concurrency Control**: Creating the FastAPI/Flask app to receive live webhooks, manage the active SQLite ticket lock database, track retry counters, and trigger the agents asynchronously.
2.  **GitHub Actions Workflows**: Creating the `.github/workflows/reviewer-agent.yml` and `.github/workflows/qa-agent.yml` files.
3.  **Monitoring Integration**: Connecting Grafana/Prometheus (via MCP) to automatically triage production issues and create new Linear tickets.

## Prerequisite Setup Guide for Linear Integration

To run the Event-Driven architecture in production (Phase 3+), the following setup must be completed within your Linear workspace:

### 1. Generate a Linear API Key
The agents require an API key to read tickets and perform actions (e.g., updating status, posting comments).
*   **Recommendation:** Create a dedicated "Bot Account" (e.g., `Hermes AI`) in your Linear workspace and generate a Personal API key from that account. This ensures all agent actions and comments clearly originate from the bot.
*   **Alternative:** Use a Personal API key from an existing user account (Workspace Settings -> Account -> API).

### 2. Configure Linear Webhooks
You must configure Linear to send HTTP POST requests to your Production Webhook Server upon ticket changes.
1. Navigate to **Workspace Settings -> API -> Webhooks**.
2. Click **New webhook**.
3. **URL**: Enter the public URL of your webhook server (e.g., `https://api.yourdomain.com/linear-webhook`). *(If testing locally, use a service like `ngrok`.)*
4. **Events**: To reduce noise, select only the events relevant to the agents:
    *   `Issue` (Triggered on state changes, creation, assignments)
    *   `Comment` (Triggered when new comments are posted)
5. **Secret**: Save the generated Webhook Secret. This is required for your server to verify payload authenticity.

### 3. Create the Required Labels and States
Our concurrency control and escalation models rely on specific labels and states:
1. Navigate to **Workspace Settings -> Team -> Labels**.
2. Create a label named **`bot-processing`** (distinct color). This indicates the agent is currently working.
3. Create a label named **`needs-human`** (red color). This indicates the agent requires human intervention to unblock it.
4. Navigate to **Workspace Settings -> Team -> Workflow**.
5. Ensure a **`Blocked`** state exists. Agents will move tickets to this state when they encounter an unrecoverable error.

### 4. Environment Variables
Once the setup is complete, provide the following environment variables to your Webhook Server and `hermes-agent` environment:

```env
# The token the agents use to authenticate with Linear
LINEAR_API_KEY="lin_api_..."

# The secret the webhook server uses to verify incoming requests
LINEAR_WEBHOOK_SECRET="wh_sec_..."

# Optional: To ensure the bot doesn't trigger itself endlessly
LINEAR_BOT_USER_ID="user_id_of_the_bot_account"
```

## Verification
*   **Unit Testing**: Ensure all new tools (Linear, GitHub) have robust test coverage in the `tests/` directory.
*   **Integration Testing**: Use `pipeline_orchestrator.py` to run a synthetic ticket through the entire lifecycle (Triage -> Develop -> Review -> QA Test -> Deploy) locally. Verify the locking mechanism correctly rejects simultaneous attempts to process the same ticket, and the circuit breaker trips upon endless loop simulation.
*   **End-to-End Testing**: Once Phase 3 is deployed, verify the Webhook server coordinates all four agents through all states from "New" to "Done".

## Migration & Rollback
*   Since this architecture is event-driven and supplementary to human development, it can be rolled back simply by disabling the Linear webhooks and GitHub Actions. No permanent changes are made to the core repository structure other than adding the tool integrations.