# Opt-in multi-turn evaluation

`valk run start ... --multi-turn`, SDK `runs.start(..., multi_turn=True)`, or API
`multi_turn: true` enables a conversation for an evaluation run. Omitted/false
uses the existing single-turn path, even if the agent declares conversation
capability. No task generation change, special benchmark name, or automatic
workflow opt-in is required. The flag is saved with the run's JSON arguments and
preserved when execution requests are reconstructed; no database migration is needed.

The benchmark service must implement `POST /conversation/turn` and honor the
internal `x-valkyrie-conversation: valkyrie.conversation.v1` setup header by
withholding its full task specification. Tracker sends that header on both HTTP
and WebSocket calls only when the run opts in. ValSmith implements this in its
ordinary service. Other services need an adapter before this mode can be used.

The agent bundle must declare `conversation` capability (protocol
`valkyrie.conversation.v1`, defaults: three agent turns, 1800 seconds). Tracker
checks the resolved bundle before admission and rejects unsupported agents.
Declaring capability is not an activation switch. Pi needs an adapter; existing
Pi bundles must not be marked capable without implementing this protocol.

Tracker keeps one sandbox, writes only the next user message to the problem
file and visible history to `/workspace/conversation-input.json`, and invokes
the agent's run command for each turn. The agent persists its own session/tool
history and writes `{ "turn": 0, "message": "clarifying question" }` to
`/workspace/conversation-output.json` before returning. This version uses process
invocations, not a live Pi RPC connection. The example agent in ValSmith is only
a protocol canary; its live model execution is not yet proven end-to-end.

The simulator decides the task appears complete from the assistant's replies
and outstanding requirements, without consulting test results or requiring a
completion keyword. It returns a user message or stop; Tracker enforces turn order, stable
policy, message limits and a shared wall-clock deadline. Stop or turn-limit
completion leads to the ordinary final grader, not an automatic pass. There is
no intermediate grading, sandbox copying, optimizer, or hidden-test feedback.
Conversation requests, user/assistant messages, terminal disposition and errors
are saved with task artifacts. A started receipt prevents uncertain replay;
multi-turn crash/retry recovery is intentionally fail-closed in v1.

Merge/deployment dependencies: the Tracker/SDK/CLI change and service adapter
must both be available before opting in. Ordinary runs do not require the
simulator gateway or ledger. No shared service has been deployed by this PR.
