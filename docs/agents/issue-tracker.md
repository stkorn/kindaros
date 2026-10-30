# Issue tracker: GitHub

Issues and specs live in GitHub Issues:
https://github.com/stkorn/kindaros

Use gh with --repo stkorn/kindaros explicitly, including when
the workspace has no Git remote.

## Operations

- Create: gh issue create --repo stkorn/kindaros --title "..." --body-file <path>
- Read: gh issue view <number> --repo stkorn/kindaros --comments
- List: gh issue list --repo stkorn/kindaros --state open
- Comment: gh issue comment <number> --repo stkorn/kindaros --body-file <path>
- Labels: gh issue edit <number> --repo stkorn/kindaros --add-label "<label>"
- Close: gh issue close <number> --repo stkorn/kindaros

Write multiline content to a temporary file and use --body-file.
Use docs/agents/triage-labels.md for triage label names.

## Pull requests as a triage surface

PRs as a request surface: no.

## Skill conventions

“Publish to the issue tracker” means create a GitHub issue.
“Fetch the relevant ticket” means read the issue and its comments.

## Wayfinding

Keep the effort map in one issue and link child tickets as sub-issues.
If sub-issues are unavailable, use a task list and a Part of #<map> reference.
Use native issue dependencies where available; otherwise record
Blocked by: #<number> near the top of each child ticket.

Claim an unblocked child by assigning it before starting work.
Resolve it by recording the answer, closing it, and updating the map.
