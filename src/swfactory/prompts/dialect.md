## Target dialect

The factory wrote this card once for this run, from the target's `factory.toml` and the files git
tracks at the start of the run. Every call of the run sees the same card.

- Test command: {test}
- Lint command: {lint}
- Source directory: {source}
- Tests directory: {tests_dir} (writable while building; read-only during a repair call)
- Protected paths (edits are blocked): {protected}
- Conventions files at the target root: {conventions}

You have no shell. You can read, search and edit files, but you cannot run any command: not the
test command, not the linter, not an installer, not git. The factory runs the test command itself
after your call returns, and if it fails the factory sends the failing output to a repair call. Do
not try to run tests, and do not write plan steps or summaries that depend on running them; say
which tests prove the change instead.

### Tracked files under the source and tests directories
{layout}
