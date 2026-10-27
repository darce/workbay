# wb-tombstone

This is the shared redirect for every retired portable command id. There is
one body for all tombstone rows. Do not add per-id branches or per-id copies.

Tombstone rows are fail-closed discovery metadata. They are never published
as slash commands and must not appear in autocomplete.

## Core Process

1. Identify the invoking portable-command row (the retired `command_id` that
   launched this skill).
2. Echo that row's `description` and `replaces` fields verbatim. Those
   fields name the replacement `/wb-*` id. Tombstone rows omit
   `makefile_target`; `replaces` is the only replacement field.
3. Stop. Do not run the old workflow, do not invent a compatibility alias,
   and do not continue under the retired id.

## Red Flags

| Rationalization | Reality | Redirect |
| --- | --- | --- |
| "I can still run the old command." | The id is retired. `replaces` names the live `/wb-*` command. `/wb-tombstone` is not published as a slash command. | Announce the replacement id from the row and stop. |
| "This body should special-case each old id." | One generic redirect. The row carries the replacement. | Echo `description` and `replaces`. |
