# review-pipeline.mk — deterministic primitives for the branch-pipeline
# transitions that were previously prose-only (docs/workbay/rules/
# branch-pipeline.md sections 2, 5 and 7).
#
#   make review-stage      SUBJECT=<branch> SLUG=<slug> [REV_WORKTREE=<path>] [REV=<n>] [INTEGRATION=main]
#   make review-adjudicate SUBJECT=<branch> PATCH=<patch> DOCS="<doc> [<doc>...]"
#   make merge-gate        SUBJECT=<branch>
#
# stage commits the review input patch and holds the proof-of-reading keys in
# a gitignored sidecar; adjudicate re-derives the keys at gate time, parses
# the verdict line and counts MEDIUM+ findings; merge-gate refuses a merge
# whose adjudication artifact is missing, non-MERGE, or fenced to a stale
# subject tip. The merge guard hook consults the same artifact, so passing
# merge-gate is the only path to a raw `git merge feature/...` on main.

override _RP_FAILURE :=

# Validate raw make data before expansion, abspath, or any shell probe.
# Spaces and newlines are deliberately unsupported. Sentinels retain whitespace
# when testing the remainder after removing the allowlisted characters.
override _RP_COMMA := ,
override _RP_SAFE = $(if $(filter xx,x$(subst :,,$(subst $(_RP_COMMA),,$(subst @,,$(subst +,,$(subst -,,$(subst _,,$(subst .,,$(subst /,,$(subst 9,,$(subst 8,,$(subst 7,,$(subst 6,,$(subst 5,,$(subst 4,,$(subst 3,,$(subst 2,,$(subst 1,,$(subst 0,,$(subst z,,$(subst y,,$(subst x,,$(subst w,,$(subst v,,$(subst u,,$(subst t,,$(subst s,,$(subst r,,$(subst q,,$(subst p,,$(subst o,,$(subst n,,$(subst m,,$(subst l,,$(subst k,,$(subst j,,$(subst i,,$(subst h,,$(subst g,,$(subst f,,$(subst e,,$(subst d,,$(subst c,,$(subst b,,$(subst a,,$(subst Z,,$(subst Y,,$(subst X,,$(subst W,,$(subst V,,$(subst U,,$(subst T,,$(subst S,,$(subst R,,$(subst Q,,$(subst P,,$(subst O,,$(subst N,,$(subst M,,$(subst L,,$(subst K,,$(subst J,,$(subst I,,$(subst H,,$(subst G,,$(subst F,,$(subst E,,$(subst D,,$(subst C,,$(subst B,,$(subst A,,$(1)))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))x),yes)
override _RP_EXPLICIT_ROOT := $(if $(value JUDGE_ROOT),yes)
override _RP_RAW := $(if $(value JUDGE_ROOT),$(value JUDGE_ROOT),$(CURDIR))
# Freeze exported command-line data too: make must not evaluate it for recipes.
override JUDGE_ROOT := $(_RP_RAW)
override _RP_ROOT_SAFE := $(call _RP_SAFE,$(_RP_RAW))
override _RP_JUDGE_ROOT :=
ifeq ($(_RP_ROOT_SAFE),yes)
override _RP_JUDGE_ROOT := $(if $(_RP_EXPLICIT_ROOT),$(abspath $(_RP_RAW)),$(shell git -C '$(_RP_RAW)' rev-parse --show-toplevel 2>/dev/null))
override _RP_ROOT_SAFE := $(call _RP_SAFE,$(_RP_JUDGE_ROOT))
endif
ifneq ($(_RP_ROOT_SAFE),yes)
override _RP_FAILURE := review_pipeline_judge_root_unsupported_characters
endif
ifeq ($(strip $(_RP_JUDGE_ROOT)),)
override _RP_FAILURE := $(if $(_RP_FAILURE),$(_RP_FAILURE),JUDGE_ROOT is required and could not be inferred; set JUDGE_ROOT=<path to the protocol monorepo>)
endif
override _RP_PYTHON := $(_RP_JUDGE_ROOT)/.venv/bin/python
# REF-26: -I drops cwd/PYTHONPATH/user-site; extra roots are prepended in -c.
override _RP_SRC_PKG := $(_RP_JUDGE_ROOT)/packages/workbay-system
override _RP_ISOLATE := from pathlib import Path; import sys; sys.path[:0] = [p for p in sys.argv[1:] if p];
override _RP_SCRIPT := $(if $(filter yes,$(_RP_ROOT_SAFE)),$(shell test -f '$(_RP_JUDGE_ROOT)/scripts/review_pipeline.py' && printf '%s' '$(_RP_JUDGE_ROOT)/scripts/review_pipeline.py'))
ifeq ($(strip $(_RP_SCRIPT)),)
override _RP_SCRIPT := $(if $(filter yes,$(_RP_ROOT_SAFE)),$(shell '$(_RP_PYTHON)' -I -c '$(_RP_ISOLATE) import workbay_system; print(Path(workbay_system.__file__).parent / "payload/scripts/review_pipeline.py")' '$(_RP_SRC_PKG)' 2>/dev/null))
endif
ifeq ($(strip $(_RP_SCRIPT)),)
# Package installs own workbay_system in the workbay CLI's tool environment.
# Accept its pinned absolute shebang; wrappers can set the owner explicitly.
# This interpreter only locates data: gates still run with the judge Python,
# and review_pipeline.py still refuses sources outside the judge root.
override _RP_PACKAGE_PYTHON := $(value REVIEW_PIPELINE_PACKAGE_PYTHON)
ifneq ($(call _RP_SAFE,$(_RP_PACKAGE_PYTHON)),yes)
$(error review_pipeline_path_unsupported_characters)
endif
override _RP_SCRIPT := $(if $(filter yes,$(_RP_ROOT_SAFE)),$(shell '$(_RP_PYTHON)' -I -c 'import sys; exec("from pathlib import Path\nimport shutil, subprocess, sys, re\ndef safe(path):\n if not re.fullmatch(r\"[A-Za-z0-9/._+@,:-]+\", path):\n  print(\"review_pipeline_path_unsupported_characters\")\n  sys.exit(0)\n return path\nowners = [sys.argv[1]] if sys.argv[1] else []\nif not owners:\n for name in (\"workbay\", \"workbay-bootstrap\"):\n  console = shutil.which(name)\n  if not console:\n   continue\n  safe(console)\n  try:\n   line = Path(console).read_text().splitlines()[0]\n  except (OSError, IndexError, UnicodeError):\n   continue\n  if line.startswith(chr(35) + \"!\") and Path(line[2:]).is_absolute():\n   owners.append(safe(line[2:]))\nfor owner in owners:\n safe(owner)\n try:\n  result = subprocess.run([owner, \"-I\", \"-c\", \"from pathlib import Path; import workbay_system; print(Path(workbay_system.__file__).parent / \\\"payload/scripts/review_pipeline.py\\\")\"], capture_output=True, text=True)\n  if result.returncode == 0:\n   script = safe(result.stdout.rstrip(\"\\n\"))\n   safe(str(Path(script).with_name(\"assert_gate_interpreter.sh\")))\n   print(script)\n   break\n except (OSError, UnicodeError):\n  continue\n")' '$(_RP_PACKAGE_PYTHON)' 2>/dev/null))
endif
# Never pass discovered data to shell source until it passes the allowlist.
ifeq ($(_RP_SCRIPT),review_pipeline_path_unsupported_characters)
$(error review_pipeline_path_unsupported_characters)
endif
ifneq ($(call _RP_SAFE,$(_RP_SCRIPT)),yes)
$(error review_pipeline_path_unsupported_characters)
endif

ifeq ($(strip $(_RP_SCRIPT)),)
override _RP_FAILURE := $(if $(_RP_FAILURE),$(_RP_FAILURE),review_pipeline_script_unresolved: cannot locate workbay_system/payload/scripts/review_pipeline.py using $(_RP_PYTHON) or the workbay tool; set REVIEW_PIPELINE_PACKAGE_PYTHON=<tool interpreter>)
endif
ifeq ($(if $(filter yes,$(_RP_ROOT_SAFE)),$(shell test -f '$(_RP_SCRIPT)' && echo yes)),)
override _RP_FAILURE := $(if $(_RP_FAILURE),$(_RP_FAILURE),review_pipeline_script_missing: $(_RP_SCRIPT))
endif
override _RP_PYTHONPATH := $(_RP_JUDGE_ROOT)/packages/workbay-protocol/src:$(_RP_JUDGE_ROOT)/packages/mcp-workbay-handoff/src:$(_RP_JUDGE_ROOT)/packages/mcp-workbay-orchestrator/src
override _RP_HELPER := $(dir $(_RP_SCRIPT))assert_gate_interpreter.sh
ifeq ($(if $(filter yes,$(_RP_ROOT_SAFE)),$(shell test -f '$(_RP_HELPER)' && echo yes)),)
override _RP_FAILURE := $(if $(_RP_FAILURE),$(_RP_FAILURE),review_pipeline_helper_missing: $(_RP_HELPER))
endif
# A legacy receipt must not silently authorize newly managed consumer gates.
# Query the canonical policy module with the judge interpreter, isolated from
# cwd and ambient PYTHONPATH (REF-26: -I, then one independently validated
# owner root). The owner is the source-repo package, the judge-venv package,
# explicit REVIEW_PIPELINE_PACKAGE_PYTHON, or a successfully imported
# installer owner. A consumer script never makes its filesystem ancestors a
# trusted import root. Import only from that owner; the module and its
# payload/scripts/review_pipeline.py must lie under it, and _RP_SCRIPT must
# resolve to that payload or be the consumer helper the predicate already
# classifies. A missing owner or mismatch is unknown and never calls the
# predicate (CARD-07). A failed probe is named explicitly; all other output
# besides the exact known states is refused as unexpected.
override _RP_OWNERSHIP_PKG := $(if $(filter yes,$(_RP_ROOT_SAFE)),$(shell '$(_RP_PYTHON)' -I -c '$(_RP_ISOLATE) import workbay_system; print(Path(workbay_system.__file__).resolve().parent.parent)' '$(_RP_SRC_PKG)' 2>/dev/null))
ifneq ($(call _RP_SAFE,$(_RP_OWNERSHIP_PKG)),yes)
override _RP_OWNERSHIP_PKG :=
endif
ifeq ($(strip $(_RP_OWNERSHIP_PKG)),)
override _RP_OWNERSHIP_HINT := $(value REVIEW_PIPELINE_PACKAGE_PYTHON)
ifneq ($(call _RP_SAFE,$(_RP_OWNERSHIP_HINT)),yes)
override _RP_OWNERSHIP_HINT :=
endif
override _RP_OWNERSHIP_PKG := $(if $(filter yes,$(_RP_ROOT_SAFE)),$(shell '$(_RP_PYTHON)' -I -c 'import sys; exec("import shutil, subprocess\nfrom pathlib import Path\nowners = [sys.argv[1]] if sys.argv[1] else []\nif not owners:\n for name in (\"workbay\", \"workbay-bootstrap\"):\n  console = shutil.which(name)\n  if not console:\n   continue\n  try:\n   line = Path(console).read_text().splitlines()[0]\n  except (OSError, IndexError, UnicodeError):\n   continue\n  if line.startswith(chr(35) + \"!\") and Path(line[2:]).is_absolute():\n   owners.append(line[2:])\nfor owner in owners:\n try:\n  result = subprocess.run([owner, \"-I\", \"-c\", \"from pathlib import Path; import workbay_system; print(Path(workbay_system.__file__).resolve().parent.parent)\"], capture_output=True, text=True)\n except (OSError, UnicodeError):\n  continue\n if result.returncode == 0 and result.stdout.strip():\n  print(result.stdout.strip())\n  break")' '$(_RP_OWNERSHIP_HINT)' 2>/dev/null))
ifneq ($(call _RP_SAFE,$(_RP_OWNERSHIP_PKG)),yes)
override _RP_OWNERSHIP_PKG :=
endif
endif
override _RP_OWNERSHIP_PYTHONPATH := $(_RP_OWNERSHIP_PKG)
override _RP_UNOWNED := $(if $(filter yes,$(_RP_ROOT_SAFE)),$(shell _rp_output=$$('$(_RP_PYTHON)' -I -c 'import sys; exec("import sys\nfrom pathlib import Path\nowner_raw, script_raw, root_raw = sys.argv[1], sys.argv[2], sys.argv[3]\nif not owner_raw:\n print(\"unknown:missing_owner\")\n raise SystemExit(0)\nowner = Path(owner_raw).resolve()\nscript = Path(script_raw).resolve() if script_raw else Path()\nroot = Path(root_raw)\nexpected = (owner / \"workbay_system/payload/scripts/review_pipeline.py\").resolve()\nroots = [str(owner)]\nif script == expected:\n derived = script.parent.parent.parent.parent\n if derived == owner:\n  roots.append(str(derived))\nsys.path[:0] = roots\ntry:\n import workbay_system.review_surface_ownership as rso\nexcept Exception:\n print(\"unknown:unresolvable\")\n raise SystemExit(0)\nmod = Path(getattr(rso, \"__file__\", \"\") or \".\").resolve()\nif \"__pycache__\" in mod.parts:\n mod = mod.parent.parent / (mod.stem.split(\".\", 1)[0] + \".py\")\npkg = mod.parent\npayload = (pkg / \"payload/scripts/review_pipeline.py\").resolve()\nunder = lambda p: owner == p or owner in p.parents\nok = under(mod) and payload.is_file() and under(payload) and (script == payload or Path(script_raw) == root / \"scripts/review_pipeline.py\")\nif not ok:\n print(\"unknown:provenance_mismatch\")\n raise SystemExit(0)\nraise SystemExit(rso.main([str(root)]))")' '$(_RP_OWNERSHIP_PKG)' '$(_RP_SCRIPT)' '$(_RP_JUDGE_ROOT)' 2>/dev/null) && printf '%s' "$$_rp_output" || printf '%s' probe_failed))
ifeq ($(_RP_UNOWNED),probe_failed)
override _RP_UNOWNED := unknown:probe_failed
endif
ifneq ($(_RP_UNOWNED),owned)
ifneq ($(filter conflict:%,$(_RP_UNOWNED)),)
ifneq ($(strip $(patsubst conflict:%,%,$(_RP_UNOWNED))),)
override _RP_FAILURE := $(if $(_RP_FAILURE),$(_RP_FAILURE),unowned_review_surface_conflict: $(patsubst conflict:%,%,$(_RP_UNOWNED)); resolve the legacy receipt before running review gates)
else
override _RP_FAILURE := $(if $(_RP_FAILURE),$(_RP_FAILURE),review_surface_ownership_unknown: unexpected_probe_output)
endif
else
ifneq ($(filter unknown:%,$(_RP_UNOWNED)),)
ifneq ($(strip $(patsubst unknown:%,%,$(_RP_UNOWNED))),)
override _RP_FAILURE := $(if $(_RP_FAILURE),$(_RP_FAILURE),review_surface_ownership_unknown: $(patsubst unknown:%,%,$(_RP_UNOWNED)))
else
override _RP_FAILURE := $(if $(_RP_FAILURE),$(_RP_FAILURE),review_surface_ownership_unknown: unexpected_probe_output)
endif
else
override _RP_FAILURE := $(if $(_RP_FAILURE),$(_RP_FAILURE),review_surface_ownership_unknown: unexpected_probe_output)
endif
endif
endif
# Defer resolution failures until a review recipe executes. Preserve the first
# failure and quote it as shell data, including paths containing apostrophes.
override _RP_GUARD := $(if $(_RP_FAILURE),printf '%s\n' '$(subst ','"'"',$(_RP_FAILURE))' >&2; exit 2,:)
override _RP_COMMAND := "$(_RP_PYTHON)" "$(_RP_SCRIPT)" --root "$(_RP_JUDGE_ROOT)"
INTEGRATION ?= main
REV ?= 1

.PHONY: review-stage review-adjudicate merge-gate

review-stage: ## Stage a subject branch for review: SUBJECT=<branch> SLUG=<slug> [REV_WORKTREE=<path>] [REV=<n>] (no REV_WORKTREE -> stage cuts one)
	@+$(_RP_GUARD)
	@test -n "$(SUBJECT)" || { echo "SUBJECT=<branch> is required" >&2; exit 2; }
	@test -n "$(SLUG)" || { echo "SLUG=<slug> is required" >&2; exit 2; }
	@bash "$(_RP_HELPER)" "$(_RP_PYTHON)" "review-stage"
	@JUDGE_ROOT="$(_RP_JUDGE_ROOT)" SYSTEM_PYTHON="$(_RP_PYTHON)" PYTHONPATH="$(_RP_PYTHONPATH)" $(_RP_COMMAND) stage --subject '$(SUBJECT)' --slug '$(SLUG)' --integration '$(INTEGRATION)' --rev '$(REV)' $(if $(REV_WORKTREE),--worktree '$(REV_WORKTREE)')

review-adjudicate: ## Adjudicate committed review docs: SUBJECT=<branch> PATCH=<path> DOCS="<doc>..."
	@+$(_RP_GUARD)
	@test -n "$(SUBJECT)" || { echo "SUBJECT=<branch> is required" >&2; exit 2; }
	@test -n "$(PATCH)" || { echo "PATCH=<input patch path> is required" >&2; exit 2; }
	@test -n "$(DOCS)" || { echo "DOCS=\"<review doc> ...\" is required" >&2; exit 2; }
	@bash "$(_RP_HELPER)" "$(_RP_PYTHON)" "review-adjudicate"
	@JUDGE_ROOT="$(_RP_JUDGE_ROOT)" SYSTEM_PYTHON="$(_RP_PYTHON)" PYTHONPATH="$(_RP_PYTHONPATH)" $(_RP_COMMAND) adjudicate --subject '$(SUBJECT)' --patch '$(PATCH)' --docs $(DOCS)

merge-gate: ## Verify the adjudication artifact authorizes merging SUBJECT=<branch> at its current tip
	@+$(_RP_GUARD)
	@test -n "$(SUBJECT)" || { echo "SUBJECT=<branch> is required" >&2; exit 2; }
	@bash "$(_RP_HELPER)" "$(_RP_PYTHON)" "merge-gate"
	@JUDGE_ROOT="$(_RP_JUDGE_ROOT)" SYSTEM_PYTHON="$(_RP_PYTHON)" PYTHONPATH="$(_RP_PYTHONPATH)" $(_RP_COMMAND) merge-gate --subject '$(SUBJECT)'
