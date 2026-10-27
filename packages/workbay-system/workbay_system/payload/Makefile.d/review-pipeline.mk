# review-pipeline.mk — deterministic primitives for the branch-pipeline
# transitions that were previously prose-only (docs/workbay/rules/
# branch-pipeline.md sections 2, 5 and 7).
#
#   make review-stage      SUBJECT=<branch> SLUG=<slug> [REV_WORKTREE=<path>] [REV=<n>] [INTEGRATION=main]
#   make review-harvest    WORKTREE=<path> DOC=<path-in-worktree>
#   make review-adjudicate SUBJECT=<branch> PATCH=<patch> DOCS="<doc> [<doc>...]"
#   make merge-gate        SUBJECT=<branch>
#
# stage commits the review input patch and holds the proof-of-reading keys in
# a gitignored sidecar; adjudicate re-derives the keys at gate time, parses
# the verdict line and counts the policy's blocking severities; merge-gate refuses
# a merge whose adjudication artifact is missing, non-MERGE, or fenced to a stale
# subject tip. The merge guard hook consults the same artifact, so passing
# merge-gate is the only path to a raw `git merge feature/...` on main.

override _RP_FAILURE :=

# Validate raw make data before expansion, abspath, or any shell probe.
# Spaces and newlines are deliberately unsupported. Sentinels retain whitespace
# when testing the remainder after removing the allowlisted characters.
override _RP_COMMA := ,
override _RP_SAFE = $(if $(filter xx,x$(subst :,,$(subst $(_RP_COMMA),,$(subst @,,$(subst +,,$(subst -,,$(subst _,,$(subst .,,$(subst /,,$(subst 9,,$(subst 8,,$(subst 7,,$(subst 6,,$(subst 5,,$(subst 4,,$(subst 3,,$(subst 2,,$(subst 1,,$(subst 0,,$(subst z,,$(subst y,,$(subst x,,$(subst w,,$(subst v,,$(subst u,,$(subst t,,$(subst s,,$(subst r,,$(subst q,,$(subst p,,$(subst o,,$(subst n,,$(subst m,,$(subst l,,$(subst k,,$(subst j,,$(subst i,,$(subst h,,$(subst g,,$(subst f,,$(subst e,,$(subst d,,$(subst c,,$(subst b,,$(subst a,,$(subst Z,,$(subst Y,,$(subst X,,$(subst W,,$(subst V,,$(subst U,,$(subst T,,$(subst S,,$(subst R,,$(subst Q,,$(subst P,,$(subst O,,$(subst N,,$(subst M,,$(subst L,,$(subst K,,$(subst J,,$(subst I,,$(subst H,,$(subst G,,$(subst F,,$(subst E,,$(subst D,,$(subst C,,$(subst B,,$(subst A,,$(1)))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))))x),yes)
REVIEW_PIPELINE_PROBE_TIMEOUT ?= 20
override _RP_PROBE_TIMEOUT_RAW := $(value REVIEW_PIPELINE_PROBE_TIMEOUT)
override _RP_PROBE_TIMEOUT_SAFE := $(call _RP_SAFE,$(_RP_PROBE_TIMEOUT_RAW))
override _RP_PROBE_TIMEOUT_DIGITS := $(subst 9,,$(subst 8,,$(subst 7,,$(subst 6,,$(subst 5,,$(subst 4,,$(subst 3,,$(subst 2,,$(subst 1,,$(subst 0,,$(_RP_PROBE_TIMEOUT_RAW)))))))))))
override _RP_PROBE_TIMEOUT_NONZERO := $(subst 0,,$(_RP_PROBE_TIMEOUT_RAW))
ifeq ($(_RP_PROBE_TIMEOUT_SAFE),yes)
ifeq ($(_RP_PROBE_TIMEOUT_DIGITS),)
ifneq ($(_RP_PROBE_TIMEOUT_NONZERO),)
override _RP_PROBE_TIMEOUT_VALID := yes
else
override _RP_PROBE_TIMEOUT_VALID := no
endif
else
override _RP_PROBE_TIMEOUT_VALID := no
endif
else
override _RP_PROBE_TIMEOUT_VALID := no
endif
ifneq ($(_RP_PROBE_TIMEOUT_VALID),yes)
override _RP_FAILURE := $(if $(_RP_FAILURE),$(_RP_FAILURE),review_pipeline_probe_timeout_invalid)
override _RP_PROBE_TIMEOUT := 20
else
override _RP_PROBE_TIMEOUT := $(_RP_PROBE_TIMEOUT_RAW)
endif
override _RP_HAS_TIMEOUT := $(shell command -v timeout >/dev/null 2>&1 && printf yes)
override _RP_BOUNDED_EXEC := $(if $(filter yes,$(_RP_HAS_TIMEOUT)),timeout -k 1 $(_RP_PROBE_TIMEOUT),perl -e 'alarm shift; exec @ARGV' $(_RP_PROBE_TIMEOUT))
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
# Keep the judge-root venv as the default candidate; configured interpreters
# and the python3 fallback are resolved immediately below.
override _RP_PYTHON := $(_RP_JUDGE_ROOT)/.venv/bin/python
override _RP_PYTHON_REQUEST := $(if $(value REVIEW_PIPELINE_PYTHON),$(value REVIEW_PIPELINE_PYTHON),$(if $(value REVIEW_PIPELINE_PACKAGE_PYTHON),$(value REVIEW_PIPELINE_PACKAGE_PYTHON),))
ifeq ($(strip $(_RP_PYTHON_REQUEST)),)
override _RP_JUDGE_VENV_PYTHON := $(_RP_JUDGE_ROOT)/.venv/bin/python
override _RP_PYTHON_REQUEST := $(if $(shell test -f '$(_RP_JUDGE_VENV_PYTHON)' && test -x '$(_RP_JUDGE_VENV_PYTHON)' && printf yes),$(_RP_JUDGE_VENV_PYTHON),python3)
endif
ifneq ($(call _RP_SAFE,$(_RP_PYTHON_REQUEST)),yes)
override _RP_FAILURE := $(if $(_RP_FAILURE),$(_RP_FAILURE),review_pipeline_interpreter_unsupported_characters)
override _RP_PYTHON_REQUEST := python3
endif
# Resolve path-like requests from make's invocation directory. Bare command
# names resolve through PATH; make any relative PATH result absolute as well.
ifeq ($(findstring /,$(_RP_PYTHON_REQUEST)),)
override _RP_PYTHON_RESOLVED := $(shell command -v '$(_RP_PYTHON_REQUEST)' 2>/dev/null)
override _RP_PYTHON := $(if $(strip $(_RP_PYTHON_RESOLVED)),$(abspath $(_RP_PYTHON_RESOLVED)),$(_RP_PYTHON_REQUEST))
else
override _RP_PYTHON := $(abspath $(_RP_PYTHON_REQUEST))
endif
override _RP_PYTHON_AVAILABLE := $(shell test -f '$(_RP_PYTHON)' && test -x '$(_RP_PYTHON)' && printf yes)
override _RP_PACKAGE_PYTHON := $(if $(or $(value REVIEW_PIPELINE_PYTHON),$(value REVIEW_PIPELINE_PACKAGE_PYTHON)),$(_RP_PYTHON),)
override _RP_INTEGRATION_RAW := $(if $(value REVIEW_PIPELINE_INTEGRATION_ROOT),$(value REVIEW_PIPELINE_INTEGRATION_ROOT),$(_RP_JUDGE_ROOT))
override _RP_INTEGRATION_SAFE := $(call _RP_SAFE,$(_RP_INTEGRATION_RAW))
ifeq ($(_RP_INTEGRATION_SAFE),yes)
override _RP_INTEGRATION_ROOT := $(abspath $(_RP_INTEGRATION_RAW))
else
override _RP_INTEGRATION_ROOT :=
override _RP_FAILURE := $(if $(_RP_FAILURE),$(_RP_FAILURE),review_pipeline_integration_root_unsupported_characters)
endif
# REF-26: -I drops cwd/PYTHONPATH/user-site; extra roots are prepended in -c.
override _RP_SRC_PKG := $(_RP_JUDGE_ROOT)/packages/workbay-system
override _RP_ISOLATE := from pathlib import Path; import sys; sys.path[:0] = [p for p in sys.argv[1:] if p];
override _RP_SCRIPT := $(if $(filter yes,$(_RP_ROOT_SAFE)),$(shell test -f '$(_RP_JUDGE_ROOT)/scripts/review_pipeline.py' && printf '%s' '$(_RP_JUDGE_ROOT)/scripts/review_pipeline.py'))
ifeq ($(strip $(_RP_SCRIPT)),)
override _RP_SCRIPT := $(if $(filter yes,$(_RP_ROOT_SAFE)),$(shell $(_RP_BOUNDED_EXEC) '$(_RP_PYTHON)' -I -c '$(_RP_ISOLATE) import workbay_system; print(Path(workbay_system.__file__).parent / "payload/scripts/review_pipeline.py")' '$(_RP_SRC_PKG)' 2>/dev/null))
endif
ifeq ($(strip $(_RP_SCRIPT)),)
# Package installs own workbay_system in the workbay CLI's tool environment.
# Accept its pinned absolute shebang; wrappers can set the owner explicitly.
override _RP_PACKAGE_PYTHON := $(if $(or $(value REVIEW_PIPELINE_PYTHON),$(value REVIEW_PIPELINE_PACKAGE_PYTHON)),$(_RP_PYTHON),)
ifneq ($(call _RP_SAFE,$(_RP_PACKAGE_PYTHON)),yes)
$(error review_pipeline_path_unsupported_characters)
endif
override _RP_SCRIPT := $(if $(filter yes,$(_RP_ROOT_SAFE)),$(shell $(_RP_BOUNDED_EXEC) '$(_RP_PYTHON)' -I -c 'import sys; exec("from pathlib import Path\nimport shutil, subprocess, sys, re\ndef safe(path):\n if not re.fullmatch(r\"[A-Za-z0-9/._+@,:-]+\", path):\n  print(\"review_pipeline_path_unsupported_characters\")\n  sys.exit(0)\n return path\nowners = [sys.argv[1]] if sys.argv[1] else []\nif not owners:\n for name in (\"workbay\", \"workbay-bootstrap\"):\n  console = shutil.which(name)\n  if not console:\n   continue\n  safe(console)\n  try:\n   line = Path(console).read_text().splitlines()[0]\n  except (OSError, IndexError, UnicodeError):\n   continue\n  if line.startswith(chr(35) + \"!\") and Path(line[2:]).is_absolute():\n   owners.append(safe(line[2:]))\nfor owner in owners:\n safe(owner)\n try:\n  result = subprocess.run([owner, \"-I\", \"-c\", \"from pathlib import Path; import workbay_system; print(Path(workbay_system.__file__).parent / \\\"payload/scripts/review_pipeline.py\\\")\"], capture_output=True, text=True, timeout=float(sys.argv[2]))\n  if result.returncode == 0:\n   script = safe(result.stdout.rstrip(\"\\n\"))\n   safe(str(Path(script).with_name(\"assert_gate_interpreter.sh\")))\n   print(script)\n   break\n except (subprocess.TimeoutExpired, OSError, UnicodeError):\n  continue\n")' '$(_RP_PACKAGE_PYTHON)' '$(_RP_PROBE_TIMEOUT)' 2>/dev/null))
endif
# Never pass discovered data to shell source until it passes the allowlist.
ifeq ($(_RP_SCRIPT),review_pipeline_path_unsupported_characters)
$(error review_pipeline_path_unsupported_characters)
endif
ifneq ($(call _RP_SAFE,$(_RP_SCRIPT)),yes)
$(error review_pipeline_path_unsupported_characters)
endif

ifeq ($(strip $(_RP_SCRIPT)),)
override _RP_FAILURE := $(if $(_RP_FAILURE),$(_RP_FAILURE),review_pipeline_script_unresolved: interpreter probe timeout after $(_RP_PROBE_TIMEOUT)s or cannot locate workbay_system/payload/scripts/review_pipeline.py using $(_RP_PYTHON) or the workbay tool; set REVIEW_PIPELINE_PACKAGE_PYTHON=<tool interpreter>)
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
# Query the canonical policy module with the selected interpreter, isolated from
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
override _RP_OWNERSHIP_PKG := $(if $(filter yes,$(_RP_ROOT_SAFE)),$(shell $(_RP_BOUNDED_EXEC) '$(_RP_PYTHON)' -I -c '$(_RP_ISOLATE) import workbay_system; print(Path(workbay_system.__file__).resolve().parent.parent)' '$(_RP_SRC_PKG)' 2>/dev/null))
ifneq ($(call _RP_SAFE,$(_RP_OWNERSHIP_PKG)),yes)
override _RP_OWNERSHIP_PKG :=
endif
ifeq ($(strip $(_RP_OWNERSHIP_PKG)),)
override _RP_OWNERSHIP_HINT := $(_RP_PACKAGE_PYTHON)
ifneq ($(call _RP_SAFE,$(_RP_OWNERSHIP_HINT)),yes)
override _RP_OWNERSHIP_HINT :=
endif
override _RP_OWNERSHIP_PKG := $(if $(filter yes,$(_RP_ROOT_SAFE)),$(shell $(_RP_BOUNDED_EXEC) '$(_RP_PYTHON)' -I -c 'import sys; exec("import shutil, subprocess\nfrom pathlib import Path\nowners = [sys.argv[1]] if sys.argv[1] else []\nif not owners:\n for name in (\"workbay\", \"workbay-bootstrap\"):\n  console = shutil.which(name)\n  if not console:\n   continue\n  try:\n   line = Path(console).read_text().splitlines()[0]\n  except (OSError, IndexError, UnicodeError):\n   continue\n  if line.startswith(chr(35) + \"!\") and Path(line[2:]).is_absolute():\n   owners.append(line[2:])\nfor owner in owners:\n try:\n  result = subprocess.run([owner, \"-I\", \"-c\", \"from pathlib import Path; import workbay_system; print(Path(workbay_system.__file__).resolve().parent.parent)\"], capture_output=True, text=True, timeout=float(sys.argv[2]))\n except (subprocess.TimeoutExpired, OSError, UnicodeError):\n  continue\n if result.returncode == 0 and result.stdout.strip():\n  print(result.stdout.strip())\n  break")' '$(_RP_OWNERSHIP_HINT)' '$(_RP_PROBE_TIMEOUT)' 2>/dev/null))
ifneq ($(call _RP_SAFE,$(_RP_OWNERSHIP_PKG)),yes)
override _RP_OWNERSHIP_PKG :=
endif
endif
override _RP_OWNERSHIP_PYTHONPATH := $(_RP_OWNERSHIP_PKG)
override _RP_UNOWNED := $(if $(filter yes,$(_RP_ROOT_SAFE)),$(shell _rp_output=$$($(_RP_BOUNDED_EXEC) '$(_RP_PYTHON)' -I -c 'import sys; exec("import sys\nfrom pathlib import Path\nowner_raw, script_raw, root_raw = sys.argv[1], sys.argv[2], sys.argv[3]\nif not owner_raw:\n print(\"unknown:missing_owner\")\n raise SystemExit(0)\nowner = Path(owner_raw).resolve()\nscript = Path(script_raw).resolve() if script_raw else Path()\nroot = Path(root_raw)\nexpected = (owner / \"workbay_system/payload/scripts/review_pipeline.py\").resolve()\nroots = [str(owner)]\nif script == expected:\n derived = script.parent.parent.parent.parent\n if derived == owner:\n  roots.append(str(derived))\nsys.path[:0] = roots\ntry:\n import workbay_system.review_surface_ownership as rso\nexcept Exception:\n print(\"unknown:unresolvable\")\n raise SystemExit(0)\nmod = Path(getattr(rso, \"__file__\", \"\") or \".\").resolve()\nif \"__pycache__\" in mod.parts:\n mod = mod.parent.parent / (mod.stem.split(\".\", 1)[0] + \".py\")\npkg = mod.parent\npayload = (pkg / \"payload/scripts/review_pipeline.py\").resolve()\nunder = lambda p: owner == p or owner in p.parents\nok = under(mod) and payload.is_file() and under(payload) and (script == payload or Path(script_raw) == root / \"scripts/review_pipeline.py\")\nif not ok:\n print(\"unknown:provenance_mismatch\")\n raise SystemExit(0)\nraise SystemExit(rso.main([str(root)]))")' '$(_RP_OWNERSHIP_PKG)' '$(_RP_SCRIPT)' '$(_RP_JUDGE_ROOT)' 2>/dev/null); _rp_status=$$?; if [ "$$_rp_status" -eq 124 ] || [ "$$_rp_status" -eq 137 ] || [ "$$_rp_status" -eq 142 ]; then printf '%s' probe_timeout; elif [ "$$_rp_status" -eq 0 ]; then printf '%s' "$$_rp_output"; else printf '%s' probe_failed; fi))
ifeq ($(_RP_UNOWNED),probe_failed)
override _RP_UNOWNED := unknown:probe_failed
endif
ifeq ($(_RP_UNOWNED),probe_timeout)
override _RP_UNOWNED := unknown:probe_timeout
endif
ifeq ($(_RP_PYTHON_AVAILABLE),)
ifneq ($(strip $(_RP_SCRIPT)),)
override _RP_FAILURE := $(if $(_RP_FAILURE),$(_RP_FAILURE),interpreter_missing{$(_RP_PYTHON)})
override _RP_UNOWNED := owned
endif
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
ifeq ($(patsubst unknown:%,%,$(_RP_UNOWNED)),probe_timeout)
override _RP_FAILURE := $(if $(_RP_FAILURE),$(_RP_FAILURE),review_surface_ownership_unknown: probe_timeout after $(_RP_PROBE_TIMEOUT)s)
else ifeq ($(patsubst unknown:%,%,$(_RP_UNOWNED)),missing_owner)
override _RP_FAILURE := $(if $(_RP_FAILURE),$(_RP_FAILURE),review_surface_ownership_unknown: missing_owner (ownership probe timeout after $(_RP_PROBE_TIMEOUT)s or package unavailable))
else
override _RP_FAILURE := $(if $(_RP_FAILURE),$(_RP_FAILURE),review_surface_ownership_unknown: $(patsubst unknown:%,%,$(_RP_UNOWNED)))
endif
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
override _RP_COMMAND := "$(_RP_PYTHON)" "$(_RP_SCRIPT)" --root "$(_RP_INTEGRATION_ROOT)"
INTEGRATION ?= main
REV ?= 1

.PHONY: review-stage review-harvest review-adjudicate merge-gate

review-stage: ## Stage a subject branch for review: SUBJECT=<branch> SLUG=<slug> [REV_WORKTREE=<path>] [REV=<n>] (no REV_WORKTREE -> stage cuts one)
	@+$(_RP_GUARD)
	@test -n "$(SUBJECT)" || { echo "SUBJECT=<branch> is required" >&2; exit 2; }
	@test -n "$(SLUG)" || { echo "SLUG=<slug> is required" >&2; exit 2; }
	@bash "$(_RP_HELPER)" "$(_RP_PYTHON)" "review-stage"
	@JUDGE_ROOT="$(_RP_JUDGE_ROOT)" SYSTEM_PYTHON="$(_RP_PYTHON)" REVIEW_PIPELINE_PYTHON="$(_RP_PYTHON)" PYTHONPATH="$(_RP_PYTHONPATH)" $(_RP_COMMAND) stage --subject '$(SUBJECT)' --slug '$(SLUG)' --integration '$(INTEGRATION)' --rev '$(REV)' $(if $(REV_WORKTREE),--worktree '$(REV_WORKTREE)')

review-harvest: ## Commit exactly one review document: WORKTREE=<path> DOC=<path relative to WORKTREE>
	@+$(_RP_GUARD)
	@test -n "$(WORKTREE)" || { echo "WORKTREE=<review worktree> is required" >&2; exit 2; }
	@test -n "$(DOC)" || { echo "DOC=<review document> is required" >&2; exit 2; }
	@bash "$(_RP_HELPER)" "$(_RP_PYTHON)" "review-harvest"
	@JUDGE_ROOT="$(_RP_JUDGE_ROOT)" SYSTEM_PYTHON="$(_RP_PYTHON)" REVIEW_PIPELINE_PYTHON="$(_RP_PYTHON)" PYTHONPATH="$(_RP_PYTHONPATH)" $(_RP_COMMAND) harvest --worktree '$(WORKTREE)' --doc '$(DOC)'

review-adjudicate: ## Adjudicate committed review docs: SUBJECT=<branch> PATCH=<path> DOCS="<doc>..."
	@+$(_RP_GUARD)
	@test -n "$(SUBJECT)" || { echo "SUBJECT=<branch> is required" >&2; exit 2; }
	@test -n "$(PATCH)" || { echo "PATCH=<input patch path> is required" >&2; exit 2; }
	@test -n "$(DOCS)" || { echo "DOCS=\"<review doc> ...\" is required" >&2; exit 2; }
	@bash "$(_RP_HELPER)" "$(_RP_PYTHON)" "review-adjudicate"
	@JUDGE_ROOT="$(_RP_JUDGE_ROOT)" SYSTEM_PYTHON="$(_RP_PYTHON)" REVIEW_PIPELINE_PYTHON="$(_RP_PYTHON)" PYTHONPATH="$(_RP_PYTHONPATH)" $(_RP_COMMAND) adjudicate --subject '$(SUBJECT)' --patch '$(PATCH)' --docs $(DOCS)

merge-gate: ## Verify the adjudication artifact authorizes merging SUBJECT=<branch> at its current tip
	@+$(_RP_GUARD)
	@test -n "$(SUBJECT)" || { echo "SUBJECT=<branch> is required" >&2; exit 2; }
	@bash "$(_RP_HELPER)" "$(_RP_PYTHON)" "merge-gate"
	@JUDGE_ROOT="$(_RP_JUDGE_ROOT)" SYSTEM_PYTHON="$(_RP_PYTHON)" REVIEW_PIPELINE_PYTHON="$(_RP_PYTHON)" PYTHONPATH="$(_RP_PYTHONPATH)" $(_RP_COMMAND) merge-gate --subject '$(SUBJECT)'
