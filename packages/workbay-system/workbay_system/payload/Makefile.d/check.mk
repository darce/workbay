# Makefile.d/check.mk — reusable stamp-skip check cache macro (implementation note S2).
#
# Ships the generic engine wrapper so a consumer can wrap its OWN suites in the
# check_all_cache stamp-skip cache without vendoring the engine or coupling to
# this monorepo's suite topology. No suite list lives here — the consumer wires
# its own keys/paths against whatever its check/CI targets are.
#
#   $(call workbay_checkall_cache,<key>,<paths>,<command>[,<fingerprint>])
#
# Overridable knobs:
#   WORKBAY_CHECKALL_ENGINE  path to check_all_cache.py (default: the installed
#                            consumer surface scripts/workbay/check_all_cache.py;
#                            the monorepo overrides it to the payload path).
#   WORKBAY_PYTHON           interpreter for the stdlib-only engine (default python3).
#
# CAVEAT: GNU make $(call) splits arguments on commas, so ANY argument that can
# contain a literal comma — <key>, <paths>, <command>, OR <fingerprint> — MUST be
# bound to a variable first and passed as $(VAR). Otherwise the comma starts a new
# $(call) argument and the tail is silently dropped: a <fingerprint> of
# "workers=4,os=linux" truncates to "workers=4", so two genuinely different modes
# digest identically and the cache false-skips one of them (OBS-08). E.g.
#   FP := workers=4,os=linux
#   $(call workbay_checkall_cache,k,src,$(MAKE) t,$(FP))

# Load sentinel (RLSE-05): GNU make expands an UNDEFINED `$(call ...)` to empty,
# so a routed recipe line `\t$(call workbay_checkall_cache,...)` becomes an empty
# recipe and the suite silently no-ops (exit 0, never runs) if this fragment was
# not loaded. Consumers/the monorepo can assert `$(WORKBAY_CHECK_MK_LOADED)` after
# their `-include ...*.mk` to fail LOUDLY instead of silently dropping the suite.
WORKBAY_CHECK_MK_LOADED := 1

WORKBAY_PYTHON          ?= python3
WORKBAY_CHECKALL_ENGINE ?= scripts/workbay/check_all_cache.py

# workbay_checkall_cache(key, paths, command[, fingerprint])
workbay_checkall_cache = $(WORKBAY_PYTHON) $(WORKBAY_CHECKALL_ENGINE) run --key $(1) --paths $(2) $(if $(4),--fingerprint "$(4)") -- $(3)
