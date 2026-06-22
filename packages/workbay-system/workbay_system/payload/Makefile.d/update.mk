# WorkBay one-shot stack update surface (internal; managed payload).
# `make workbay-update` upgrades the workbay-stack meta-package in the
# runtime owning workbay-bootstrap, refreshes the overlay from its recorded
# source, runs doctor, and prints the stack version table.
#
#   REMOTE_REF=<tag>            git_overlay consumers: ref to update to
#   WORKBAY_UPDATE_DRY_RUN=1  preview every mutating step without running it

.PHONY: workbay-update
workbay-update: ## Upgrade the workbay stack (one version anchor) + refresh overlay + doctor
	@sh scripts/workbay/update.sh
