#!/bin/sh -e

# Emit stamp values for every build, not just CI, so local builds end
# up with the same `${TIMESTAMP}-${SHA}` image-tag format that the GitHub
# Actions workflow produces. Image push targets (e.g.
# //cmd/bb_worker:bb_worker_container_push) read these via
# @com_github_buildbarn_bb_storage//tools:stamped_tags and template-expand
# them into the pushed tag.
TS=$(git show -s --format=%ct HEAD)
SHA=$(git rev-parse --short HEAD)

# GNU date uses -d "@<unix_ts>"; BSD/macOS date uses -r <unix_ts>. Try the
# GNU form first (CI runners on Linux), fall back to the BSD form so this
# script also works for local builds on macOS.
if FORMATTED=$(TZ=UTC date -u -d "@${TS}" +%Y%m%dT%H%M%SZ 2>/dev/null); then
  :
else
  FORMATTED=$(TZ=UTC date -u -r "${TS}" +%Y%m%dT%H%M%SZ)
fi

echo "BUILD_SCM_REVISION ${SHA}"
echo "BUILD_SCM_TIMESTAMP ${FORMATTED}"
