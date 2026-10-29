#!/bin/sh
set -eu
cd "${GC_CITY_PATH:?GC_CITY_PATH is required}"
export BEADS_DIR="$GC_CITY_PATH/.beads"
exec "${ANTFLY_BD_BIN:-bd}" antfly context "$@"
