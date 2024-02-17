#!/usr/bin/env bash

set -eo pipefail

echo ::group::CHECK_FORMATTING
scripts/format.sh --check
echo ::endgroup::
