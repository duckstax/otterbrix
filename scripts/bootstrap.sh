#!/usr/bin/env bash

set -eu
set -o pipefail

chmod +x scripts/format.sh
chmod +x scripts/git-hooks/setup-git-hooks.sh

scripts/git-hooks/setup-git-hooks.sh