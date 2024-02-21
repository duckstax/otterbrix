#!/usr/bin/env bash

set -eu
set -o pipefail

function move_hook {
    cp $1 $2
    chmod +x $2
}

move_hook scripts/git-hooks/pre-commit.sh.in .git/hooks/pre-commit