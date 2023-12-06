#!/usr/bin/env bash

set -eo pipefail
# need for format python script
apt -y install python3-pip && pip3 install termcolor


echo ::group::CHECK_FORMATTING
scripts/format.sh --check
echo ::endgroup::
