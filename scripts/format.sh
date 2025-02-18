##!/usr/bin/env bash

set -eo pipefail


python3 scripts/tools/code-fromat.py ${1} \
components \
core \
example \
integration \
services