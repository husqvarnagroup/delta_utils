#!/bin/bash

set -eax

# shellcheck disable=SC2068
TZ=UTC uv run pytest --cov=delta_utils --cov-report term-missing -vvv -x $@
