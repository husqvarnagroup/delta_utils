#!/bin/bash

set -eax

poetry install

# shellcheck disable=SC2068
pytest --cov=delta_utils --cov-report term-missing -vvv -x $@
