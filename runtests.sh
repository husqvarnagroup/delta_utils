#!/bin/bash

set -eax

poetry install

pytest --cov=delta_utils --cov-report term-missing -vvv
