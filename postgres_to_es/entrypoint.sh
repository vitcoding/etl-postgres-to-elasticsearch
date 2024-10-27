#!/usr/bin/env bash

set -e

# while ! nc -z $SQL_HOST $SQL_PORT; do
#     sleep 0.1
# done

python main.py
