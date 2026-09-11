#!/bin/bash
set -eu

test -f /tmp/post_start_completed
: > "/dev/tcp/$(hostname)/7077"