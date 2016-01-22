#!/bin/bash

set -xe

WORKING_COPY=`pwd`

git checkout nofib-testing
git submodule update --init
echo "Hello"
