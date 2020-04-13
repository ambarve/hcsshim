#!/bin/bash

set -eux

cd /source

gitVersion=$(git describe --match 'v[0-9]*' --always --long --tags)
branch=$(git branch --show-current)
timeStamp=$(date --utc +%Y%m%d)
version=cdpx-${gitVersion}+${branch}.${timeStamp}.${CDP_DEFINITION_BUILD_COUNT}

echo '##vso[build.updatebuildnumber]'$version