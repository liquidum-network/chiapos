#!/bin/bash
set -e

export target_name=ProofOfSpace

. fast_build.sh

plot_file=~/storage/chia-final/ff8032e1d99fa037456bc73bddaf2f305a2368c6b89f5aade1f9046375f62e77.json

$build_dir/ProofOfSpace check-s3 $@ --file $plot_file
