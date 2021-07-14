#!/bin/bash
set -e

export target_name=ProofOfSpace

. fast_build.sh

plot_tmp=~/storage/tmp
args="\
  -r 2 -u 256 \
  -b 7200 -k 24 -s 32768 \
  -t $plot_tmp \
  -d $plot_tmp \
  -2 $plot_tmp \
  -m 9395f6d06314386c14b8d4f1fde5b964d0ba459c804665310a7c5d8271013e26cb2445660c7cdb422f89b9586d9f53c0b298524587a3e55e0977b51856cb7506df9c748f3c6716419691d136f45ac8837b62569669fcc3f50b60f63f41fc68a33a25cc813682b3960453415ffbcdfecd496e0511dcf334fcab0b5702beb6077b \
  -i 9fe8dbc4292650d65f9e8039f3c73c00ac36703997b379ebd05644e64d33aa79 \
  -p 1 $@"

$build_dir/ProofOfSpace create ${args}
