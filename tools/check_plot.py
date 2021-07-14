import argparse
import os
import sys
import json
import random
import time
import statistics
from chiapos import DiskProver, S3Prover, Verifier
# from verifier_py import Verifier

def print_stats(values):
  print(f"  Mean: {statistics.mean(values)}")
  try:
    print(f"   Min: {min(values)}")
    print(f"   Max: {max(values)}")
    print(f"   Var: {statistics.pvariance(values)}")
    print(f" Quant: {statistics.quantiles(values)}")
  except statistics.StatisticsError:
    pass

def get_arg_parser():
  parser = argparse.ArgumentParser(
      formatter_class=argparse.ArgumentDefaultsHelpFormatter)

  parser.add_argument("plot_file", help="plot file to check")
  parser.add_argument("-i", "--iterations", type=int, default=10, help="Number of iterations.")
  parser.add_argument("-q", "--qualities_only", default=False, action="store_true",
      help="Only fetch qualities, no proofs.")
  parser.add_argument(
      "--old_algo",
      default=False,
      action="store_true", help="Use old serial full proof lookup")

  return parser


def old_proof_algorithm(prover, p7_entries, challenge_time, total_times):
  total_time = challenge_time
  proofs = []
  for p7_entry in p7_entries:
    time_before_proof = time.time()
    full_proof = prover.get_full_proof_from_p7_entry(p7_entry)
    proof_time = time.time() - time_before_proof
    total_time += proof_time

    proofs.append(full_proof)

  total_times.append(total_time)
  return proofs

def new_proof_algorithm(prover, p7_entries, challenge_time, total_times):
  time_before_proof = time.time()
  err_proof_pairs = prover.get_full_proofs_from_p7_entries(p7_entries)
  total_time = challenge_time + time.time() - time_before_proof
  total_times.append(total_time)

  for message, _ in err_proof_pairs:
    if message:
      raise Exception(message)

  return [proof for _, proof in err_proof_pairs]


def main():
  parser = get_arg_parser()
  opts = parser.parse_args()

  if opts.plot_file.endswith('json'):
    with open(opts.plot_file, "rb") as f:
      manifest_str = f.read()

    manifest = json.loads(manifest_str.decode())
    k = manifest['k']
    plot_id = bytes.fromhex(manifest['id'])
    print(f"Checking {opts.plot_file} with k={k}")
    prover = S3Prover(manifest_str)
  else:
    raise NotImplementedError()
    prover = DiskProver(opts.plot_file)
    k = prover.get_size()
    plot_id = prover.get_id()

  verifier = Verifier()

  random.seed(1234)

  challenge_times = []
  total_times = []

  total_proofs = 0
  for i in range(opts.iterations + 1):
    challenge = bytes(random.getrandbits(8) for _ in range(32))
     
    # Make sure verifier works
    verified_quality = verifier.validate_proof(plot_id, k, challenge, b"")
    assert verified_quality is None

    t_before_challenge = time.time()
    qualities, p7_entries = prover.get_qualities_and_p7_entries_for_challenge(challenge)
    challenge_time = time.time() - t_before_challenge
    challenge_times.append(challenge_time)

    if opts.qualities_only:
      continue

    total_proofs += len(qualities)
    if opts.old_algo:
      proofs = old_proof_algorithm(prover, p7_entries, challenge_time, total_times)
    else:
      proofs = new_proof_algorithm(prover, p7_entries, challenge_time, total_times)

    for quality, full_proof in zip(qualities, proofs):
      assert len(quality) == 32, len(quality)
      verified_quality = verifier.validate_proof(plot_id, k, challenge, full_proof)
      if quality != verified_quality:
        raise Exception(f"Proof failed, quality={quality.hex()} != {verified_quality.hex()}")
    sys.stdout.write('.')
    sys.stdout.flush()

  challenge_times = challenge_times[1:]
  total_times = total_times[1:]

  print("")
  print(f"Found {total_proofs} / {opts.iterations} proofs")
  print(f"Times n={len(challenge_times)}")
  print("== Qualities ==")
  print_stats(challenge_times)

  if not opts.qualities_only:
    print("== End To End ==")
    print_stats(total_times)

if __name__ == "__main__":
  main()
