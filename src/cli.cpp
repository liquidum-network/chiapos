// Copyright 2018 Chia Network Inc

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//    http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <ctime>
#include <numeric>
#include <set>

#include "../lib/include/picosha2.hpp"
#include "cxxopts.hpp"
#include "entry_sizes.hpp"
#include "logging.hpp"
#include "logging_helpers.hpp"
#include "plotter_disk.hpp"
#include "progress.hpp"
#include "prover_disk.hpp"
#include "prover_s3.hpp"
#include "serialization.hpp"
#include "spdlog/pattern_formatter.h"
#include "spdlog/sinks/rotating_file_sink.h"
#include "spdlog/sinks/stdout_sinks.h"
#include "time_helpers.hpp"
#include "verifier.hpp"

using std::cout;
using std::endl;
using std::string;
using std::vector;

vector<unsigned char> intToBytes(uint32_t paramInt, uint32_t numBytes)
{
    vector<unsigned char> arrayOfByte(numBytes, 0);
    for (uint32_t i = 0; paramInt > 0; i++) {
        arrayOfByte[numBytes - i - 1] = paramInt & 0xff;
        paramInt >>= 8;
    }
    return arrayOfByte;
}

void HelpAndQuit(cxxopts::Options options)
{
    cout << options.help({""}) << endl;
    cout << "./ProofOfSpace create" << endl;
    cout << "./ProofOfSpace prove <challenge>" << endl;
    cout << "./ProofOfSpace verify <proof> <challenge>" << endl;
    cout << "./ProofOfSpace check" << endl;
    cout << "./ProofOfSpace check-s3" << endl;
    cout << "./ProofOfSpace show-info" << endl;
    exit(0);
}

void SetupLogging(const std::string &plot_id)
{
    auto stdout_sink = std::make_shared<spdlog::sinks::stdout_sink_mt>();
    stdout_sink->set_level(spdlog::level::info);

    auto stderr_sink = std::make_shared<spdlog::sinks::stderr_sink_mt>();
    stderr_sink->set_level(spdlog::level::err);

    auto file_sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
        "logs/chiapos_cli_", 1024 * 1024 * 100, 3, true);
    file_sink->set_level(spdlog::level::trace);

    std::vector<spdlog::sink_ptr> sinks = {
        std::move(stdout_sink),
        std::move(stderr_sink),
        std::move(file_sink),
    };

    auto logger = std::make_shared<spdlog::logger>("default", begin(sinks), end(sinks));
    auto formatter = std::make_unique<spdlog::pattern_formatter>(
        fmt::format("%L %Y-%m-%dT%T.%e {}:%t %s:%#] %v", plot_id.substr(0, 4)),
        spdlog::pattern_time_type::utc);
    logger->set_formatter(std::move(formatter));
    logger->set_level(spdlog::level::trace);
    logger->flush_on(spdlog::level::warn);

    spdlog::set_default_logger(std::move(logger));

    R"(
    XXX TODO
      * Figure out why sometimes I delete files before the next checkpoint is
      * saved, like in
      *   https://us-west-2.console.aws.amazon.com/cloudwatch/home?region=us-west-2#logsV2:log-groups/log-group/chia-plotter-stdout/log-events/chia-plotter-i-0439182c39252eb07-stdout
      * Upload checkpoints to S3.
      * Fix timeouts waiting to put to buffer.
      * Maybe upload glog logs and aws logs with cloudwatch
    )";
    // gflags::AllowCommandLineReparsing();
    // gflags::ParseCommandLineFlags(argc, argv, true);
}

int main(int argc, char *argv[])
try {
    cxxopts::Options options(
        "ProofOfSpace", "Utility for plotting, generating and verifying proofs of space.");
    options.positional_help("(create/prove/verify/check/check-s3) param1 param2 ")
        .show_positional_help();

    // Default values
    uint8_t k = 20;
    uint32_t num_buckets = 0;
    uint32_t num_stripes = 0;
    uint8_t num_threads = 0;
    string filename = "plot.dat";
    string tempdir = ".";
    string operation = "help";
    string memo = "";
    string id = "";
    bool nobitfield = false;
    bool show_progress = false;
    int seed_arg = 0;
    uint32_t buffmegabytes = 0;
    bool qualities_only = false;

    options.allow_unrecognised_options().add_options()(
        "k, size", "Plot size", cxxopts::value<uint8_t>(k))(
        "r, threads", "Number of threads", cxxopts::value<uint8_t>(num_threads))(
        "u, buckets", "Number of buckets", cxxopts::value<uint32_t>(num_buckets))(
        "s, stripes", "Size of stripes", cxxopts::value<uint32_t>(num_stripes))(
        "t, tempdir", "Temporary directory", cxxopts::value<string>(tempdir))(
        "f, file", "Filename", cxxopts::value<string>(filename))(
        "m, memo", "Memo to insert into the plot", cxxopts::value<string>(memo))(
        "i, id", "Unique 32-byte seed for the plot", cxxopts::value<string>(id))(
        "e, nobitfield", "Disable bitfield", cxxopts::value<bool>(nobitfield))(
        "b, buffer",
        "Megabytes to be used as buffer for sorting and plotting",
        cxxopts::value<uint32_t>(buffmegabytes))(
        "p, progress",
        "Display progress percentage during plotting",
        cxxopts::value<bool>(show_progress))("help", "Print help")(
        "z, seed", "Random Seed", cxxopts::value<int>(seed_arg))(
        "q, qualities_only",
        "Only compute qualities, no proofs",
        cxxopts::value<bool>(qualities_only));

    auto result = options.parse(argc, argv);

    if (result.count("help") || argc < 2) {
        HelpAndQuit(options);
    }

    id = Strip0x(id);
    SetupLogging(id);

    operation = argv[1];
    std::cout << "operation: " << operation << std::endl;

    if (operation == "help") {
        HelpAndQuit(options);
    } else if (operation == "create") {
        cout << "Generating plot for k=" << static_cast<int>(k) << " filename=" << filename
             << " id=" << id << endl
             << endl;
        if (id.size() != 64) {
            cout << "Invalid ID, should be 32 bytes (hex), was " << id << endl;
            exit(1);
        }
        memo = Strip0x(memo);
        if (memo.size() % 2 != 0) {
            cout << "Invalid memo, should be only whole bytes (hex)" << endl;
            exit(1);
        }
        const auto memo_bytes = HexToBytes(memo);
        const auto id_bytes = HexToBytes(id);

        DiskPlotter plotter = DiskPlotter();
        uint8_t phases_flags = 0;
        if (!nobitfield) {
            phases_flags = ENABLE_BITFIELD;
        }
        if (show_progress) {
            phases_flags = phases_flags | SHOW_PROGRESS;
        }
        plotter.CreatePlotDisk(
            tempdir,
            filename,
            k,
            memo_bytes,
            id_bytes,
            buffmegabytes,
            num_buckets,
            num_stripes,
            num_threads,
            phases_flags);
    } else if (operation == "prove") {
        if (argc < 3) {
            HelpAndQuit(options);
        }
        cout << "Proving using filename=" << filename << " challenge=" << argv[2] << endl << endl;
        string challenge = Strip0x(argv[2]);
        if (challenge.size() != 64) {
            cout << "Invalid challenge, should be 32 bytes" << endl;
            exit(1);
        }
        const auto challenge_bytes = HexToBytes(challenge);

        DiskProver prover(filename);
        try {
            vector<LargeBits> qualities = prover.GetQualitiesForChallenge(challenge_bytes.data());
            for (uint32_t i = 0; i < qualities.size(); i++) {
                k = prover.GetSize();
                uint8_t *proof_data = new uint8_t[8 * k];
                LargeBits proof = prover.GetFullProof(challenge_bytes.data(), i);
                proof.ToBytes(proof_data);
                cout << "Proof: 0x" << Util::HexStr(proof_data, k * 8) << endl;
                delete[] proof_data;
            }
            if (qualities.empty()) {
                cout << "No proofs found." << endl;
                exit(1);
            }
        } catch (const std::exception &ex) {
            std::cout << "Error proving. " << ex.what() << std::endl;
            exit(1);
        } catch (...) {
            std::cout << "Error proving. " << std::endl;
            exit(1);
        }
    } else if (operation == "verify") {
        if (argc < 4) {
            HelpAndQuit(options);
        }
        Verifier verifier = Verifier();

        string proof = Strip0x(argv[2]);
        string challenge = Strip0x(argv[3]);
        if (id.size() != 64) {
            cout << "Invalid ID, should be 32 bytes" << endl;
            exit(1);
        }
        if (challenge.size() != 64) {
            cout << "Invalid challenge, should be 32 bytes" << endl;
            exit(1);
        }
        if (proof.size() % 16) {
            cout << "Invalid proof, should be a multiple of 8 bytes" << endl;
            exit(1);
        }
        k = proof.size() / 16;
        cout << "Verifying proof=" << argv[2] << " for challenge=" << argv[3]
             << " and k=" << static_cast<int>(k) << endl
             << endl;
        const auto id_bytes = HexToBytes(id);
        const auto challenge_bytes = HexToBytes(challenge);
        const auto proof_bytes = HexToBytes(proof);

        LargeBits quality = verifier.ValidateProof(
            id_bytes.data(), k, challenge_bytes.data(), proof_bytes.data(), k * 8);
        if (quality.GetSize() == 256) {
            cout << "Proof verification suceeded. Quality: " << quality << endl;
        } else {
            cout << "Proof verification failed." << endl;
            exit(1);
        }
    } else if (operation == "check") {
        uint32_t iterations = 1000;
        if (argc == 3) {
            iterations = std::stoi(argv[2]);
        }

        DiskProver prover(filename);
        Verifier verifier = Verifier();

        uint32_t success = 0;
        uint8_t id_bytes[32];
        prover.GetId(id_bytes);
        k = prover.GetSize();
        {
            CHECK_EQ(k, prover.GetSize());
        }

        for (uint32_t num = 0; num < iterations; num++) {
            vector<unsigned char> hash_input = intToBytes(num, 4);
            hash_input.insert(hash_input.end(), &id_bytes[0], &id_bytes[32]);

            vector<unsigned char> hash(picosha2::k_digest_size);
            picosha2::hash256(hash_input.begin(), hash_input.end(), hash.begin(), hash.end());

            try {
                vector<LargeBits> qualities = prover.GetQualitiesForChallenge(hash.data());

                for (uint32_t i = 0; i < qualities.size(); i++) {
                    LargeBits proof = prover.GetFullProof(hash.data(), i);
                    uint8_t *proof_data = new uint8_t[proof.GetSize() / 8];
                    proof.ToBytes(proof_data);
                    LargeBits quality =
                        verifier.ValidateProof(id_bytes, k, hash.data(), proof_data, k * 8);
                    if (quality.GetSize() == 256 && quality == qualities[i]) {
                        cout << "Proof verification suceeded. k = " << static_cast<int>(k) << endl;
                        success++;
                    } else {
                        cout << "i: " << num << std::endl;
                        cout << "challenge: 0x" << Util::HexStr(hash.data(), 256 / 8) << endl;
                        cout << "proof: 0x" << Util::HexStr(proof_data, k * 8) << endl;
                        cout << "quality: " << quality << endl;
                        cout << "Proof verification failed." << endl;
                    }
                    delete[] proof_data;
                }
            } catch (const std::exception &error) {
                cout << "Threw: " << error.what() << endl;
                continue;
            }
        }
        std::cout << "Total success: " << success << "/" << iterations << ", "
                  << (success * 100 / static_cast<double>(iterations)) << "%." << std::endl;
        if (show_progress) {
            progress(4, 1, 1);
        }
    } else if (operation == "check-s3") {
        uint32_t iterations = 10;
        if (argc == 3) {
            iterations = std::stoi(argv[2]);
        }

        std::ifstream manifest_stream(filename);
        std::string manifest_json_str{
            std::istreambuf_iterator<char>(manifest_stream), std::istreambuf_iterator<char>()};

        S3Prover prover(manifest_json_str);
        Verifier verifier = Verifier();

        uint32_t success = 0;
        uint8_t id_bytes[32];
        prover.GetId(id_bytes);
        k = prover.GetSize();
        const auto seed = seed_arg == 0 ? time(nullptr) : seed_arg;
        std::cout << "seeding with " << seed << "\n";
        srand(seed);

        std::vector<uint8_t> proof_data;
        std::vector<double> qualities_times;
        std::vector<double> full_proof_times;
        std::vector<int> num_proofs_per_challenge;

        for (uint32_t num = 0; num < iterations; num++) {
            std::vector<unsigned char> hash_input = intToBytes(std::rand(), 4);
            hash_input.insert(hash_input.end(), &id_bytes[0], &id_bytes[32]);

            std::vector<unsigned char> hash(picosha2::k_digest_size);
            picosha2::hash256(hash_input.begin(), hash_input.end(), hash.begin(), hash.end());

            try {
                const auto time_before_qualities = get_now_time();
                const auto [qualities, challenge_p7_entries] =
                    prover.GetQualitiesAndP7EntriesForChallenge(hash.data());
                const auto quality_time = get_now_time() - time_before_qualities;
                qualities_times.push_back(quality_time);
                if (qualities_only) {
                    continue;
                }

                double total_proof_time = quality_time;
                for (uint32_t i = 0; i < qualities.size(); i++) {
                    const auto time_before_proof = get_now_time();
                    const auto [error_message, proof] =
                        prover.GetFullProofFromP7Entry(challenge_p7_entries[i]);
                    if (!error_message.empty()) {
                        std::cerr << "Proof verification failed: " << error_message << endl;
                        exit(1);
                    }
                    total_proof_time += (get_now_time() - time_before_proof);

                    cout << "num=" << num << ", i=" << i << std::endl;
                    proof_data.resize(proof.GetSize() / 8);
                    proof.ToBytes(proof_data.data());
                    LargeBits quality =
                        verifier.ValidateProof(id_bytes, k, hash.data(), proof_data.data(), k * 8);
                    if (quality.GetSize() == 256 && quality == qualities[i]) {
                        cout << "Proof verification suceeded. k = " << static_cast<int>(k) << endl;
                        success++;
                    } else {
                        cout << "challenge: 0x" << Util::HexStr(hash.data(), 256 / 8) << endl;
                        cout << "proof: 0x" << Util::HexStr(proof_data.data(), k * 8) << endl;
                        cout << "quality: " << quality << endl;
                        cout << "Proof verification failed." << endl;
                        exit(1);
                    }
                }

                full_proof_times.push_back(total_proof_time);
                num_proofs_per_challenge.push_back(qualities.size());
            } catch (const std::exception &error) {
                cout << "Threw: " << error.what() << endl;
                continue;
            }
        }
        std::cout << "Total success: " << success << "/" << iterations << ", "
                  << (success * 100 / static_cast<double>(iterations)) << "%." << std::endl;

        const auto total_proofs =
            std::reduce(num_proofs_per_challenge.begin(), num_proofs_per_challenge.end());

        std::cout << "Time Stats (n=" << iterations << ")" << std::endl;
        std::cout << "  Avg Quality Time: "
                  << (qualities_times.empty()
                          ? 0
                          : std::reduce(qualities_times.begin(), qualities_times.end()) /
                                qualities_times.size())
                  << " (" << std::reduce(qualities_times.begin(), qualities_times.end()) << ") / "
                  << qualities_times.size() << std::endl;
        std::cout << "  Avg Proof Time: "
                  << (total_proofs == 0
                          ? 0
                          : std::reduce(full_proof_times.begin(), full_proof_times.end()) /
                                total_proofs)
                  << " (" << std::reduce(full_proof_times.begin(), full_proof_times.end()) << ") / "
                  << total_proofs << std::endl;

        int num_nonempty_challenges = 0;
        double total_nonempty_time = 0;
        for (size_t i = 0; i < full_proof_times.size(); ++i) {
            if (num_proofs_per_challenge[i] > 0) {
                total_nonempty_time += full_proof_times[i];
                num_nonempty_challenges += 1;
            }
        }

        std::cout << "  Avg Proof Time by challenge: "
                  << (num_nonempty_challenges == 0 ? 0
                                                   : total_nonempty_time / num_nonempty_challenges)
                  << " (" << total_nonempty_time << ") / " << num_nonempty_challenges << std::endl;

        if (show_progress) {
            progress(4, 1, 1);
        }
    } else if (operation == "show-info") {
        std::cout << "buffer lcm alignments k=19 through k=33" << std::endl;
        std::cout << "{\n";
        uint32_t overall_lcm = 1;
        for (int k = 19; k <= 33; ++k) {
            uint32_t k_lcm = 1;
            for (int table_index = 1; table_index <= 7; ++table_index) {
                for (bool phase_1_size : {false, true}) {
                    const uint32_t max_entry_size =
                        EntrySizes::GetMaxEntrySize(k, table_index, phase_1_size);
                    overall_lcm = std::lcm(overall_lcm, max_entry_size);
                    k_lcm = std::lcm(k_lcm, max_entry_size);
                }
                int16_t const phase2_entry_size =
                    cdiv(k + kOffsetSize + (table_index == 7 ? k : 0), 8);
                k_lcm = std::lcm(k_lcm, phase2_entry_size);
            }

            std::cout << "  " << k_lcm << ",\n";
        }
        std::cout << "}\n";
        std::cout << "overall lcm=" << overall_lcm << std::endl;

        std::cout << "Max entry sizes\n";
        for (int k = 19; k <= 33; ++k) {
            for (int table_index = 1; table_index <= 7; ++table_index) {
                for (bool phase_1_size : {false, true}) {
                    const uint32_t max_entry_size =
                        EntrySizes::GetMaxEntrySize(k, table_index, phase_1_size);
                    std::cout << fmt::format(
                        "entry size k={} table_index={} phase={} -> {}\n",
                        k,
                        table_index,
                        phase_1_size,
                        max_entry_size);
                }
            }

            const uint8_t phase2_entry_size = EntrySizes::GetKeyPosOffsetSize(k);
            std::cout << fmt::format("phase2 entry size k={} -> {}\n", k, phase2_entry_size);
        }
    } else {
        cout << "Invalid operation. Use create/prove/verify/check" << endl;
        return 1;
    }

    return 0;
} catch (const cxxopts::OptionException &e) {
    cout << "error parsing options: " << e.what() << endl;
    return 1;
}
