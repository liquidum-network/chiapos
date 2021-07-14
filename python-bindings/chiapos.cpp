#ifndef PYTHON_BINDINGS_PYTHON_BINDINGS_HPP_
#define PYTHON_BINDINGS_PYTHON_BINDINGS_HPP_

#if __has_include(<optional>)

#include <optional>
namespace stdx {
using std::optional;
}

#elif __has_include(<experimental/optional>)

#include <experimental/optional>
namespace stdx {
using std::experimental::optional;
}

#else
#error "an implementation of optional is required!"
#endif

#include <pybind11/operators.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "../src/prover_disk.hpp"
#include "../src/prover_s3.hpp"
#include "../src/verifier.hpp"

namespace py = pybind11;

PYBIND11_MODULE(chiapos, m)
{
    m.doc() = "Chia Proof of Space";

    py::class_<DiskProver>(m, "DiskProver")
        .def(py::init<const std::string &>())
        .def(
            "get_memo",
            [](DiskProver &dp) {
                uint8_t *memo = new uint8_t[dp.GetMemoSize()];
                dp.GetMemo(memo);
                py::bytes ret = py::bytes(reinterpret_cast<char *>(memo), dp.GetMemoSize());
                delete[] memo;
                return ret;
            })
        .def(
            "get_id",
            [](DiskProver &dp) {
                uint8_t *id = new uint8_t[kIdLen];
                dp.GetId(id);
                py::bytes ret = py::bytes(reinterpret_cast<char *>(id), kIdLen);
                delete[] id;
                return ret;
            })
        .def("get_size", [](DiskProver &dp) { return dp.GetSize(); })
        .def("get_filename", [](DiskProver &dp) { return dp.GetFilename(); })
        .def(
            "get_qualities_for_challenge",
            [](DiskProver &dp, const py::bytes &challenge) {
                if (len(challenge) != 32) {
                    throw std::invalid_argument("Challenge must be exactly 32 bytes");
                }
                std::string challenge_str(challenge);
                const uint8_t *challenge_ptr =
                    reinterpret_cast<const uint8_t *>(challenge_str.data());
                py::gil_scoped_release release;
                std::vector<LargeBits> qualities = dp.GetQualitiesForChallenge(challenge_ptr);
                py::gil_scoped_acquire acquire;
                std::vector<py::bytes> ret;
                uint8_t *quality_buf = new uint8_t[32];
                for (LargeBits quality : qualities) {
                    quality.ToBytes(quality_buf);
                    py::bytes quality_py = py::bytes(reinterpret_cast<char *>(quality_buf), 32);
                    ret.push_back(quality_py);
                }
                delete[] quality_buf;
                return ret;
            })
        .def("get_full_proof", [](DiskProver &dp, const py::bytes &challenge, uint32_t index) {
            std::string challenge_str(challenge);
            const uint8_t *challenge_ptr = reinterpret_cast<const uint8_t *>(challenge_str.data());
            py::gil_scoped_release release;
            LargeBits proof = dp.GetFullProof(challenge_ptr, index);
            py::gil_scoped_acquire acquire;
            uint8_t *proof_buf = new uint8_t[Util::ByteAlign(64 * dp.GetSize()) / 8];
            proof.ToBytes(proof_buf);
            py::bytes ret = py::bytes(
                reinterpret_cast<char *>(proof_buf), Util::ByteAlign(64 * dp.GetSize()) / 8);
            delete[] proof_buf;
            return ret;
        });

    py::class_<S3Prover>(m, "S3Prover")
        .def(py::init<const std::string &>())
        .def(
            "get_memo",
            [](S3Prover &dp) {
                uint8_t *memo = new uint8_t[dp.GetMemoSize()];
                dp.GetMemo(memo);
                py::bytes ret = py::bytes(reinterpret_cast<char *>(memo), dp.GetMemoSize());
                delete[] memo;
                return ret;
            })
        .def(
            "get_id",
            [](S3Prover &dp) {
                uint8_t id[kIdLen];
                dp.GetId(id);
                py::bytes ret = py::bytes(reinterpret_cast<char *>(id), kIdLen);
                return ret;
            })
        .def("get_size", [](S3Prover &dp) { return dp.GetSize(); })
        .def("get_file_size", [](S3Prover &dp) { return dp.GetFileSize(); })
        .def(
            "get_qualities_and_p7_entries_for_challenge",
            [](S3Prover &dp, const py::bytes &challenge) {
                if (len(challenge) != 32) {
                    throw std::invalid_argument("Challenge must be exactly 32 bytes");
                }
                std::string challenge_str(challenge);
                const uint8_t *challenge_ptr =
                    reinterpret_cast<const uint8_t *>(challenge_str.data());

                py::gil_scoped_release release;
                auto [qualities, p7_entries] =
                    dp.GetQualitiesAndP7EntriesForChallenge(challenge_ptr);
                py::gil_scoped_acquire acquire;

                std::vector<py::bytes> py_qualities;
                uint8_t quality_buf[32];
                for (const LargeBits &quality : qualities) {
                    quality.ToBytes(quality_buf);
                    py_qualities.push_back(py::bytes(reinterpret_cast<char *>(quality_buf), 32));
                }
                py::list py_p7_entries;
                for (const uint64_t entry : p7_entries) {
                    py_p7_entries.append(entry);
                }

                return py::make_tuple(py_qualities, py_p7_entries);
            })
        .def(
            "get_full_proof_from_p7_entry",
            [](S3Prover &dp, uint64_t p7_entry) {
                py::gil_scoped_release release;
                auto [error_message, proof] = dp.GetFullProofFromP7Entry(p7_entry);
                std::vector<uint8_t> proof_buf(Util::ByteAlign(64 * dp.GetSize()) / 8);
                if (error_message.empty()) {
                    proof.ToBytes(proof_buf.data());
                }
                py::gil_scoped_acquire acquire;

                if (!error_message.empty()) {
                    throw std::runtime_error(error_message);
                    proof.ToBytes(proof_buf.data());
                }

                return py::bytes(
                    reinterpret_cast<const char *>(proof_buf.data()),
                    Util::ByteAlign(64 * dp.GetSize()) / 8);
            })
        .def("get_full_proofs_from_p7_entries", [](S3Prover &dp, std::vector<uint64_t> p7_entries) {
            py::gil_scoped_release release;
            const auto proofs_with_errors = dp.GetFullProofsFromP7Entries(p7_entries);
            py::gil_scoped_acquire acquire;

            py::list result;

            std::vector<uint8_t> proof_buf(Util::ByteAlign(64 * dp.GetSize()) / 8);
            for (const auto &[error_message, proof] : proofs_with_errors) {
                proof.ToBytes(proof_buf.data());
                result.append(py::make_tuple(
                    py::str(error_message),
                    py::bytes(
                        reinterpret_cast<const char *>(proof_buf.data()),
                        Util::ByteAlign(64 * dp.GetSize()) / 8)));
            }

            return result;
        });

    py::class_<Verifier>(m, "Verifier")
        .def(py::init<>())
        .def(
            "validate_proof",
            [](Verifier &v,
               const py::bytes &seed,
               uint8_t k,
               const py::bytes &challenge,
               const py::bytes &proof) {
                std::string seed_str(seed);
                const uint8_t *seed_ptr = reinterpret_cast<const uint8_t *>(seed_str.data());

                std::string challenge_str(challenge);
                const uint8_t *challenge_ptr =
                    reinterpret_cast<const uint8_t *>(challenge_str.data());

                std::string proof_str(proof);
                const uint8_t *proof_ptr = reinterpret_cast<const uint8_t *>(proof_str.data());

                // py::gil_scoped_release release;
                LargeBits quality =
                    v.ValidateProof(seed_ptr, k, challenge_ptr, proof_ptr, len(proof));
                // py::gil_scoped_acquire acquire;
                if (quality.GetSize() == 0) {
                    return stdx::optional<py::bytes>();
                }
                uint8_t *quality_buf = new uint8_t[32];
                quality.ToBytes(quality_buf);
                py::bytes quality_py = py::bytes(reinterpret_cast<char *>(quality_buf), 32);
                delete[] quality_buf;
                return stdx::optional<py::bytes>(quality_py);
            });
}

#endif  // PYTHON_BINDINGS_PYTHON_BINDINGS_HPP_
