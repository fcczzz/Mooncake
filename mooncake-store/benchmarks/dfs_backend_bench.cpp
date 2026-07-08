#include <gflags/gflags.h>
#include <glog/logging.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <numeric>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "replica.h"
#include "storage/distributed/dfs_descriptor_cache.h"
#include "storage/distributed/dfs_global_allocator.h"
#include "storage/distributed/distributed_storage_backend.h"
#include "storage/distributed/fs_adapter.h"
#include "storage/distributed/posix_fs_adapter.h"
#include "storage_backend.h"
#include "types.h"

#ifdef USE_3FS
#include <limits.h>

#include "hf3fs/hf3fs.h"
#include "storage/distributed/hf3fs_adapter.h"
#endif

DEFINE_string(fs_adapter, "posix", "DFS adapter: posix or hf3fs");
DEFINE_string(mode, "backend", "Benchmark mode: adapter or backend");
DEFINE_string(test, "all", "Test phase: write, read, or all");
DEFINE_string(storage_path, "/tmp/mooncake_dfs_backend_bench",
              "Parent directory for benchmark shard files");
DEFINE_uint64(value_size, 128 * 1024, "Object value size in bytes");
DEFINE_uint64(num_objects, 1024, "Number of objects");
DEFINE_uint64(batch_size, 32, "Objects per measured request");
DEFINE_uint64(num_threads, 1, "Worker threads");
DEFINE_int32(shard_count, 4, "DFS shard count");
DEFINE_uint64(shard_capacity, 1ULL << 30, "Capacity per DFS shard");
DEFINE_uint64(alignment, 4096, "DFS object alignment");
DEFINE_uint64(warmup_batches, 0, "Unmeasured batches before each phase");
DEFINE_bool(verify, false, "Verify payloads after reads");
DEFINE_bool(skip_cleanup, false, "Keep benchmark files after exit");

namespace mooncake {
namespace {

struct ObjectLayout {
    std::string key;
    std::string shard_path;
    uint64_t offset = 0;
    int shard_idx = 0;
};

struct PhaseStats {
    uint64_t requests = 0;
    uint64_t objects = 0;
    uint64_t errors = 0;
    uint64_t bytes = 0;
    double duration_s = 0.0;
    std::vector<double> latencies_us;
};

class AlignedBuffer {
   public:
    AlignedBuffer() = default;

    AlignedBuffer(size_t size, size_t alignment) { Reset(size, alignment); }

    ~AlignedBuffer() { std::free(ptr_); }

    AlignedBuffer(const AlignedBuffer&) = delete;
    AlignedBuffer& operator=(const AlignedBuffer&) = delete;

    void Reset(size_t size, size_t alignment) {
        std::free(ptr_);
        ptr_ = nullptr;
        size_ = 0;
        if (size == 0) return;
        if (alignment < sizeof(void*)) alignment = sizeof(void*);
        void* raw = nullptr;
        if (::posix_memalign(&raw, alignment, size) != 0 || raw == nullptr) {
            throw std::bad_alloc();
        }
        ptr_ = static_cast<char*>(raw);
        size_ = size;
    }

    char* data() { return ptr_; }
    const char* data() const { return ptr_; }
    size_t size() const { return size_; }

   private:
    char* ptr_ = nullptr;
    size_t size_ = 0;
};

bool IsPowerOfTwo(uint64_t value) {
    return value != 0 && (value & (value - 1)) == 0;
}

uint64_t AlignUp(uint64_t value, uint64_t alignment) {
    return (value + alignment - 1) & ~(alignment - 1);
}

std::string AbsolutePath(const std::string& path) {
    std::error_code ec;
    auto abs = std::filesystem::absolute(path, ec);
    return ec ? path : abs.string();
}

std::string MakeRunDir(const std::string& storage_path) {
    const auto now = std::chrono::system_clock::now().time_since_epoch();
    const auto ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(now).count();
    std::filesystem::path root = AbsolutePath(storage_path);
    return (root / ("dfs_backend_bench_" + std::to_string(::getpid()) + "_" +
                    std::to_string(ns)))
        .string();
}

std::unique_ptr<FileSystemAdapter> MakeAdapter(const std::string& name) {
    if (name == "posix") return std::make_unique<PosixFsAdapter>();
    if (name == "hf3fs") {
#ifdef USE_3FS
        return std::make_unique<Hf3fsAdapter>();
#else
        throw std::runtime_error(
            "--fs_adapter=hf3fs requires building with USE_3FS=ON");
#endif
    }
    throw std::runtime_error("unsupported --fs_adapter=" + name);
}

#ifdef USE_3FS
std::optional<std::string> ExistingAncestor(std::filesystem::path path) {
    if (path.empty()) return std::nullopt;
    std::error_code ec;
    path = std::filesystem::absolute(path, ec);
    if (ec) return std::nullopt;
    while (!path.empty()) {
        if (std::filesystem::exists(path, ec) && !ec) return path.string();
        auto parent = path.parent_path();
        if (parent == path) break;
        path = parent;
    }
    return std::nullopt;
}

bool IsHf3fsPath(const std::string& path) {
    char mount_point[PATH_MAX] = {};
    int ret = hf3fs_extract_mount_point(mount_point, sizeof(mount_point),
                                        path.c_str());
    return ret > 0 && ret <= static_cast<int>(sizeof(mount_point));
}
#endif

void ValidateHf3fsStoragePath(const std::string& path) {
    if (FLAGS_fs_adapter != "hf3fs") return;
#ifndef USE_3FS
    throw std::runtime_error(
        "--fs_adapter=hf3fs requires building with USE_3FS=ON");
#else
    auto ancestor = ExistingAncestor(path);
    if (!ancestor.has_value()) {
        throw std::runtime_error("no existing ancestor for --storage_path=" +
                                 path);
    }
    if (!IsHf3fsPath(*ancestor)) {
        throw std::runtime_error("--storage_path is not under an HF3FS mount: " +
                                 path + " (checked ancestor " + *ancestor +
                                 ")");
    }
#endif
}

std::string KeyFor(uint64_t object_idx) {
    std::ostringstream oss;
    oss << "dfs_backend_bench_key_" << std::setw(16) << std::setfill('0')
        << object_idx;
    return oss.str();
}

void FillPayload(uint64_t object_idx, char* dst, size_t len) {
    if (len == 0) return;
    const auto fill = static_cast<unsigned char>((object_idx * 1315423911ULL) &
                                                 0xFF);
    std::memset(dst, fill, len);
    if (len >= sizeof(uint64_t)) {
        std::memcpy(dst, &object_idx, sizeof(object_idx));
    }
    if (len >= 2 * sizeof(uint64_t)) {
        const uint64_t inverse = ~object_idx;
        std::memcpy(dst + sizeof(uint64_t), &inverse, sizeof(inverse));
    }
    dst[len - 1] = static_cast<char>(fill ^ 0x5A);
}

bool VerifyPayload(uint64_t object_idx, const char* src, size_t len) {
    if (len == 0) return true;
    const auto fill = static_cast<unsigned char>((object_idx * 1315423911ULL) &
                                                 0xFF);
    if (len >= sizeof(uint64_t)) {
        uint64_t got = 0;
        std::memcpy(&got, src, sizeof(got));
        if (got != object_idx) return false;
    }
    if (len >= 2 * sizeof(uint64_t)) {
        uint64_t got = 0;
        std::memcpy(&got, src + sizeof(uint64_t), sizeof(got));
        if (got != ~object_idx) return false;
    }
    return src[len - 1] == static_cast<char>(fill ^ 0x5A);
}

std::vector<ObjectLayout> BuildLayout(const std::string& run_dir,
                                      uint64_t aligned_size) {
    std::vector<ObjectLayout> layout;
    layout.reserve(FLAGS_num_objects);
    for (uint64_t i = 0; i < FLAGS_num_objects; ++i) {
        const int shard_idx = static_cast<int>(i % FLAGS_shard_count);
        const uint64_t slot = i / FLAGS_shard_count;
        std::string shard_path =
            run_dir + "/dfs_shard_" +
            DfsGlobalAllocator::FormatShardIdx(shard_idx, FLAGS_shard_count) +
            ".data";
        layout.push_back(
            {KeyFor(i), std::move(shard_path), slot * aligned_size, shard_idx});
    }
    return layout;
}

void PreallocateShards(const std::string& run_dir) {
    std::error_code ec;
    std::filesystem::create_directories(run_dir, ec);
    if (ec) {
        throw std::runtime_error("failed to create run dir " + run_dir + ": " +
                                 ec.message());
    }

    auto adapter = MakeAdapter(FLAGS_fs_adapter);
    auto init = adapter->Init(run_dir);
    if (!init) throw std::runtime_error("adapter Init failed before prealloc");

    for (int i = 0; i < FLAGS_shard_count; ++i) {
        const std::string path =
            run_dir + "/dfs_shard_" +
            DfsGlobalAllocator::FormatShardIdx(i, FLAGS_shard_count) + ".data";
        auto prealloc = adapter->PreallocateFile(path, FLAGS_shard_capacity);
        if (!prealloc) {
            throw std::runtime_error("failed to preallocate shard " + path);
        }
    }

    auto shutdown = adapter->Shutdown();
    if (!shutdown) throw std::runtime_error("adapter Shutdown failed");
}

double Percentile(std::vector<double> values, double percentile) {
    if (values.empty()) return 0.0;
    std::sort(values.begin(), values.end());
    if (values.size() == 1) return values[0];
    const double rank = (values.size() - 1) * percentile;
    const size_t lo = static_cast<size_t>(std::floor(rank));
    const size_t hi = static_cast<size_t>(std::ceil(rank));
    if (lo == hi) return values[lo];
    return values[lo] + (values[hi] - values[lo]) * (rank - lo);
}

void PrintStats(const std::string& phase, const std::string& mode,
                const std::string& adapter, const PhaseStats& stats) {
    const double mib =
        static_cast<double>(stats.bytes) / static_cast<double>(1024 * 1024);
    const double mib_s = stats.duration_s > 0.0 ? mib / stats.duration_s : 0.0;
    const double objects_s = stats.duration_s > 0.0
                                 ? static_cast<double>(stats.objects) /
                                       stats.duration_s
                                 : 0.0;
    const double mean =
        stats.latencies_us.empty()
            ? 0.0
            : std::accumulate(stats.latencies_us.begin(),
                              stats.latencies_us.end(), 0.0) /
                  static_cast<double>(stats.latencies_us.size());
    const double p50 = Percentile(stats.latencies_us, 0.50);
    const double p95 = Percentile(stats.latencies_us, 0.95);
    const double p99 = Percentile(stats.latencies_us, 0.99);
    const double max = stats.latencies_us.empty()
                           ? 0.0
                           : *std::max_element(stats.latencies_us.begin(),
                                               stats.latencies_us.end());

    std::cout << "phase=" << phase << " mode=" << mode
              << " fs_adapter=" << adapter << " requests=" << stats.requests
              << " objects=" << stats.objects << " errors=" << stats.errors
              << " bytes=" << stats.bytes << std::fixed
              << std::setprecision(6) << " duration_s=" << stats.duration_s
              << " MiB/s=" << mib_s << " objects/s=" << objects_s
              << " lat_us_mean=" << mean << " lat_us_p50=" << p50
              << " lat_us_p95=" << p95 << " lat_us_p99=" << p99
              << " lat_us_max=" << max << std::endl;
}

PhaseStats MergeStats(const std::vector<PhaseStats>& parts,
                      double duration_s) {
    PhaseStats out;
    out.duration_s = duration_s;
    for (const auto& part : parts) {
        out.requests += part.requests;
        out.objects += part.objects;
        out.errors += part.errors;
        out.bytes += part.bytes;
        out.latencies_us.insert(out.latencies_us.end(), part.latencies_us.begin(),
                                part.latencies_us.end());
    }
    return out;
}

class AdapterRunner {
   public:
    AdapterRunner(std::string run_dir, std::vector<ObjectLayout> layout)
        : run_dir_(std::move(run_dir)), layout_(std::move(layout)) {}

    void Init() {
        adapter_ = MakeAdapter(FLAGS_fs_adapter);
        auto init = adapter_->Init(run_dir_);
        if (!init) throw std::runtime_error("adapter Init failed");
        shard_fds_.reserve(FLAGS_shard_count);
        for (int i = 0; i < FLAGS_shard_count; ++i) {
            const std::string path =
                run_dir_ + "/dfs_shard_" +
                DfsGlobalAllocator::FormatShardIdx(i, FLAGS_shard_count) +
                ".data";
            auto fd = adapter_->OpenFile(path);
            if (!fd) {
                throw std::runtime_error("adapter OpenFile failed: " + path);
            }
            shard_fds_.push_back(*fd);
        }
    }

    ~AdapterRunner() {
        if (adapter_) {
            for (int fd : shard_fds_) {
                if (fd >= 0) adapter_->CloseFile(fd);
            }
            adapter_->Shutdown();
        }
    }

    bool WriteOne(uint64_t object_idx, const char* data, size_t size) {
        const auto& obj = layout_[object_idx];
        iovec iov{const_cast<char*>(data), size};
        auto written =
            adapter_->WriteAt(shard_fds_[obj.shard_idx], &iov, 1, obj.offset);
        return written && *written == size;
    }

    bool ReadOne(uint64_t object_idx, char* data, size_t size) {
        const auto& obj = layout_[object_idx];
        iovec iov{data, size};
        auto read =
            adapter_->ReadAt(shard_fds_[obj.shard_idx], &iov, 1, obj.offset);
        return read && *read == size;
    }

   private:
    std::string run_dir_;
    std::vector<ObjectLayout> layout_;
    std::unique_ptr<FileSystemAdapter> adapter_;
    std::vector<int> shard_fds_;
};

class BackendRunner {
   public:
    BackendRunner(std::string run_dir, std::vector<ObjectLayout> layout)
        : run_dir_(std::move(run_dir)), layout_(std::move(layout)) {}

    void Init() {
        FileStorageConfig file_config;
        file_config.storage_backend_type = StorageBackendType::kDistributed;
        file_config.storage_filepath = run_dir_;
        file_config.enable_dfs = true;

        DistributedStorageConfig distributed_config;
        distributed_config.fsdir = run_dir_;
        distributed_config.fs_adapter_type = FLAGS_fs_adapter;
        distributed_config.shard_count = FLAGS_shard_count;
        distributed_config.shard_capacity = FLAGS_shard_capacity;
        distributed_config.alignment = FLAGS_alignment;
        distributed_config.single_tenant = true;

        desc_cache_ = std::make_shared<DfsDescriptorCache>();
        for (const auto& obj : layout_) {
            desc_cache_->Put(
                obj.key, {obj.shard_path, obj.offset, FLAGS_value_size,
                          AlignUp(FLAGS_value_size, FLAGS_alignment),
                          obj.shard_idx});
        }

        backend_ = std::make_unique<DistributedStorageBackend>(
            file_config, distributed_config, MakeAdapter(FLAGS_fs_adapter));
        backend_->SetDescriptorCache(desc_cache_);
        auto init = backend_->Init();
        if (!init) {
            throw std::runtime_error(
                "DistributedStorageBackend Init failed");
        }
    }

    bool WriteBatch(const std::vector<uint64_t>& object_ids, char* base) {
        std::unordered_map<std::string, std::vector<Slice>> batch;
        batch.reserve(object_ids.size());
        for (size_t i = 0; i < object_ids.size(); ++i) {
            const uint64_t object_idx = object_ids[i];
            batch[layout_[object_idx].key] = {
                Slice{base + i * FLAGS_value_size,
                      static_cast<size_t>(FLAGS_value_size)}};
        }

        auto result = backend_->BatchOffload(
            batch,
            [](const std::vector<std::string>&,
               std::vector<StorageObjectMetadata>&) { return ErrorCode::OK; });
        return result && *result == static_cast<int64_t>(object_ids.size());
    }

    bool ReadBatch(const std::vector<uint64_t>& object_ids, char* base) {
        std::unordered_map<std::string, Slice> batch;
        batch.reserve(object_ids.size());
        for (size_t i = 0; i < object_ids.size(); ++i) {
            const uint64_t object_idx = object_ids[i];
            batch[layout_[object_idx].key] =
                Slice{base + i * FLAGS_value_size,
                      static_cast<size_t>(FLAGS_value_size)};
        }
        auto result = backend_->BatchLoad(batch);
        return result.has_value();
    }

   private:
    std::string run_dir_;
    std::vector<ObjectLayout> layout_;
    std::shared_ptr<DfsDescriptorCache> desc_cache_;
    std::unique_ptr<DistributedStorageBackend> backend_;
};

template <typename Runner>
void WriteObjects(Runner& runner, const std::vector<uint64_t>& ids) {
    AlignedBuffer buffer(ids.size() * FLAGS_value_size, FLAGS_alignment);
    for (size_t i = 0; i < ids.size(); ++i) {
        FillPayload(ids[i], buffer.data() + i * FLAGS_value_size,
                    FLAGS_value_size);
    }
    if constexpr (std::is_same_v<Runner, AdapterRunner>) {
        for (size_t i = 0; i < ids.size(); ++i) {
            if (!runner.WriteOne(ids[i], buffer.data() + i * FLAGS_value_size,
                                 FLAGS_value_size)) {
                throw std::runtime_error("warmup adapter write failed");
            }
        }
    } else {
        if (!runner.WriteBatch(ids, buffer.data())) {
            throw std::runtime_error("warmup backend write failed");
        }
    }
}

template <typename Runner>
void ReadObjects(Runner& runner, const std::vector<uint64_t>& ids) {
    AlignedBuffer buffer(ids.size() * FLAGS_value_size, FLAGS_alignment);
    if constexpr (std::is_same_v<Runner, AdapterRunner>) {
        for (size_t i = 0; i < ids.size(); ++i) {
            if (!runner.ReadOne(ids[i], buffer.data() + i * FLAGS_value_size,
                                FLAGS_value_size)) {
                throw std::runtime_error("warmup adapter read failed");
            }
        }
    } else {
        if (!runner.ReadBatch(ids, buffer.data())) {
            throw std::runtime_error("warmup backend read failed");
        }
    }
}

std::vector<uint64_t> IdRange(uint64_t begin, uint64_t end) {
    end = std::min(end, FLAGS_num_objects);
    std::vector<uint64_t> ids;
    ids.reserve(end - begin);
    for (uint64_t i = begin; i < end; ++i) ids.push_back(i);
    return ids;
}

template <typename Runner>
void WriteFirstObjects(Runner& runner, uint64_t count) {
    count = std::min(count, FLAGS_num_objects);
    for (uint64_t begin = 0; begin < count; begin += FLAGS_batch_size) {
        WriteObjects(runner, IdRange(begin, begin + FLAGS_batch_size));
    }
}

template <typename Runner>
void ReadFirstObjects(Runner& runner, uint64_t count) {
    count = std::min(count, FLAGS_num_objects);
    for (uint64_t begin = 0; begin < count; begin += FLAGS_batch_size) {
        ReadObjects(runner, IdRange(begin, begin + FLAGS_batch_size));
    }
}

std::vector<uint64_t> NextBatch(std::atomic<uint64_t>& next_object) {
    const uint64_t begin = next_object.fetch_add(FLAGS_batch_size);
    if (begin >= FLAGS_num_objects) return {};
    return IdRange(begin, begin + FLAGS_batch_size);
}

template <typename Runner>
PhaseStats RunWritePhase(Runner& runner) {
    const uint64_t warmup_count = FLAGS_warmup_batches * FLAGS_batch_size;
    if (warmup_count > 0) WriteFirstObjects(runner, warmup_count);

    std::atomic<uint64_t> next_object{0};
    std::vector<PhaseStats> per_thread(FLAGS_num_threads);
    std::vector<std::thread> threads;
    threads.reserve(FLAGS_num_threads);

    const auto start = std::chrono::steady_clock::now();
    for (uint64_t thread_idx = 0; thread_idx < FLAGS_num_threads; ++thread_idx) {
        threads.emplace_back([&runner, &next_object, &per_thread, thread_idx] {
            auto& stats = per_thread[thread_idx];
            AlignedBuffer buffer(FLAGS_batch_size * FLAGS_value_size,
                                 FLAGS_alignment);
            while (true) {
                auto ids = NextBatch(next_object);
                if (ids.empty()) break;
                for (size_t i = 0; i < ids.size(); ++i) {
                    FillPayload(ids[i], buffer.data() + i * FLAGS_value_size,
                                FLAGS_value_size);
                }

                const auto op_start = std::chrono::steady_clock::now();
                uint64_t ok = 0;
                if constexpr (std::is_same_v<Runner, AdapterRunner>) {
                    for (size_t i = 0; i < ids.size(); ++i) {
                        if (runner.WriteOne(ids[i],
                                            buffer.data() +
                                                i * FLAGS_value_size,
                                            FLAGS_value_size)) {
                            ++ok;
                        }
                    }
                } else {
                    ok = runner.WriteBatch(ids, buffer.data()) ? ids.size() : 0;
                }
                const auto op_end = std::chrono::steady_clock::now();

                stats.requests += 1;
                stats.objects += ok;
                stats.errors += ids.size() - ok;
                stats.bytes += ok * FLAGS_value_size;
                stats.latencies_us.push_back(
                    std::chrono::duration<double, std::micro>(op_end - op_start)
                        .count());
            }
        });
    }
    for (auto& thread : threads) thread.join();
    const auto end = std::chrono::steady_clock::now();
    return MergeStats(per_thread,
                      std::chrono::duration<double>(end - start).count());
}

template <typename Runner>
PhaseStats RunReadPhase(Runner& runner) {
    const uint64_t warmup_count = FLAGS_warmup_batches * FLAGS_batch_size;
    if (warmup_count > 0) ReadFirstObjects(runner, warmup_count);

    std::atomic<uint64_t> next_object{0};
    std::vector<PhaseStats> per_thread(FLAGS_num_threads);
    std::vector<std::thread> threads;
    threads.reserve(FLAGS_num_threads);

    const auto start = std::chrono::steady_clock::now();
    for (uint64_t thread_idx = 0; thread_idx < FLAGS_num_threads; ++thread_idx) {
        threads.emplace_back([&runner, &next_object, &per_thread, thread_idx] {
            auto& stats = per_thread[thread_idx];
            AlignedBuffer buffer(FLAGS_batch_size * FLAGS_value_size,
                                 FLAGS_alignment);
            while (true) {
                auto ids = NextBatch(next_object);
                if (ids.empty()) break;
                std::memset(buffer.data(), 0, ids.size() * FLAGS_value_size);

                const auto op_start = std::chrono::steady_clock::now();
                uint64_t ok = 0;
                if constexpr (std::is_same_v<Runner, AdapterRunner>) {
                    for (size_t i = 0; i < ids.size(); ++i) {
                        if (runner.ReadOne(ids[i],
                                           buffer.data() + i * FLAGS_value_size,
                                           FLAGS_value_size)) {
                            ++ok;
                        }
                    }
                } else {
                    ok = runner.ReadBatch(ids, buffer.data()) ? ids.size() : 0;
                }
                if (FLAGS_verify && ok == ids.size()) {
                    for (size_t i = 0; i < ids.size(); ++i) {
                        if (!VerifyPayload(
                                ids[i], buffer.data() + i * FLAGS_value_size,
                                FLAGS_value_size)) {
                            --ok;
                        }
                    }
                }
                const auto op_end = std::chrono::steady_clock::now();

                stats.requests += 1;
                stats.objects += ok;
                stats.errors += ids.size() - ok;
                stats.bytes += ok * FLAGS_value_size;
                stats.latencies_us.push_back(
                    std::chrono::duration<double, std::micro>(op_end - op_start)
                        .count());
            }
        });
    }
    for (auto& thread : threads) thread.join();
    const auto end = std::chrono::steady_clock::now();
    return MergeStats(per_thread,
                      std::chrono::duration<double>(end - start).count());
}

void ValidateArgs() {
    if (FLAGS_fs_adapter != "posix" && FLAGS_fs_adapter != "hf3fs") {
        throw std::runtime_error("--fs_adapter must be posix or hf3fs");
    }
    if (FLAGS_mode != "adapter" && FLAGS_mode != "backend") {
        throw std::runtime_error("--mode must be adapter or backend");
    }
    if (FLAGS_test != "write" && FLAGS_test != "read" &&
        FLAGS_test != "all") {
        throw std::runtime_error("--test must be write, read, or all");
    }
    if (FLAGS_value_size == 0 || FLAGS_num_objects == 0 ||
        FLAGS_batch_size == 0 || FLAGS_num_threads == 0) {
        throw std::runtime_error(
            "--value_size, --num_objects, --batch_size, and --num_threads "
            "must be positive");
    }
    if (FLAGS_shard_count <= 0) {
        throw std::runtime_error("--shard_count must be positive");
    }
    if (!IsPowerOfTwo(FLAGS_alignment)) {
        throw std::runtime_error("--alignment must be a power of two");
    }
    if (FLAGS_shard_capacity == 0 ||
        FLAGS_shard_capacity % FLAGS_alignment != 0) {
        throw std::runtime_error(
            "--shard_capacity must be positive and aligned");
    }
    const uint64_t aligned_size = AlignUp(FLAGS_value_size, FLAGS_alignment);
    const uint64_t max_slots =
        (FLAGS_num_objects + static_cast<uint64_t>(FLAGS_shard_count) - 1) /
        static_cast<uint64_t>(FLAGS_shard_count);
    if (max_slots != 0 &&
        max_slots > FLAGS_shard_capacity / aligned_size) {
        throw std::runtime_error(
            "--shard_capacity is too small for num_objects/value_size/alignment");
    }
    ValidateHf3fsStoragePath(AbsolutePath(FLAGS_storage_path));
}

template <typename Runner>
int RunWithRunner(Runner& runner) {
    runner.Init();
    bool has_error = false;

    if (FLAGS_test == "read") {
        WriteFirstObjects(runner, FLAGS_num_objects);
    }

    if (FLAGS_test == "write" || FLAGS_test == "all") {
        auto stats = RunWritePhase(runner);
        PrintStats("write", FLAGS_mode, FLAGS_fs_adapter, stats);
        has_error = has_error || stats.errors != 0;
    }

    if (FLAGS_test == "all") {
        auto stats = RunReadPhase(runner);
        PrintStats("read", FLAGS_mode, FLAGS_fs_adapter, stats);
        has_error = has_error || stats.errors != 0;
    } else if (FLAGS_test == "read") {
        auto stats = RunReadPhase(runner);
        PrintStats("read", FLAGS_mode, FLAGS_fs_adapter, stats);
        has_error = has_error || stats.errors != 0;
    }

    return has_error ? 2 : 0;
}

}  // namespace
}  // namespace mooncake

int main(int argc, char** argv) {
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = true;
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    try {
        mooncake::ValidateArgs();
        const std::string run_dir = mooncake::MakeRunDir(FLAGS_storage_path);
        std::cout << "dfs_backend_bench fs_adapter=" << FLAGS_fs_adapter
                  << " mode=" << FLAGS_mode << " test=" << FLAGS_test
                  << " storage_path="
                  << mooncake::AbsolutePath(FLAGS_storage_path)
                  << " run_dir=" << run_dir
                  << " value_size=" << FLAGS_value_size
                  << " num_objects=" << FLAGS_num_objects
                  << " batch_size=" << FLAGS_batch_size
                  << " num_threads=" << FLAGS_num_threads
                  << " shard_count=" << FLAGS_shard_count
                  << " shard_capacity=" << FLAGS_shard_capacity
                  << " alignment=" << FLAGS_alignment
                  << " warmup_batches=" << FLAGS_warmup_batches
                  << " verify=" << (FLAGS_verify ? "true" : "false")
                  << std::endl;

        mooncake::PreallocateShards(run_dir);
        const uint64_t aligned_size =
            mooncake::AlignUp(FLAGS_value_size, FLAGS_alignment);
        auto layout = mooncake::BuildLayout(run_dir, aligned_size);

        int rc = 0;
        if (FLAGS_mode == "adapter") {
            mooncake::AdapterRunner runner(run_dir, std::move(layout));
            rc = mooncake::RunWithRunner(runner);
        } else {
            mooncake::BackendRunner runner(run_dir, std::move(layout));
            rc = mooncake::RunWithRunner(runner);
        }

        if (!FLAGS_skip_cleanup) {
            std::error_code ec;
            std::filesystem::remove_all(run_dir, ec);
            if (ec) {
                std::cerr << "warning: failed to remove " << run_dir << ": "
                          << ec.message() << std::endl;
            }
        }
        return rc;
    } catch (const std::exception& ex) {
        std::cerr << "dfs_backend_bench failed: " << ex.what() << std::endl;
        return 1;
    }
}
