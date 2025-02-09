#include "duckdb/common/thread.hpp"
#include "disk_cache_filesystem.hpp"
#include "crypto.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "utils/include/resize_uninitialized.hpp"
#include "utils/include/filesystem_utils.hpp"
#include "utils/include/thread_utils.hpp"

#include <cstdint>
#include <utility>
#include <utime.h>

namespace duckdb {

namespace {

// All read requests are split into chunks, and executed in parallel.
// A [CacheReadChunk] represents a chunked IO request and its corresponding
// partial IO request.
struct CacheReadChunk {
  // Requested memory address and file offset to read from for current chunk.
  char *requested_start_addr = nullptr;
  idx_t requested_start_offset = 0;
  // Block size aligned [requested_start_offset].
  idx_t aligned_start_offset = 0;

  // Number of bytes for the chunk for IO operations, apart from the last chunk
  // it's always cache block size.
  idx_t chunk_size = 0;

  // Always allocate block size of memory for first and last chunk.
  // For middle chunks, if local cache is not hit, we also allocate memory for
  // [content] as intermediate buffer.
  //
  // TODO(hjiang): For middle chunks, the performance could be improved further:
  // remote IO operation directly read into [requested_start_addr] then write to
  // local cache file; but for code simplicity we also allocate here.
  string content;
  // Number of bytes to copy from [content] to requested memory address.
  idx_t bytes_to_copy = 0;

  // Copy from [content] to application-provided buffer.
  void CopyBufferToRequestedMemory() {
    if (!content.empty()) {
      const idx_t delta_offset = requested_start_offset - aligned_start_offset;
      std::memmove(requested_start_addr,
                   const_cast<char *>(content.data()) + delta_offset,
                   bytes_to_copy);
    }
  }
};

// Convert SHA256 value to hex string.
string Sha256ToHexString(const duckdb::hash_bytes &sha256) {
  static constexpr char kHexChars[] = "0123456789abcdef";
  std::string result;
  // SHA256 has 32 byte, we encode 2 chars for each byte of SHA256.
  result.reserve(64);

  for (unsigned char byte : sha256) {
    result += kHexChars[byte >> 4];  // Get high 4 bits
    result += kHexChars[byte & 0xF]; // Get low 4 bits
  }
  return result;
}

// Get local cache filename for the given [remote_file].
//
// Cache filename is formatted as
// `<cache-directory>/<filename-sha256>.<filename>`. So we could get all cache
// files under one directory, and get all cache files with commands like `ls`.
//
// Considering the naming format, it's worth noting it might _NOT_ work for
// local files, including mounted filesystems.
string GetLocalCacheFile(const string &cache_directory,
                         const string &remote_file, idx_t start_offset,
                         idx_t bytes_to_read) {
  duckdb::hash_bytes remote_file_sha256_val;
  duckdb::sha256(remote_file.data(), remote_file.length(),
                 remote_file_sha256_val);
  const string remote_file_sha256_str =
      Sha256ToHexString(remote_file_sha256_val);

  const string fname = StringUtil::GetFileName(remote_file);
  return StringUtil::Format("%s/%s.%s-%llu-%llu", cache_directory,
                            remote_file_sha256_str, fname, start_offset,
                            bytes_to_read);
}

// Attempt to cache [chunk] to local filesystem, if there's sufficient disk
// space available.
//
// TODO(hjiang): Document local cache file pattern and its eviction policy.
void CacheLocal(const CacheReadChunk &chunk, FileSystem &local_filesystem,
                const FileHandle &handle, const string &cache_directory,
                const string &local_cache_file) {
  // Skip local cache if insufficient disk space.
  // It's worth noting it's not a strict check since there could be concurrent
  // check and write operation (RMW operation), but it's acceptable since min
  // available disk space reservation is an order of magnitude bigger than cache
  // chunk size.
  auto disk_space = local_filesystem.GetAvailableDiskSpace(cache_directory);
  if (!disk_space.IsValid() ||
      disk_space.GetIndex() < MIN_DISK_SPACE_FOR_CACHE) {
    EvictStaleCacheFiles(local_filesystem, cache_directory);
    return;
  }

  // Dump to a temporary location at local filesystem.
  const auto fname = StringUtil::GetFileName(handle.GetPath());
  const auto local_temp_file =
      StringUtil::Format("%s%s.%s.httpfs_local_cache", cache_directory, fname,
                         UUID::ToString(UUID::GenerateRandomUUID()));
  {
    auto file_handle = local_filesystem.OpenFile(
        local_temp_file, FileOpenFlags::FILE_FLAGS_WRITE |
                             FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW);
    local_filesystem.Write(*file_handle,
                           const_cast<char *>(chunk.content.data()),
                           /*nr_bytes=*/chunk.content.length(),
                           /*location=*/0);
    file_handle->Sync();
  }

  // Then atomically move to the target postion to prevent data corruption due
  // to concurrent write.
  local_filesystem.MoveFile(/*source=*/local_temp_file,
                            /*target=*/local_cache_file);
}

} // namespace

DiskCacheFileSystem::DiskCacheFileSystem(
    unique_ptr<FileSystem> internal_filesystem_p,
    OnDiskCacheConfig cache_config_p)
    : CacheFileSystem(std::move(internal_filesystem_p)),
      cache_config(std::move(cache_config_p)),
      local_filesystem(FileSystem::CreateLocal()) {
  local_filesystem->CreateDirectory(cache_config.on_disk_cache_directory);
}

std::string DiskCacheFileSystem::GetName() const {
  return StringUtil::Format("disk_cache_filesystem for %s",
                            internal_filesystem->GetName());
}

void DiskCacheFileSystem::ReadAndCache(FileHandle &handle, char *buffer,
                                       idx_t requested_start_offset,
                                       idx_t requested_bytes_to_read,
                                       idx_t file_size) {
  const idx_t block_size = cache_config.block_size;
  const idx_t aligned_start_offset =
      requested_start_offset / block_size * block_size;
  const idx_t aligned_last_chunk_offset =
      (requested_start_offset + requested_bytes_to_read) / block_size *
      block_size;

  // Indicate the meory address to copy to for each IO operation
  char *addr_to_write = buffer;
  // Used to calculate bytes to copy for last chunk.
  idx_t already_read_bytes = 0;
  // Threads to parallelly perform IO.
  vector<thread> io_threads;

  // To improve IO performance, we split requested bytes (after alignment) into
  // multiple chunks and fetch them in parallel.
  for (idx_t io_start_offset = aligned_start_offset;
       io_start_offset <= aligned_last_chunk_offset;
       io_start_offset += block_size) {
    CacheReadChunk cache_read_chunk;
    cache_read_chunk.requested_start_addr = addr_to_write;
    cache_read_chunk.aligned_start_offset = io_start_offset;
    cache_read_chunk.requested_start_offset = requested_start_offset;

    // Implementation-wise, middle chunks are easy to handle -- read in
    // [block_size], and copy the whole chunk to the requested memory address;
    // but the first and last chunk require special handling.
    // For first chunk, requested start offset might not be aligned with block
    // size; for the last chunk, we might not need to copy the whole
    // [block_size] of memory.
    //
    // Case-1: If there's only one chunk, which serves as both the first chunk
    // and the last one.
    if (io_start_offset == aligned_start_offset &&
        io_start_offset == aligned_last_chunk_offset) {
      cache_read_chunk.chunk_size =
          MinValue<idx_t>(block_size, file_size - io_start_offset);
      cache_read_chunk.content =
          CreateResizeUninitializedString(cache_read_chunk.chunk_size);
      cache_read_chunk.bytes_to_copy = requested_bytes_to_read;
    }
    // Case-2: First chunk.
    else if (io_start_offset == aligned_start_offset) {
      const idx_t delta_offset = requested_start_offset - aligned_start_offset;
      addr_to_write += block_size - delta_offset;
      already_read_bytes += block_size - delta_offset;

      cache_read_chunk.chunk_size = block_size;
      cache_read_chunk.content = CreateResizeUninitializedString(block_size);
      cache_read_chunk.bytes_to_copy = block_size - delta_offset;
    }
    // Case-3: Last chunk.
    else if (io_start_offset == aligned_last_chunk_offset) {
      cache_read_chunk.chunk_size =
          MinValue<idx_t>(block_size, file_size - io_start_offset);
      cache_read_chunk.content =
          CreateResizeUninitializedString(cache_read_chunk.chunk_size);
      cache_read_chunk.bytes_to_copy =
          requested_bytes_to_read - already_read_bytes;
    }
    // Case-4: Middle chunks.
    else {
      addr_to_write += block_size;
      already_read_bytes += block_size;

      cache_read_chunk.bytes_to_copy = block_size;
      cache_read_chunk.chunk_size = block_size;
    }

    // Update read offset for next chunk read.
    requested_start_offset = io_start_offset + block_size;

    // Perform read operation in parallel.
    io_threads.emplace_back([this, &handle, block_size,
                             cache_read_chunk =
                                 std::move(cache_read_chunk)]() mutable {
      SetThreadName("RdCachRdThd");

      // Check local cache first, see if we could do a cached read.
      const auto local_cache_file = GetLocalCacheFile(
          cache_config.on_disk_cache_directory, handle.GetPath(),
          cache_read_chunk.aligned_start_offset, cache_read_chunk.chunk_size);

      // TODO(hjiang): Add documentation and implementation for stale cache
      // eviction policy, before that it's safe to access cache file directly.
      if (local_filesystem->FileExists(local_cache_file)) {
        auto file_handle = local_filesystem->OpenFile(
            local_cache_file, FileOpenFlags::FILE_FLAGS_READ);
        void *addr = !cache_read_chunk.content.empty()
                         ? const_cast<char *>(cache_read_chunk.content.data())
                         : cache_read_chunk.requested_start_addr;
        local_filesystem->Read(*file_handle, addr, cache_read_chunk.chunk_size,
                               /*location=*/0);
        cache_read_chunk.CopyBufferToRequestedMemory();

        // Update access and modification timestamp for the cache file, so it
        // won't get evicted.
        if (utime(local_cache_file.data(), /*times*/ nullptr) < 0) {
          throw IOException("Fails to update %s's access and modification "
                            "timestamp because %s",
                            local_cache_file, strerror(errno));
        }

        return;
      }

      // We suffer a cache loss, fallback to remote access then local filesystem
      // write.
      if (cache_read_chunk.content.empty()) {
        cache_read_chunk.content =
            CreateResizeUninitializedString(cache_read_chunk.chunk_size);
      }
      auto &disk_cache_handle = handle.Cast<CacheFileSystemHandle>();
      internal_filesystem->Read(
          *disk_cache_handle.internal_file_handle,
          const_cast<char *>(cache_read_chunk.content.data()),
          cache_read_chunk.content.length(),
          cache_read_chunk.aligned_start_offset);

      // Copy to destination buffer.
      cache_read_chunk.CopyBufferToRequestedMemory();

      // Attempt to cache file locally.
      CacheLocal(cache_read_chunk, *local_filesystem, handle,
                 cache_config.on_disk_cache_directory, local_cache_file);
    });
  }
  for (auto &cur_thd : io_threads) {
    D_ASSERT(cur_thd.joinable());
    cur_thd.join();
  }
}

} // namespace duckdb
