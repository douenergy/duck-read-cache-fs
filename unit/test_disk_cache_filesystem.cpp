// Unit test for disk cache filesystem.
//
// IO operations are performed in chunks, testing senarios are listed as
// follows:
// (1) There're one or more chunks to read;
// (2) Chunks to read include start, middle and end parts of the file;
// (3) Bytes to read include partial or complete part of a chunk;
// (4) Chunks to read is not cached, partially cached, or completed cached.
// These senarios are orthogonal and should be tested in their combinations.

#define CATCH_CONFIG_RUNNER
#include "catch.hpp"

#include "disk_cache_filesystem.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "cache_filesystem_config.hpp"
#include "filesystem_utils.hpp"

using namespace duckdb; // NOLINT

namespace {

constexpr uint64_t TEST_FILE_SIZE = 26;
const auto TEST_FILE_CONTENT = []() {
  string content(TEST_FILE_SIZE, '\0');
  for (uint64_t idx = 0; idx < TEST_FILE_SIZE; ++idx) {
    content[idx] = 'a' + idx;
  }
  return content;
}();
const auto TEST_FILENAME =
    StringUtil::Format("/tmp/%s", UUID::ToString(UUID::GenerateRandomUUID()));
const auto TEST_ON_DISK_CACHE_DIRECTORY = "/tmp/duckdb_test_cached_http_cache";
} // namespace

// One chunk is involved, requested bytes include only "first and last chunk".
TEST_CASE("Test on disk cache filesystem with requested chunk the first "
          "meanwhile last chunk",
          "[on-disk cache filesystem test]") {
  LocalFileSystem::CreateLocal()->RemoveDirectory(TEST_ON_DISK_CACHE_DIRECTORY);
  const uint64_t test_block_size = 26;
  OnDiskCacheConfig cache_config;
  cache_config.on_disk_cache_directory = TEST_ON_DISK_CACHE_DIRECTORY;
  cache_config.block_size = test_block_size;
  DiskCacheFileSystem disk_cache_fs{LocalFileSystem::CreateLocal(),
                                    cache_config};

  // First uncached read.
  {
    auto handle =
        disk_cache_fs.OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
    const uint64_t start_offset = 1;
    const uint64_t bytes_to_read = TEST_FILE_SIZE - 2;
    string content(bytes_to_read, '\0');
    disk_cache_fs.Read(
        *handle, const_cast<void *>(static_cast<const void *>(content.data())),
        bytes_to_read, start_offset);
    REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
  }

  // Second cached read.
  {
    auto handle =
        disk_cache_fs.OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
    const uint64_t start_offset = 1;
    const uint64_t bytes_to_read = TEST_FILE_SIZE - 2;
    string content(bytes_to_read, '\0');
    disk_cache_fs.Read(
        *handle, const_cast<void *>(static_cast<const void *>(content.data())),
        bytes_to_read, start_offset);
    REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
  }
}

// Two chunks are involved, which include both first and last chunks.
TEST_CASE("Test on disk cache filesystem with requested chunk the first and "
          "last chunk",
          "[on-disk cache filesystem test]") {
  LocalFileSystem::CreateLocal()->RemoveDirectory(TEST_ON_DISK_CACHE_DIRECTORY);
  const uint64_t test_block_size = 5;
  OnDiskCacheConfig cache_config;
  cache_config.on_disk_cache_directory = TEST_ON_DISK_CACHE_DIRECTORY;
  cache_config.block_size = test_block_size;
  DiskCacheFileSystem disk_cache_fs{LocalFileSystem::CreateLocal(),
                                    cache_config};

  // First uncached read.
  {
    auto handle =
        disk_cache_fs.OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
    const uint64_t start_offset = 2;
    const uint64_t bytes_to_read = 5;
    string content(bytes_to_read, '\0');
    disk_cache_fs.Read(
        *handle, const_cast<void *>(static_cast<const void *>(content.data())),
        bytes_to_read, start_offset);
    REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
  }

  // Second cached read.
  {
    auto handle =
        disk_cache_fs.OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
    const uint64_t start_offset = 3;
    const uint64_t bytes_to_read = 4;
    string content(bytes_to_read, '\0');
    disk_cache_fs.Read(
        *handle, const_cast<void *>(static_cast<const void *>(content.data())),
        bytes_to_read, start_offset);
    REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
  }
}

// Three blocks involved, which include first, last and middle chunk.
TEST_CASE("Test on disk cache filesystem with requested chunk the first, "
          "middle and last chunk",
          "[on-disk cache filesystem test]") {
  LocalFileSystem::CreateLocal()->RemoveDirectory(TEST_ON_DISK_CACHE_DIRECTORY);
  const uint64_t test_block_size = 5;
  OnDiskCacheConfig cache_config;
  cache_config.on_disk_cache_directory = TEST_ON_DISK_CACHE_DIRECTORY;
  cache_config.block_size = test_block_size;
  DiskCacheFileSystem disk_cache_fs{LocalFileSystem::CreateLocal(),
                                    cache_config};

  // First uncached read.
  {
    auto handle =
        disk_cache_fs.OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
    const uint64_t start_offset = 2;
    const uint64_t bytes_to_read = 11;
    string content(bytes_to_read, '\0');
    disk_cache_fs.Read(
        *handle, const_cast<void *>(static_cast<const void *>(content.data())),
        bytes_to_read, start_offset);
    REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
  }

  // Second cached read.
  {
    auto handle =
        disk_cache_fs.OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
    const uint64_t start_offset = 3;
    const uint64_t bytes_to_read = 10;
    string content(bytes_to_read, '\0');
    disk_cache_fs.Read(
        *handle, const_cast<void *>(static_cast<const void *>(content.data())),
        bytes_to_read, start_offset);
    REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
  }
}

// One block involved, it's the first meanwhile last block; requested content
// doesn't involve the end of the file.
TEST_CASE(
    "Test on disk cache filesystem with requested chunk first and last one",
    "[on-disk cache filesystem test]") {
  LocalFileSystem::CreateLocal()->RemoveDirectory(TEST_ON_DISK_CACHE_DIRECTORY);
  const uint64_t test_block_size = 5;
  OnDiskCacheConfig cache_config;
  cache_config.on_disk_cache_directory = TEST_ON_DISK_CACHE_DIRECTORY;
  cache_config.block_size = test_block_size;
  DiskCacheFileSystem disk_cache_fs{LocalFileSystem::CreateLocal(),
                                    cache_config};

  // First uncached read.
  {
    auto handle =
        disk_cache_fs.OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
    const uint64_t start_offset = 2;
    const uint64_t bytes_to_read = 2;
    string content(bytes_to_read, '\0');
    disk_cache_fs.Read(
        *handle, const_cast<void *>(static_cast<const void *>(content.data())),
        bytes_to_read, start_offset);
    REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
  }

  // Second cached read.
  {
    auto handle =
        disk_cache_fs.OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
    const uint64_t start_offset = 3;
    const uint64_t bytes_to_read = 2;
    string content(bytes_to_read, '\0');
    disk_cache_fs.Read(
        *handle, const_cast<void *>(static_cast<const void *>(content.data())),
        bytes_to_read, start_offset);
    REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
  }
}

// Requested chunk involves the end of the file.
TEST_CASE("Test on disk cache filesystem with requested chunk at last of file",
          "[on-disk cache filesystem test]") {
  LocalFileSystem::CreateLocal()->RemoveDirectory(TEST_ON_DISK_CACHE_DIRECTORY);
  const uint64_t test_block_size = 5;
  OnDiskCacheConfig cache_config;
  cache_config.on_disk_cache_directory = TEST_ON_DISK_CACHE_DIRECTORY;
  cache_config.block_size = test_block_size;
  DiskCacheFileSystem disk_cache_fs{LocalFileSystem::CreateLocal(),
                                    cache_config};

  // First uncached read.
  {
    auto handle =
        disk_cache_fs.OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
    const uint64_t start_offset = 23;
    const uint64_t bytes_to_read = 10;
    string content(3, '\0'); // effective offset range: [23, 25]
    disk_cache_fs.Read(
        *handle, const_cast<void *>(static_cast<const void *>(content.data())),
        bytes_to_read, start_offset);
    REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
  }

  // Check cache files count.
  REQUIRE(GetFileCountUnder(TEST_ON_DISK_CACHE_DIRECTORY) == 2);

  // Second cached read, partial cached and another part uncached.
  {
    auto handle =
        disk_cache_fs.OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
    const uint64_t start_offset = 15;
    const uint64_t bytes_to_read = 15;
    string content(11, '\0'); // effective offset range: [15, 25]
    disk_cache_fs.Read(
        *handle, const_cast<void *>(static_cast<const void *>(content.data())),
        bytes_to_read, start_offset);
    REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
  }

  // Get all cache files and check file count.
  REQUIRE(GetFileCountUnder(TEST_ON_DISK_CACHE_DIRECTORY) == 3);
}

// Requested chunk involves the middle of the file.
TEST_CASE(
    "Test on disk cache filesystem with requested chunk at middle of file",
    "[on-disk cache filesystem test]") {
  LocalFileSystem::CreateLocal()->RemoveDirectory(TEST_ON_DISK_CACHE_DIRECTORY);
  const uint64_t test_block_size = 5;
  OnDiskCacheConfig cache_config;
  cache_config.on_disk_cache_directory = TEST_ON_DISK_CACHE_DIRECTORY;
  cache_config.block_size = test_block_size;
  DiskCacheFileSystem disk_cache_fs{LocalFileSystem::CreateLocal(),
                                    cache_config};

  // First uncached read.
  {
    auto handle =
        disk_cache_fs.OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
    const uint64_t start_offset = 16;
    const uint64_t bytes_to_read = 3;
    string content(bytes_to_read, '\0'); // effective offset range: [16, 18]
    disk_cache_fs.Read(
        *handle, const_cast<void *>(static_cast<const void *>(content.data())),
        bytes_to_read, start_offset);
    REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
  }

  // Get all cache files and check file count.
  REQUIRE(GetFileCountUnder(TEST_ON_DISK_CACHE_DIRECTORY) == 1);

  // Second cached read, partial cached and another part uncached.
  {
    auto handle =
        disk_cache_fs.OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
    const uint64_t start_offset = 8;
    const uint64_t bytes_to_read = 14;
    string content(bytes_to_read, '\0'); // effective offset range: [8, 21]
    disk_cache_fs.Read(
        *handle, const_cast<void *>(static_cast<const void *>(content.data())),
        bytes_to_read, start_offset);
    REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
  }

  // Get all cache files and check file count.
  REQUIRE(GetFileCountUnder(TEST_ON_DISK_CACHE_DIRECTORY) == 4);
}

// All chunks cached locally, later access shouldn't create new cache file.
TEST_CASE("Test on disk cache filesystem no new cache file after a full cache",
          "[on-disk cache filesystem test]") {
  LocalFileSystem::CreateLocal()->RemoveDirectory(TEST_ON_DISK_CACHE_DIRECTORY);
  const uint64_t test_block_size = 5;
  OnDiskCacheConfig cache_config;
  cache_config.on_disk_cache_directory = TEST_ON_DISK_CACHE_DIRECTORY;
  cache_config.block_size = test_block_size;
  DiskCacheFileSystem disk_cache_fs{LocalFileSystem::CreateLocal(),
                                    cache_config};

  // First uncached read.
  {
    auto handle =
        disk_cache_fs.OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
    const uint64_t start_offset = 0;
    const uint64_t bytes_to_read = TEST_FILE_SIZE;
    string content(bytes_to_read, '\0');
    disk_cache_fs.Read(
        *handle, const_cast<void *>(static_cast<const void *>(content.data())),
        bytes_to_read, start_offset);
    REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
  }

  // Get all cache files.
  auto cache_files1 = GetSortedFilesUnder(TEST_ON_DISK_CACHE_DIRECTORY);

  // Second cached read.
  {
    auto handle =
        disk_cache_fs.OpenFile(TEST_FILENAME, FileOpenFlags::FILE_FLAGS_READ);
    const uint64_t start_offset = 3;
    const uint64_t bytes_to_read = 10;
    string content(bytes_to_read, '\0');
    disk_cache_fs.Read(
        *handle, const_cast<void *>(static_cast<const void *>(content.data())),
        bytes_to_read, start_offset);
    REQUIRE(content == TEST_FILE_CONTENT.substr(start_offset, bytes_to_read));
  }

  // Get all cache files and check unchanged.
  auto cache_files2 = GetSortedFilesUnder(TEST_ON_DISK_CACHE_DIRECTORY);
  REQUIRE(cache_files1 == cache_files2);
}

TEST_CASE("Test on reading non-existent file",
          "[on-disk cache filesystem test]") {
  LocalFileSystem::CreateLocal()->RemoveDirectory(TEST_ON_DISK_CACHE_DIRECTORY);
  DiskCacheFileSystem disk_cache_fs{LocalFileSystem::CreateLocal(),
                                    OnDiskCacheConfig{}};
  REQUIRE_THROWS(disk_cache_fs.OpenFile("non-existent-file",
                                        FileOpenFlags::FILE_FLAGS_READ));
}

int main(int argc, char **argv) {
  auto local_filesystem = LocalFileSystem::CreateLocal();
  auto file_handle = local_filesystem->OpenFile(
      TEST_FILENAME, FileOpenFlags::FILE_FLAGS_WRITE |
                         FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW);
  local_filesystem->Write(
      *file_handle,
      const_cast<void *>(static_cast<const void *>(TEST_FILE_CONTENT.data())),
      TEST_FILE_SIZE, /*location=*/0);
  file_handle->Sync();
  file_handle->Close();

  int result = Catch::Session().run(argc, argv);
  local_filesystem->RemoveFile(TEST_FILENAME);
  return result;
}
