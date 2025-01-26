#include "disk_cache_filesystem.hpp"

#include <utility>

namespace duckdb {

DiskCacheFileSystem::DiskCacheFileSystem(
    unique_ptr<FileSystem> internal_filesystem_p)
    : internal_filesystem(std::move(internal_filesystem_p)) {}

} // namespace duckdb
