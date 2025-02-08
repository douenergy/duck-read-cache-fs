#pragma once

#include <string>

namespace duckdb {

// Set thread name to current thread; fail silently if an error happens.
void SetThreadName(const std::string &thread_name);

} // namespace duckdb
