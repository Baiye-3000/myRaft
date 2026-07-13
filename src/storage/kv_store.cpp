#include "storage/kv_store.h"

#include <algorithm>
#include <mutex>
#include <utility>

namespace distributed_kv::storage {

bool KVStore::put(std::string key, std::string value) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  const auto result =
      entries_.insert_or_assign(std::move(key), std::move(value));
  return result.second;
}

std::optional<std::string> KVStore::get(const std::string& key) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  const auto entry = entries_.find(key);
  if (entry == entries_.end()) {
    return std::nullopt;
  }
  return entry->second;
}

bool KVStore::remove(const std::string& key) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  return entries_.erase(key) != 0U;
}

std::size_t KVStore::size() const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  return entries_.size();
}

std::vector<std::pair<std::string, std::string>>
KVStore::snapshotEntries() const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  std::vector<std::pair<std::string, std::string>> snapshot(
      entries_.begin(), entries_.end());
  std::sort(snapshot.begin(), snapshot.end(),
            [](const auto& left, const auto& right) {
              return left.first < right.first;
            });
  return snapshot;
}

bool KVStore::replaceAll(
    const std::vector<std::pair<std::string, std::string>>& entries,
    std::string& error) {
  std::unordered_map<std::string, std::string> replacement;
  replacement.reserve(entries.size());
  for (const auto& entry : entries) {
    if (!replacement.emplace(entry.first, entry.second).second) {
      error = "snapshot contains duplicate KV key";
      return false;
    }
  }
  std::unique_lock<std::shared_mutex> lock(mutex_);
  entries_.swap(replacement);
  error.clear();
  return true;
}

}  // namespace distributed_kv::storage
