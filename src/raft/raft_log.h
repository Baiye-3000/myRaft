#pragma once

#include <cstddef>
#include <optional>
#include <string>
#include <vector>

#include "raft/types.h"

namespace distributed_kv::raft {

/**
 * Contiguous in-memory Raft log with an absolute-index boundary entry.
 *
 * RaftLog performs structural validation but has no locking or persistence.
 * The owning RaftNode event thread serializes all access.
 */
class RaftLog final {
 public:
  /**
   * Creates a log containing only the index-zero, term-zero sentinel.
   *
   * Input: none.
   * Output: valid empty Raft log.
   * Thread safety: construction requires exclusive access.
   */
  RaftLog();

  /**
   * Returns the retained compaction boundary's absolute index.
   *
   * The boundary is a metadata-only no-op entry and is excluded from size().
   */
  [[nodiscard]] LogIndex firstIndex() const noexcept;

  /**
   * Returns the highest local log index.
   *
   * Input: none.
   * Output: zero for an empty logical log.
   * Thread safety: owning event thread only.
   */
  [[nodiscard]] LogIndex lastIndex() const noexcept;

  /**
   * Returns the term of the highest local log entry.
   *
   * Input: none.
   * Output: zero for the sentinel-only log.
   * Thread safety: owning event thread only.
   */
  [[nodiscard]] Term lastTerm() const noexcept;

  /**
   * Returns the number of entries after the retained boundary.
   *
   * Input: none.
   * Output: logical entry count.
   * Thread safety: owning event thread only.
   */
  [[nodiscard]] std::size_t size() const noexcept;

  /**
   * Looks up a term by absolute log index.
   *
   * Input: log index.
   * Output: term when present, otherwise std::nullopt.
   * Thread safety: owning event thread only.
   */
  [[nodiscard]] std::optional<Term> termAt(LogIndex index) const noexcept;

  /**
   * Returns a copied entry by absolute log index.
   *
   * Input: log index.
   * Output: copied entry when present.
   * Thread safety: owning event thread only.
   */
  [[nodiscard]] std::optional<LogEntry> entryAt(LogIndex index) const;

  /**
   * Appends one locally-created entry at lastIndex()+1.
   *
   * Input: term, entry type, and command bytes.
   * Output: assigned absolute log index.
   * Thread safety: owning event thread only.
   */
  [[nodiscard]] LogIndex append(Term term, EntryType type,
                                std::string command);

  /**
   * Copies up to maximum_count entries starting at an absolute index.
   *
   * Input: first index and maximum count.
   * Output: contiguous copied suffix, possibly empty.
   * Thread safety: owning event thread only.
   */
  [[nodiscard]] std::vector<LogEntry> entriesFrom(
      LogIndex first_index, std::size_t maximum_count) const;

  /**
   * Applies a leader suffix after a known matching previous index.
   *
   * Input: previous index, contiguous incoming entries, writable error.
   * Output: true after conflicts are truncated and missing entries appended.
   * Thread safety: owning event thread only.
   */
  [[nodiscard]] bool appendFrom(LogIndex previous_index,
                                const std::vector<LogEntry>& incoming,
                                std::string& error);

  /**
   * Tests whether an index exists with the supplied term.
   *
   * Input: index and term.
   * Output: true for an exact match.
   * Thread safety: owning event thread only.
   */
  [[nodiscard]] bool matches(LogIndex index, Term term) const noexcept;

  /**
   * Finds the first local index belonging to a term.
   *
   * Input: term.
   * Output: first index or std::nullopt.
   * Thread safety: owning event thread only.
   */
  [[nodiscard]] std::optional<LogIndex> firstIndexOfTerm(
      Term term) const noexcept;

  /**
   * Finds the last local index belonging to a term.
   *
   * Input: term.
   * Output: last index or std::nullopt.
   * Thread safety: owning event thread only.
   */
  [[nodiscard]] std::optional<LogIndex> lastIndexOfTerm(
      Term term) const noexcept;

  /**
   * Copies the complete persistent log including the boundary.
   *
   * Input: none.
   * Output: self-contained contiguous log image.
   * Thread safety: owning event thread only.
   */
  [[nodiscard]] std::vector<LogEntry> persistentEntries() const;

  /**
   * Drops entries before boundary while retaining its index and term.
   *
   * Input: an existing absolute index and its expected Snapshot term.
   * Output: true after atomic in-memory replacement; false leaves log intact.
   */
  [[nodiscard]] bool compactTo(LogIndex boundary_index, Term boundary_term,
                               std::string& error);

  /**
   * Replaces the log from a validated persistent image.
   *
   * Input: boundary-prefixed contiguous entries and writable error.
   * Output: true after replacement; false leaves the current log unchanged.
   * Thread safety: owning event thread before RPC processing.
   */
  [[nodiscard]] bool restore(const std::vector<LogEntry>& entries,
                             std::string& error);

 private:
  /**
   * Removes index and every following entry.
   *
   * Input: first removable index greater than zero.
   * Output: shortened contiguous log.
   * Thread safety: owning event thread only.
   */
  void truncateFrom(LogIndex index);

  [[nodiscard]] std::optional<std::size_t> offsetOf(
      LogIndex index) const noexcept;

  std::vector<LogEntry> entries_;
};

}  // namespace distributed_kv::raft
