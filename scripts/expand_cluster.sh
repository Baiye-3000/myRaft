#!/usr/bin/env bash
set -euo pipefail

build_dir="${1:-./build}"
root="$(mktemp -d /tmp/distributed-kv-membership-demo-XXXXXX)"
pids=()

cleanup() {
  for pid in "${pids[@]:-}"; do
    kill "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
  rm -rf "$root"
}
trap cleanup EXIT INT TERM

for binary in dkv_node dkv_client dkv_admin; do
  if [[ ! -x "$build_dir/$binary" ]]; then
    echo "missing executable: $build_dir/$binary" >&2
    exit 2
  fi
done

members=(
  "1,127.0.0.1,17101,127.0.0.1,18101"
  "2,127.0.0.1,17102,127.0.0.1,18102"
  "3,127.0.0.1,17103,127.0.0.1,18103"
)
member4="4,127.0.0.1,17104,127.0.0.1,18104"

start_node() {
  local id="$1"
  shift
  mkdir -p "$root/node$id"
  "$build_dir/dkv_node" "$id" "$root/node$id" "$@" \
    >"$root/node$id.log" 2>&1 &
  pids+=("$!")
}

wait_for_member_count() {
  local expected="$1"
  local output=""
  for _ in {1..40}; do
    output="$($build_dir/dkv_admin 127.0.0.1 17101 show-members)"
    if [[ "$(wc -l <<<"$output")" -eq "$expected" ]]; then
      printf '%s\n' "$output"
      return 0
    fi
    sleep 0.1
  done
  echo "membership did not converge to $expected nodes" >&2
  return 1
}

start_node 1 "${members[@]}"
start_node 2 "${members[@]}"
start_node 3 "${members[@]}"
sleep 2

printf 'SET membership-demo before-expand\nQUIT\n' |
  "$build_dir/dkv_client" 127.0.0.1 17101 >/dev/null

start_node 4 --learner "${members[@]}" "$member4"
"$build_dir/dkv_admin" 127.0.0.1 17101 add-node \
  4 127.0.0.1 17104 127.0.0.1 18104

expanded="$(wait_for_member_count 4)"
printf 'GET membership-demo\nQUIT\n' |
  "$build_dir/dkv_client" 127.0.0.1 17104 | grep -q before-expand

"$build_dir/dkv_admin" 127.0.0.1 17101 remove-node 4
contracted="$(wait_for_member_count 3)"

echo "membership demo passed: 3 -> 4 -> 3 with KV data preserved"
