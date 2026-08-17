#!/usr/bin/env bash
# Run release.yml's verify-pin and fanout-alarm scripts against a stubbed forge.
#
# These two jobs are the READER half of the CD pin fan-out, and on 2026-08-17
# (#622) the reader is what filed a false alarm: verify-pin polled for 10m, gave
# up, and the alarm asserted "the bump action produced nothing" while PR #423
# was open, armed and merged 5m later. A reader whose verdict is wrong is worse
# than no reader - it sends a human hunting for a PR that does not exist.
#
# The scripts are extracted from the workflow with yq and executed for real with
# `curl` stubbed on PATH, so what is tested is what CI runs. Routes are matched
# against the request URL, first match wins:
#
#     <url-substring>|<curl exit status>|<response body, or @file>
#
# Wired into CI through .pre-commit-config.yaml (the `lint` job runs
# `pre-commit run --all-files`) and `make test-cd-jobs`.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORKFLOW="$REPO_ROOT/.github/workflows/release.yml"

PARENT="szymonrychu/tatara-helmfile"
PR_URL="https://github.com/${PARENT}/pull/423"
HEAD_SHA="40005c5aa1f0e0b1c2d3e4f5a6b7c8d9e0f1a2b3"

pass=0
fail=0
current=""

# --- harness ----------------------------------------------------------------

extract() { # <yq path> - pull a step's `run:` script out of the workflow
  yq -r "$1" "$WORKFLOW"
}

make_stub_dir() {
  local d
  d="$(mktemp -d)"
  mkdir -p "$d/bin"
  cat >"$d/bin/curl" <<'STUB'
#!/usr/bin/env bash
# Scripted curl. Ignores flags; routes on the last argument (the URL).
url="${*: -1}"
printf '%s\n' "$url" >> "$STUB_DIR/curl.log"
# `-d @-` means the request body is on stdin; keep it so tests can assert on
# what the alarm actually files. Only then, or an unrelated curl would block.
case "$*" in *"@-"*) cat > "$STUB_DIR/post-body" ;; esac
while IFS='|' read -r pat code body; do
  [ -n "${pat:-}" ] || continue
  case "$url" in
    *"$pat"*)
      case "$body" in
        @*) cat "$STUB_DIR/${body#@}" ;;
        "") ;;
        *) printf '%s' "$body" ;;
      esac
      exit "$code"
      ;;
  esac
done < "$STUB_DIR/routes"
echo "no stub route for: $url" >&2
exit 22
STUB
  chmod +x "$d/bin/curl"
  printf '%s' "$d"
}

routes() { # write the route table; args are route lines
  local d="$1"; shift
  printf '%s\n' "$@" > "$d/routes"
}

body_file() { # <dir> <name> <content>
  printf '%s' "$3" > "$1/$2"
}

# Sets OUT (stdout+stderr) and RC. NOT called in a command substitution: that
# would run it in a subshell and neither assignment would reach the caller.
run_script() { # <dir> <script> [VAR=VAL ...]
  local d="$1" script="$2"; shift 2
  : > "$d/github_output"
  set +e
  OUT="$(env -i \
    PATH="$d/bin:$PATH" \
    HOME="$HOME" \
    STUB_DIR="$d" \
    GITHUB_OUTPUT="$d/github_output" \
    "$@" \
    bash -c "$script" 2>&1)"
  RC=$?
  set -e
}

step_output() { # <dir> <key>
  sed -n "s/^$2=//p" "$1/github_output" | tail -n 1
}

it() { current="$1"; }

ok() { pass=$((pass + 1)); }

bad() {
  fail=$((fail + 1))
  echo "FAIL: $current"
  echo "      $1"
}

expect_rc() {
  if [ "$RC" = "$1" ]; then ok; else bad "expected exit $1, got $RC"; fi
}

expect_has() {
  case "$1" in *"$2"*) ok ;; *) bad "expected output to contain '$2'"; echo "--- got ---"; echo "$1" ;; esac
}

expect_lacks() {
  case "$1" in *"$2"*) bad "expected output NOT to contain '$2'"; echo "--- got ---"; echo "$1" ;; *) ok ;; esac
}

expect_eq() {
  if [ "$1" = "$2" ]; then ok; else bad "expected '$2', got '$1'"; fi
}

# --- fixtures ---------------------------------------------------------------

PINNED_HELMFILE='  - name: tatara-chat
    version: 0.1.3
  - name: tatara-operator
    version: 3.4.0
  - name: project-tatara
    version: 3.4.0
  - name: project-infrastructure
    version: 3.4.0
  - name: project-mtg
    version: 3.4.0'
PINNED_VALUES='  image:
    tag: "v3.4.0"'
STALE_HELMFILE='  - name: tatara-operator
    version: 3.3.0'
STALE_VALUES='  image:
    tag: "v3.3.0"'
# The bump rewrites FOUR version pins in helmfile.yaml.gotmpl in one commit
# (tatara-operator + the three tatara-project releases). A file where only some
# of them moved is a broken fan-out, not a landed one.
PARTIAL_HELMFILE='  - name: tatara-operator
    version: 3.4.0
  - name: project-tatara
    version: 3.4.0
  - name: project-infrastructure
    version: 3.4.0
  - name: project-mtg
    version: 3.3.0'
# `.` is regex-any and the old check was unanchored, so 13.4.0 matched 3.4.0.
LOOKALIKE_HELMFILE='  - name: tatara-chat
    version: 13.4.0
  - name: other
    version: 3.4.0-rc1'

pr_json() { # <auto_merge json>
  printf '[{"number":423,"html_url":"%s","head":{"sha":"%s"},"auto_merge":%s}]' \
    "$PR_URL" "$HEAD_SHA" "$1"
}

checks_json() { # <conclusion>
  printf '{"check_runs":[{"name":"helmfile diff","conclusion":"%s"}]}' "$1"
}

# Contents routes for a given ref: whether that ref carries v3.4.0.
contents_routes() { # <dir> <ref> <pinned|stale>
  local d="$1" ref="$2" what="$3" slug="${2//\//-}"
  if [ "$what" = pinned ]; then
    body_file "$d" "hf-$slug" "$PINNED_HELMFILE"
    body_file "$d" "val-$slug" "$PINNED_VALUES"
  else
    body_file "$d" "hf-$slug" "$STALE_HELMFILE"
    body_file "$d" "val-$slug" "$STALE_VALUES"
  fi
  printf '%s\n%s\n' \
    "contents/helmfile.yaml.gotmpl?ref=${ref}|0|@hf-${slug}" \
    "contents/values/tatara-operator/common.yaml?ref=${ref}|0|@val-${slug}"
}

VERIFY_ENV=(TOKEN=t VERSION=v3.4.0 VERIFY_POLL_ATTEMPTS=1 VERIFY_POLL_SLEEP=0)

verify_script() {
  extract '.jobs.verify-pin.steps[] | select(.id == "verify") | .run'
}

alarm_script() {
  extract '.jobs.fanout-alarm.steps[0].run'
}

# --- verify-pin -------------------------------------------------------------

test_pin_on_main_is_the_happy_path() {
  it "pin already on main -> exit 0, state=pinned"
  local d; d="$(make_stub_dir)"
  routes "$d" "$(contents_routes "$d" main pinned)"
  run_script "$d" "$(verify_script)" "${VERIFY_ENV[@]}"
  expect_rc 0
  expect_has "$OUT" "is pinned on ${PARENT} main"
  expect_eq "$(step_output "$d" state)" "pinned"
}

test_no_pr_at_all_is_a_down_cascade() {
  it "not on main, no open deploy-train PR -> exit 1, state=no-pr"
  local d; d="$(make_stub_dir)"
  routes "$d" "$(contents_routes "$d" main stale)" "pulls?state=open|0|[]"
  run_script "$d" "$(verify_script)" "${VERIFY_ENV[@]}"
  expect_rc 1
  expect_has "$OUT" "::error::"
  expect_eq "$(step_output "$d" state)" "no-pr"
}

test_open_armed_and_green_is_in_flight_not_a_failure() {
  it "open + carries pin + armed + no red check -> exit 0, state=in-flight"
  local d; d="$(make_stub_dir)"
  routes "$d" \
    "$(contents_routes "$d" main stale)" \
    "$(contents_routes "$d" cd/deploy-train pinned)" \
    "pulls?state=open|0|$(pr_json '{"merge_method":"squash"}')" \
    "check-runs|0|$(checks_json success)"
  run_script "$d" "$(verify_script)" "${VERIFY_ENV[@]}"
  expect_rc 0
  expect_has "$OUT" "::notice::"
  expect_has "$OUT" "$PR_URL"
  expect_eq "$(step_output "$d" state)" "in-flight"
  # The whole point of #622: this state must not be reported as a dead cascade.
  expect_lacks "$OUT" "::error::"
}

test_open_but_not_armed_never_merges() {
  it "open + carries pin + NOT armed -> exit 1, state=not-armed"
  local d; d="$(make_stub_dir)"
  routes "$d" \
    "$(contents_routes "$d" main stale)" \
    "$(contents_routes "$d" cd/deploy-train pinned)" \
    "pulls?state=open|0|$(pr_json null)" \
    "check-runs|0|$(checks_json success)"
  run_script "$d" "$(verify_script)" "${VERIFY_ENV[@]}"
  expect_rc 1
  expect_has "$OUT" "$PR_URL"
  expect_eq "$(step_output "$d" state)" "not-armed"
}

test_armed_but_red_never_merges_either() {
  it "open + armed + a red check -> exit 1, state=checks-red"
  local d; d="$(make_stub_dir)"
  routes "$d" \
    "$(contents_routes "$d" main stale)" \
    "$(contents_routes "$d" cd/deploy-train pinned)" \
    "pulls?state=open|0|$(pr_json '{"merge_method":"squash"}')" \
    "check-runs|0|$(checks_json failure)"
  run_script "$d" "$(verify_script)" "${VERIFY_ENV[@]}"
  expect_rc 1
  expect_has "$OUT" "helmfile diff"
  expect_eq "$(step_output "$d" state)" "checks-red"
}

test_pr_that_does_not_carry_the_pin() {
  it "open PR without this version -> exit 1, state=pin-missing"
  local d; d="$(make_stub_dir)"
  routes "$d" \
    "$(contents_routes "$d" main stale)" \
    "$(contents_routes "$d" cd/deploy-train stale)" \
    "pulls?state=open|0|$(pr_json '{"merge_method":"squash"}')" \
    "check-runs|0|$(checks_json success)"
  run_script "$d" "$(verify_script)" "${VERIFY_ENV[@]}"
  expect_rc 1
  expect_eq "$(step_output "$d" state)" "pin-missing"
}

test_an_unreadable_forge_is_not_a_missing_pr() {
  it "pulls query fails -> exit 1, state=forge-unreadable (not no-pr)"
  local d; d="$(make_stub_dir)"
  routes "$d" "$(contents_routes "$d" main stale)" "pulls?state=open|22|"
  run_script "$d" "$(verify_script)" "${VERIFY_ENV[@]}"
  expect_rc 1
  expect_eq "$(step_output "$d" state)" "forge-unreadable"
}

test_a_check_runs_failure_is_a_verdict_not_a_silent_death() {
  it "check-runs read fails -> exit 1 WITH a verdict, not errexit with none"
  local d; d="$(make_stub_dir)"
  routes "$d" \
    "$(contents_routes "$d" main stale)" \
    "$(contents_routes "$d" cd/deploy-train pinned)" \
    "pulls?state=open|0|$(pr_json '{"merge_method":"squash"}')" \
    "check-runs|22|"
  run_script "$d" "$(verify_script)" "${VERIFY_ENV[@]}"
  expect_rc 1
  expect_has "$OUT" "::error::"
  # A bare assignment under `set -e` dies here writing NO state, and the alarm
  # then reports "the bump job failed first" on a healthy, armed, correct PR.
  expect_eq "$(step_output "$d" state)" "forge-unreadable"
}

test_check_runs_is_paged_at_100() {
  it "check-runs is requested with per_page=100 (a red run on page 2 counts)"
  local d; d="$(make_stub_dir)"
  routes "$d" \
    "$(contents_routes "$d" main stale)" \
    "$(contents_routes "$d" cd/deploy-train pinned)" \
    "pulls?state=open|0|$(pr_json '{"merge_method":"squash"}')" \
    "check-runs|0|$(checks_json success)"
  run_script "$d" "$(verify_script)" "${VERIFY_ENV[@]}"
  expect_has "$(cat "$d/curl.log")" "check-runs?per_page=100"
}

test_a_partially_applied_pin_is_not_reported_as_pinned() {
  it "3 of the 4 operator pins moved -> NOT pinned, not a silent exit 0"
  local d; d="$(make_stub_dir)"
  body_file "$d" hf-part "$PARTIAL_HELMFILE"
  body_file "$d" val-part "$PINNED_VALUES"
  routes "$d" \
    "contents/helmfile.yaml.gotmpl?ref=main|0|@hf-part" \
    "contents/values/tatara-operator/common.yaml?ref=main|0|@val-part" \
    "pulls?state=open|0|[]"
  run_script "$d" "$(verify_script)" "${VERIFY_ENV[@]}"
  expect_rc 1
  expect_eq "$(step_output "$d" state)" "no-pr"
}

test_a_longer_version_is_not_mistaken_for_this_one() {
  it "unanchored grep: version 13.4.0 must not satisfy a 3.4.0 pin"
  local d; d="$(make_stub_dir)"
  body_file "$d" hf-look "$LOOKALIKE_HELMFILE"
  body_file "$d" val-look "$PINNED_VALUES"
  routes "$d" \
    "contents/helmfile.yaml.gotmpl?ref=main|0|@hf-look" \
    "contents/values/tatara-operator/common.yaml?ref=main|0|@val-look" \
    "pulls?state=open|0|[]"
  run_script "$d" "$(verify_script)" "${VERIFY_ENV[@]}"
  expect_rc 1
  expect_eq "$(step_output "$d" state)" "no-pr"
}

test_the_fast_path_polls_before_giving_a_verdict() {
  it "main is polled VERIFY_POLL_ATTEMPTS times before the state query"
  local d; d="$(make_stub_dir)"
  routes "$d" "$(contents_routes "$d" main stale)" "pulls?state=open|0|[]"
  run_script "$d" "$(verify_script)" TOKEN=t VERSION=v3.4.0 \
    VERIFY_POLL_ATTEMPTS=3 VERIFY_POLL_SLEEP=0
  local polls
  polls="$(grep -c 'contents/helmfile.yaml.gotmpl?ref=main' "$d/curl.log")"
  expect_eq "$polls" "3"
}

test_every_curl_retries_transient_forge_errors() {
  it "no bare curl: a 503 must not decide the verdict (att 8 died on one)"
  local script line
  script="$(verify_script)$(alarm_script)"
  local bare
  while IFS= read -r line; do
    bare="${line#"${line%%[![:space:]]*}"}"
    case "$bare" in \#*) continue ;; esac
    case "$line" in
      *curl\ *)
        case "$line" in
          *--retry*) ;;
          *) bad "curl without --retry: ${line#"${line%%[![:space:]]*}"}"; return ;;
        esac
        ;;
    esac
  done <<< "$script"
  ok
}

# --- fanout-alarm -----------------------------------------------------------

ALARM_ENV=(
  GITHUB_REPOSITORY=szymonrychu/tatara-operator
  TOKEN=t VERSION=v3.4.0
  RUN_URL=https://github.com/szymonrychu/tatara-operator/actions/runs/32035597231
)

issues_json() { # <title> - one open issue, plus a PR that must be ignored
  printf '[{"number":99,"title":"a PR that must not be treated as the tracker","pull_request":{"url":"x"}},{"number":622,"title":"%s"}]' "$1"
}

test_alarm_reports_the_observed_state_not_a_guess() {
  it "alarm names the PR that exists instead of asserting nothing was produced"
  local d; d="$(make_stub_dir)"
  body_file "$d" issues "$(issues_json 'CD pin fan-out stalled: v3.3.0 is not pinned in tatara-helmfile')"
  routes "$d" "issues?state=open|0|@issues" "issues/622/comments|0|{}"
  run_script "$d" "$(alarm_script)" "${ALARM_ENV[@]}" \
    BUMP_RESULT=success VERIFY_RESULT=failure VERIFY_STATE=checks-red \
    VERIFY_DETAIL="${PARENT}#423 (${PR_URL}) carries v3.4.0 and is armed, but the check 'helmfile diff' is red"
  expect_rc 0
  local body; body="$(cat "$d/post-body" 2>/dev/null || echo "")"
  expect_has "$body" "$PR_URL"
  expect_has "$body" "checks-red"
  # The 2026-08-17 text sent a human hunting for a PR that never exists: the
  # terminal hop opens `cd: deploy-train`, not `cd: bump <component> to <v>`.
  expect_lacks "$body" "produced nothing"
  expect_lacks "$body" "cd: bump tatara-operator to v3.4.0"
}

test_alarm_says_open_it_by_hand_only_when_there_is_no_pr() {
  it "state=no-pr -> the body does tell a human to apply the pin by hand"
  local d; d="$(make_stub_dir)"
  body_file "$d" issues "[]"
  routes "$d" "issues?state=open|0|@issues" "repos/szymonrychu/tatara-operator/issues|0|{}"
  run_script "$d" "$(alarm_script)" "${ALARM_ENV[@]}" \
    BUMP_RESULT=failure VERIFY_RESULT=skipped VERIFY_STATE=no-pr VERIFY_DETAIL=""
  expect_rc 0
  local body; body="$(cat "$d/post-body" 2>/dev/null || echo "")"
  expect_has "$body" "by hand"
}

test_alarm_dedupes_onto_the_open_tracker() {
  it "an open tracker gets a comment; no second issue is opened"
  local d; d="$(make_stub_dir)"
  body_file "$d" issues "$(issues_json 'CD pin fan-out stalled: v3.3.0 is not pinned in tatara-helmfile')"
  routes "$d" "issues?state=open|0|@issues" "issues/622/comments|0|{}"
  run_script "$d" "$(alarm_script)" "${ALARM_ENV[@]}" \
    BUMP_RESULT=failure VERIFY_RESULT=failure VERIFY_STATE=no-pr VERIFY_DETAIL=""
  expect_rc 0
  expect_has "$OUT" "622"
  expect_has "$(cat "$d/curl.log")" "issues/622/comments"
}

test_alarm_finds_a_tracker_past_the_first_page() {
  it "100 open issues before the tracker -> still comments, does not duplicate"
  local d; d="$(make_stub_dir)"
  # A full page of open PRs (which /issues returns and which count toward the
  # 100 BEFORE the pull_request filter), then the tracker on page 2.
  local filler i
  filler="$(for i in $(seq 1 100); do
    printf '{"number":%d,"title":"filler","pull_request":{"url":"x"}},' "$i"
  done)"
  body_file "$d" page1 "[${filler%,}]"
  body_file "$d" page2 "$(issues_json 'CD pin fan-out stalled: v3.3.0 is not pinned in tatara-helmfile')"
  routes "$d" \
    "page=2|0|@page2" \
    "issues?state=open|0|@page1" \
    "issues/622/comments|0|{}"
  run_script "$d" "$(alarm_script)" "${ALARM_ENV[@]}" \
    BUMP_RESULT=failure VERIFY_RESULT=failure VERIFY_STATE=no-pr VERIFY_DETAIL=""
  expect_rc 0
  expect_has "$(cat "$d/curl.log")" "issues/622/comments"
}

test_alarm_never_mistakes_a_pull_request_for_the_tracker() {
  it "GET /issues returns PRs too; they must not be commented on"
  local d; d="$(make_stub_dir)"
  body_file "$d" issues '[{"number":99,"title":"CD pin fan-out stalled: v3.4.0 is not pinned in tatara-helmfile","pull_request":{"url":"x"}}]'
  routes "$d" "issues?state=open|0|@issues" "repos/szymonrychu/tatara-operator/issues|0|{}"
  run_script "$d" "$(alarm_script)" "${ALARM_ENV[@]}" \
    BUMP_RESULT=failure VERIFY_RESULT=skipped VERIFY_STATE=no-pr VERIFY_DETAIL=""
  expect_rc 0
  expect_lacks "$(cat "$d/curl.log")" "issues/99/comments"
}

# --- run --------------------------------------------------------------------

for t in $(declare -F | awk '{print $3}' | grep '^test_'); do
  "$t"
done

echo "cd-release job scripts: ${pass} assertions passed, ${fail} failed"
[ "$fail" -eq 0 ]
