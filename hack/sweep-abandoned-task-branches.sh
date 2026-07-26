#!/usr/bin/env bash
# One-off GC for the tatara/task-* branches that accumulated before issue #443.
#
# WHY IT EXISTS: Task names (and therefore agent branch names) used to be a
# constant per project and kind, and NOTHING ever deleted a head branch, so ~522
# abandoned tatara/task-* branches piled up across the enrolled repos. The fix
# stops the growth in two places - GenerateName-minted Task names, and the B.6
# reaper's now-live DeleteBranch - but neither can reach a branch whose Task is
# long gone. This script is the backfill. It is a ONE-OFF, run by a human, and
# it is deliberately not wired into any controller.
#
# DRY RUN BY DEFAULT. It prints what it WOULD delete and exits 0. Deletion needs
# --execute, and --execute needs an interactive "yes" unless --yes is passed.
#
# A branch is deleted only when ALL of these hold:
#   1. it matches tatara/task-*                    (the name-derived agent branch)
#   2. NO open PR has it as head                   (never touch live review work)
#   3. NO live Task is named after it              (tatara/task-<name> -> Task <name>)
#   4. NO live Task takes it over                  (tatara.dev/takeover-head-branch)
# "Live" here means any Task that is not delivered/failed/rejected. parked counts
# as LIVE, exactly as controller.liveTaskPushingTo treats it: a parked takeover
# Task is unparked by a maintainer's comment and RESUMES PUSHING to its branch.
#
# SCOPE LIMITS, on purpose:
#   - tatara/task-* only. The numbered (tatara/<kind>-<number>-<slug>) and
#     documentation (tatara/docs-<sha>) branch shapes carry a title slug this
#     script would have to re-derive to map back to a Task, and a WRONG mapping
#     here deletes live work. They are left alone.
#   - GitHub only (gh). A non-github Repository URL is reported and skipped.
#
# Usage:
#   hack/sweep-abandoned-task-branches.sh [-n NAMESPACE] [-p PROJECT] [--repo OWNER/NAME]... [--execute] [--yes]
#
#   -n, --namespace  namespace holding the tatara CRs (default: tatara)
#   -p, --project    only repos of this Project (default: every Repository CR)
#       --repo       audit this repo instead of discovering Repository CRs;
#                    repeatable. Cluster access is still required for the
#                    live-Task checks.
#       --execute    actually delete. Without it, nothing is written.
#       --yes        skip the interactive confirmation (for --execute).
set -euo pipefail

NAMESPACE="tatara"
PROJECT=""
EXECUTE=0
ASSUME_YES=0
REPOS=()

while [ $# -gt 0 ]; do
	case "$1" in
	-n | --namespace)
		NAMESPACE="$2"
		shift 2
		;;
	-p | --project)
		PROJECT="$2"
		shift 2
		;;
	--repo)
		REPOS+=("$2")
		shift 2
		;;
	--execute)
		EXECUTE=1
		shift
		;;
	--yes)
		ASSUME_YES=1
		shift
		;;
	-h | --help)
		sed -n '2,40p' "$0"
		exit 0
		;;
	*)
		echo "unknown argument: $1" >&2
		exit 2
		;;
	esac
done

for bin in gh kubectl jq; do
	command -v "$bin" >/dev/null || {
		echo "missing required tool: $bin" >&2
		exit 2
	}
done

# Live Tasks: everything the stage machine has not finished with. The three
# stages below are api/v1alpha1's terminal set MINUS parked (see the header).
live_json="$(kubectl get tasks.tatara.dev -n "$NAMESPACE" -o json)"
# shellcheck disable=SC2016 # $s is a jq variable, not a shell one
live_filter='.items[] | select((.status.stage // "") as $s | $s != "delivered" and $s != "failed" and $s != "rejected")'
if [ -n "$PROJECT" ]; then
	live_filter="$live_filter | select(.spec.projectRef == \"$PROJECT\")"
fi
live_task_names="$(jq -r "$live_filter | .metadata.name" <<<"$live_json" | sort -u)"
live_takeover_branches="$(jq -r "$live_filter | .metadata.annotations[\"tatara.dev/takeover-head-branch\"] // empty" <<<"$live_json" | sort -u)"

if [ ${#REPOS[@]} -eq 0 ]; then
	repo_filter='.items[]'
	if [ -n "$PROJECT" ]; then
		repo_filter="$repo_filter | select(.spec.projectRef == \"$PROJECT\")"
	fi
	while read -r url; do
		[ -n "$url" ] || continue
		case "$url" in
		*github.com*) ;;
		*)
			echo "SKIP (not github): $url" >&2
			continue
			;;
		esac
		slug="${url##*github.com/}"
		slug="${slug%.git}"
		REPOS+=("$slug")
	done < <(jq -r "$repo_filter | .spec.url" <<<"$(kubectl get repositories.tatara.dev -n "$NAMESPACE" -o json)")
fi

if [ ${#REPOS[@]} -eq 0 ]; then
	echo "no repositories to audit" >&2
	exit 1
fi

live_count="$(grep -c . <<<"$live_task_names" || true)"
echo "namespace=$NAMESPACE project=${PROJECT:-<all>} repos=${#REPOS[@]} live_tasks=$live_count"
echo

doomed=()
for slug in "${REPOS[@]}"; do
	branches="$(gh api --paginate "repos/$slug/branches?per_page=100" --jq '.[].name' | grep '^tatara/task-' || true)"
	[ -n "$branches" ] || continue
	open_heads="$(gh api --paginate "repos/$slug/pulls?state=open&per_page=100" --jq '.[].head.ref' || true)"
	while read -r branch; do
		[ -n "$branch" ] || continue
		if grep -Fxq "$branch" <<<"$open_heads"; then
			echo "KEEP   $slug $branch (open PR)"
			continue
		fi
		if grep -Fxq "$branch" <<<"$live_takeover_branches"; then
			echo "KEEP   $slug $branch (live takeover Task)"
			continue
		fi
		task="${branch#tatara/task-}"
		if grep -Fxq "$task" <<<"$live_task_names"; then
			echo "KEEP   $slug $branch (live Task $task)"
			continue
		fi
		echo "DELETE $slug $branch"
		doomed+=("$slug $branch")
	done <<<"$branches"
done

echo
echo "would delete ${#doomed[@]} branch(es)"
if [ "$EXECUTE" -eq 0 ]; then
	echo "dry run: nothing was deleted. Re-run with --execute to delete."
	exit 0
fi
if [ ${#doomed[@]} -eq 0 ]; then
	exit 0
fi
if [ "$ASSUME_YES" -eq 0 ]; then
	read -r -p "delete ${#doomed[@]} branch(es)? type yes: " answer
	[ "$answer" = "yes" ] || {
		echo "aborted"
		exit 1
	}
fi
for entry in "${doomed[@]}"; do
	slug="${entry%% *}"
	branch="${entry#* }"
	if gh api -X DELETE "repos/$slug/git/refs/heads/$branch" >/dev/null; then
		echo "deleted $slug $branch"
	else
		echo "FAILED  $slug $branch" >&2
	fi
done
