#!/usr/bin/env bash
# Helper operations for the manual upstream sync workflow.

set -euo pipefail

COMMAND="${1:?Usage: upstream-sync-run.sh <prepare|collect-patch|apply-patch|delete-stale-branch|record-decision|write-pr-body>}"
WORK_DIR="${UPSTREAM_SYNC_WORK_DIR:-.github/upstream-sync/work}"
CANDIDATE_JSON="${UPSTREAM_SYNC_CANDIDATE_JSON:-${WORK_DIR}/candidate.json}"
RESULT_JSON="${UPSTREAM_SYNC_RESULT_JSON:-${WORK_DIR}/result.json}"
PATCH_FILE="${UPSTREAM_SYNC_PATCH_FILE:-${WORK_DIR}/upstream-sync.patch}"
PR_BODY_FILE="${UPSTREAM_SYNC_PR_BODY_FILE:-${WORK_DIR}/pr-body.md}"

require_file() {
  local file="$1"
  if [ ! -f "$file" ]; then
    echo "ERROR: missing required file: $file" >&2
    exit 1
  fi
}

validate_pr_body_autolinks() {
  local file="$1"
  if grep -Eq '(^|[^A-Za-z0-9_])#[0-9]+' "$file"; then
    echo "ERROR: final PR body contains a bare issue/PR autolink" >&2
    exit 1
  fi
  if grep -Eq '[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+#[0-9]+' "$file"; then
    echo "ERROR: final PR body contains an owner/repo issue/PR autolink" >&2
    exit 1
  fi
  if grep -Eq 'https://github\.com/[^[:space:]]+/(pull|issues)/[0-9]+' "$file"; then
    echo "ERROR: final PR body contains a GitHub PR/issue URL" >&2
    exit 1
  fi
}

case "$COMMAND" in
  prepare)
    require_file "$CANDIDATE_JSON"
    require_file "${WORK_DIR}/source.patch"
    require_file "${WORK_DIR}/source.diff"
    mkdir -p "$WORK_DIR"
    jq -r '.source_files[]' "$CANDIDATE_JSON" > "${WORK_DIR}/source-files.txt"
    {
      echo "Prepared upstream sync context"
      echo "Candidate: upstream PR $(jq -r '.source_pr' "$CANDIDATE_JSON")"
      echo "Branch: $(jq -r '.branch_name' "$CANDIDATE_JSON")"
    } | tee "${WORK_DIR}/prepare.log"
    ;;

  collect-patch)
    require_file "$RESULT_JSON"
    mkdir -p "$WORK_DIR"
    STATUS="$(jq -r '.status' "$RESULT_JSON")"
    git reset --mixed >/dev/null
    while IFS= read -r -d '' path; do
      git add -N -- "$path"
    done < <(git ls-files --others --exclude-standard -z)
    if [ "$STATUS" = "applied" ]; then
      if git diff --quiet; then
        echo "ERROR: result status is applied, but Codex produced no patch" >&2
        exit 1
      fi
      git diff --binary > "$PATCH_FILE"
      git diff --stat > "${WORK_DIR}/upstream-sync.diffstat"
      git diff --name-only > "${WORK_DIR}/changed-files.txt"
    else
      : > "$PATCH_FILE"
      : > "${WORK_DIR}/upstream-sync.diffstat"
      : > "${WORK_DIR}/changed-files.txt"
    fi
    echo "status=${STATUS}" >> "${GITHUB_OUTPUT:-/dev/null}"
    echo "Collected Codex patch with status: ${STATUS}"
    ;;

  apply-patch)
    require_file "$PATCH_FILE"
    if [ ! -s "$PATCH_FILE" ]; then
      echo "ERROR: patch file is empty; nothing to apply" >&2
      exit 1
    fi
    git apply --3way --index "$PATCH_FILE"
    ;;

  delete-stale-branch)
    require_file "$RESULT_JSON"
    BRANCH="$(jq -r '.branch_name' "$RESULT_JSON")"
    case "$BRANCH" in
      upstream-sync/pr-[0-9]*) ;;
      *)
        echo "ERROR: refusing to delete non upstream-sync PR branch: $BRANCH" >&2
        exit 1
        ;;
    esac

    if git ls-remote --exit-code --heads origin "$BRANCH" >/dev/null 2>&1; then
      OPEN_PRS="$(
        gh pr list \
          --repo "${GITHUB_REPOSITORY:?GITHUB_REPOSITORY must be set}" \
          --state open \
          --head "$BRANCH" \
          --json number,headRefName \
          | jq --arg branch "$BRANCH" '[.[] | select(.headRefName == $branch)] | length'
      )"
      if [ "$OPEN_PRS" != "0" ]; then
        echo "ERROR: refusing to delete $BRANCH because it has an open PR" >&2
        exit 1
      fi

      git push origin --delete "$BRANCH"
      echo "Deleted stale upstream sync branch: $BRANCH"
    else
      echo "No stale upstream sync branch to delete: $BRANCH"
    fi
    ;;

  record-decision)
    require_file "$RESULT_JSON"
    require_file "$CANDIDATE_JSON"
    STATUS="$(jq -r '.status' "$RESULT_JSON")"
    case "$STATUS" in
      already_present|needs_human|skipped) ;;
      *)
        echo "No terminal triage decision to record for status: $STATUS"
        exit 0
        ;;
    esac

    STATE_BRANCH="${UPSTREAM_SYNC_STATE_BRANCH:-upstream-sync/state}"
    STATE_FILE="${UPSTREAM_SYNC_STATE_FILE:-.github/upstream-sync/triage-ledger.jsonl}"
    SOURCE_PR="$(jq -r '.source_pr' "$RESULT_JSON")"
    RECORD="$(
      jq -c -n \
        --arg decision "$STATUS" \
        --arg recommendation "$(jq -r '.recommendation' "$RESULT_JSON")" \
        --arg title "$(jq -r '.source_pr_title' "$CANDIDATE_JSON")" \
        --arg merge_commit "$(jq -r '.source_merge_commit // ""' "$CANDIDATE_JSON")" \
        --arg run_url "${GITHUB_SERVER_URL:-https://github.com}/${GITHUB_REPOSITORY:-unknown}/actions/runs/${GITHUB_RUN_ID:-unknown}" \
        --argjson upstream_pr "$SOURCE_PR" \
        --argjson confidence_percent "$(jq -r '.confidence_percent' "$RESULT_JSON")" \
        '{
          schema_version: 1,
          upstream_pr: $upstream_pr,
          decision: $decision,
          confidence_percent: $confidence_percent,
          recommendation: $recommendation,
          source_title: $title,
          source_merge_commit: $merge_commit,
          run_url: $run_url
        }'
    )"

    git config user.name "github-actions[bot]"
    git config user.email "41898282+github-actions[bot]@users.noreply.github.com"
    if git ls-remote --exit-code --heads origin "$STATE_BRANCH" >/dev/null 2>&1; then
      git fetch origin "$STATE_BRANCH"
      git switch --force-create "$STATE_BRANCH" FETCH_HEAD
    else
      git switch --orphan "$STATE_BRANCH"
      git rm -rf . >/dev/null 2>&1 || true
    fi

    mkdir -p "$(dirname "$STATE_FILE")"
    printf '%s\n' "$RECORD" >> "$STATE_FILE"
    git add "$STATE_FILE"
    if git diff --cached --quiet; then
      echo "No triage decision changes to record"
      exit 0
    fi
    git commit -m "Record upstream PR ${SOURCE_PR} ${STATUS}"
    git push origin "HEAD:${STATE_BRANCH}"
    ;;

  write-pr-body)
    require_file "$RESULT_JSON"
    BODY="$(jq -r '.pr_body' "$RESULT_JSON")"
    CONFIDENCE="$(jq -r '.confidence_percent' "$RESULT_JSON")"
    RECOMMENDATION="$(
      jq -r '.recommendation' "$RESULT_JSON" \
        | tr '\n' ' ' \
        | sed -E 's/[[:space:]]+/ /g; s/^ //; s/ $//; s/([.!?]) .*/\1/'
    )"
    if [ -z "$RECOMMENDATION" ]; then
      RECOMMENDATION="Review the generated patch and validation evidence before merging."
    fi
    if [ "${#RECOMMENDATION}" -gt 180 ]; then
      RECOMMENDATION="${RECOMMENDATION:0:177}..."
    fi

    if printf '%s\n' "$BODY" | head -n 1 | grep -Eq '^AI Confidence: [0-9]+% - .+'; then
      printf '%s\n' "$BODY" > "$PR_BODY_FILE"
    else
      {
        printf 'AI Confidence: %s%% - %s\n\n' "$CONFIDENCE" "$RECOMMENDATION"
        printf '%s\n' "$BODY"
      } > "$PR_BODY_FILE"
    fi
    validate_pr_body_autolinks "$PR_BODY_FILE"
    TITLE="$(jq -r '.pr_title' "$RESULT_JSON")"
    BRANCH="$(jq -r '.branch_name' "$RESULT_JSON")"
    {
      printf 'title=%s\n' "$TITLE"
      printf 'branch=%s\n' "$BRANCH"
    } >> "${GITHUB_OUTPUT:?GITHUB_OUTPUT must be set}"
    ;;

  *)
    echo "ERROR: unknown command: $COMMAND" >&2
    exit 1
    ;;
esac
