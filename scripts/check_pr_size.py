"""Count all added AND deleted lines, including generated files, without exclusions."""
import subprocess
import sys

if len(sys.argv) != 3:
    raise SystemExit("Usage: check_pr_size.py BASE HEAD")
diff = subprocess.check_output(
    ["git", "diff", "--numstat", "--no-renames", f"{sys.argv[1]}...{sys.argv[2]}"],
    text=True,
)
total = 0
for line in diff.splitlines():
    added, deleted, path = line.split("\t", 2)
    if added == "-" or deleted == "-":
        raise SystemExit(f"Binary changes cannot be measured: {path}")
    total += int(added) + int(deleted)
print(f"Changed lines: {total}/500")
if total > 500:
    raise SystemExit("PR exceeds 500 changed lines; split it before merging")
