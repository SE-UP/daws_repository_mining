import json
import subprocess
import os
import sys

def get_git_commits(repo_path):
    """Retrieve commit history from a Git repository in a structured format."""
    if not os.path.isdir(repo_path):
        print(f"Error: The path '{repo_path}' is not a valid directory.")
        return []
    
    try:
        log_format = "%H|%an|%ad|%s"  # Hash | Author | Date | Message
        result = subprocess.run(
            ["git", "-C", repo_path, "log", "--pretty=format:" + log_format, "--date=iso"],
            capture_output=True,
            text=True,
            check=True
        )
        return result.stdout.strip().split("\n") if result.stdout else []
    except subprocess.CalledProcessError as e:
        print("Error fetching git log:", e)
        return []


def parse_commit_logs(log_lines):
    """Convert raw commit log lines into a structured list of dictionaries."""
    commits = []
    for line in log_lines:
        parts = line.split("|")
        if len(parts) == 4:
            commits.append({
                "commit_hash": parts[0],
                "author": parts[1],
                "date": parts[2],
                "message": parts[3]
            })
    return commits


def save_to_json(data, output_file):
    """Save commit data to a JSON file."""
    try:
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)
        print(f"Successfully saved {len(data)} commits to {output_file}")
    except IOError as e:
        print("Error saving to file:", e)


def main():
    """Main function to execute the script."""
    if len(sys.argv) != 3:
        print("Usage: python parse_git_commits.py <repo_path> <output_file>")
        sys.exit(1)

    repo_path = sys.argv[1]
    output_file = sys.argv[2]

    print("Fetching commit logs...\n")
    log_lines = get_git_commits(repo_path)
    
    if log_lines:
        parsed_commits = parse_commit_logs(log_lines)
        save_to_json(parsed_commits, output_file)
    else:
        print("No commits found or unable to retrieve logs. Please check the repository path.")

if __name__ == "__main__":
    main()
