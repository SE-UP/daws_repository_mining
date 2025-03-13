import json
import subprocess
import os

def run_git_command(repo_path, command):
    """
    Runs a Git command in the specified repository and returns the output.
    
    If the command fails, it handles the error and returns an empty string
    instead of crashing.

    Parameters:
        repo_path (str): The path to the Git repository.
        command (str): The Git command to execute.

    Returns:
        str: The command output if successful, otherwise an empty string.
    """
    try:
        result = subprocess.run(
            ["git", "-C", repo_path] + command.split(),
            capture_output=True,
            text=True,
            check=True
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError:
        return ""  # Return an empty string instead of failing the script

def get_git_commits(repo_path, repo_name):
    """
    Retrieves the commit history of a Git repository and formats it as structured data.

    The function extracts key details from each commit, including:
    - Commit hash
    - Author and committer names
    - Commit timestamp
    - Commit message
    - Parent commit(s)
    - Branch name (if available)
    - Associated tags
    - Changed files in the commit
    - Number of lines added and deleted
    - Whether the commit is a merge commit
    - Merged branch name (if applicable)

    Parameters:
        repo_path (str): The path to the Git repository.
        repo_name (str): The name of the repository.

    Returns:
        list[dict]: A list of commit metadata dictionaries.
    """
    if not os.path.isdir(repo_path):
        print(f"Error: The path '{repo_path}' is not a valid directory.")
        return []

    # Ensure the repository has full history (useful for shallow clones)
    run_git_command(repo_path, "fetch --unshallow")

    # Define the format for retrieving commit details
    log_format = "%H|%an|%cn|%ad|%s|%P"
    commit_logs = run_git_command(repo_path, f"log --pretty=format:'{log_format}' --date=iso").split("\n")

    commits = []
    for line in commit_logs:
        parts = line.split("|")
        if len(parts) == 6:
            commit_hash, author, committer, date, message, parent_commit = parts

            # Get branch name (returns "unknown" if commit is not on a known branch)
            branch = run_git_command(repo_path, f"name-rev --name-only {commit_hash}") or "unknown"

            # Get list of tags associated with this commit
            tags = run_git_command(repo_path, f"tag --contains {commit_hash}").split("\n")
            tags = [tag for tag in tags if tag]  # Filter out empty lines

            # Get the list of changed files in the commit (returns ["unknown"] if missing)
            changed_files = run_git_command(repo_path, f"show --pretty=format: --name-only {commit_hash}").split("\n")
            changed_files = [file for file in changed_files if file] or ["unknown"]

            # Get number of lines added/deleted in the commit (returns 0 if missing)
            line_stats = run_git_command(repo_path, f"show --numstat --pretty=format: {commit_hash}").split("\n")
            lines_added, lines_deleted = 0, 0
            for stat in line_stats:
                parts = stat.split("\t")
                if len(parts) == 3:
                    try:
                        lines_added += int(parts[0]) if parts[0] != "-" else 0
                        lines_deleted += int(parts[1]) if parts[1] != "-" else 0
                    except ValueError:
                        pass  # Ignore conversion errors for binary files

            # Determine if this is a merge commit (if it has more than one parent)
            is_merge_commit = len(parent_commit.split()) > 1
            merged_branch = run_git_command(repo_path, f"name-rev --name-only {parent_commit.split()[0]}") if is_merge_commit else ""

            # Store commit metadata in a structured dictionary
            commits.append({
                "repo_name": repo_name,             # Repository name
                "commit_hash": commit_hash,         # Unique commit identifier
                "author": author,                   # Author of the commit
                "committer": committer,             # Who applied the commit (may differ in rebases)
                "date": date,                       # Timestamp of the commit
                "message": message,                 # Commit message
                "parent_commit": parent_commit,     # Parent commit(s)
                "branch": branch,                   # Branch name (if available)
                "tags": tags or [],                 # List of associated tags
                "changed_files": changed_files,     # List of modified files
                "lines_added": lines_added,         # Number of lines added
                "lines_deleted": lines_deleted,     # Number of lines deleted
                "is_merge_commit": is_merge_commit, # Whether the commit is a merge commit
                "merged_branch": merged_branch if is_merge_commit else None  # Merged branch (if applicable)
            })

    return commits

def process_all_repositories(cloned_repos_dir, output_file):
    """
    Processes all cloned repositories, extracts commit logs, and saves them to a single JSON file.

    This function:
    - Iterates through all cloned repositories.
    - Extracts commit history and relevant metadata.
    - Stores all commit logs in a structured JSON file.

    Parameters:
        cloned_repos_dir (str): Path to the directory containing cloned repositories.
        output_file (str): Path to the output JSON file where commit logs will be saved.
    """
    all_commits = []

    print("Processing repositories...\n")
    for repo_name in os.listdir(cloned_repos_dir):
        repo_path = os.path.join(cloned_repos_dir, repo_name)
        if os.path.isdir(repo_path):
            print(f"Fetching commits for: {repo_name}")
            commits = get_git_commits(repo_path, repo_name)
            if commits:
                all_commits.extend(commits)

    # Save all collected commit logs into a single JSON file
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(all_commits, f, indent=4)

    print(f"Successfully saved commit logs from all repositories to {output_file}")