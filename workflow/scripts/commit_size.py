import os
import git
import json

def get_commit_size(commit):
    commit_size = 0
    if commit.parents:
        parent_commit = commit.parents[0]
        diffs = commit.diff(parent_commit, create_patch=True)
    else:
        diffs = commit.diff(None, create_patch=True)

    for diff in diffs:
        if diff.a_path and (diff.a_path.endswith('.py') or diff.a_path.endswith('.smk')):
            commit_size += len(diff.diff)
    return commit_size

def process_single_repo(repo_path):
    try:
        repo = git.Repo(repo_path)
    except Exception as e:
        print(f"[!] Skipping {repo_path}: {e}")
        return

    commit_sizes = []
    for commit in repo.iter_commits('main'):
        commit_size = get_commit_size(commit)
        commit_data = {
            "commit_id": commit.hexsha,
            "commit_size": commit_size,
            "datetime": commit.committed_datetime.isoformat(),
            "commit_message": commit.message.strip()
        }
        commit_sizes.append(commit_data)

    commit_sizes.reverse()
    for idx, commit_data in enumerate(commit_sizes):
        if idx == 0:
            commit_data["codebase_size"] = commit_data["commit_size"]
        else:
            commit_data["codebase_size"] = commit_sizes[idx - 1]["codebase_size"] + commit_data["commit_size"]

    output_path = os.path.join(repo_path, "commit_sizes.json")
    with open(output_path, "w") as f:
        json.dump(commit_sizes, f, indent=4)
    print(f"commit_sizes.json created in {repo_path}")

def process_all_repositories(cloned_repos_dir):
    for repo_name in os.listdir(cloned_repos_dir):
        repo_path = os.path.join(cloned_repos_dir, repo_name)
        if os.path.isdir(os.path.join(repo_path, ".git")):
            process_single_repo(repo_path)