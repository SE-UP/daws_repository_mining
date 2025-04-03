import os
import git
import json
import matplotlib
matplotlib.use('Agg')  # Use a non-GUI backend for plotting in Snakemake
import matplotlib.pyplot as plt
import pandas as pd


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
       return None


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
           commit_data["codebase_size"] = (
               commit_sizes[idx - 1]["codebase_size"] + commit_data["commit_size"]
           )


   # Save JSON
   output_json_path = os.path.join(repo_path, "commit_sizes.json")
   with open(output_json_path, "w") as f:
       json.dump(commit_sizes, f, indent=4)
   print(f"commit_sizes.json created in {repo_path}")


   # Plot individual repo
   plot_individual_repo(commit_sizes, repo_path)
   return commit_sizes


def plot_individual_repo(commit_data, repo_path):
   df = pd.DataFrame(commit_data)
   df["datetime"] = pd.to_datetime(df["datetime"], utc=True)


   plt.figure(figsize=(10, 6))
   plt.plot(df["datetime"], df["codebase_size"], marker='o', linestyle='-')
   plt.title("Codebase Size Growth")
   plt.xlabel("Date")
   plt.ylabel("Codebase Size (diff characters)")
   plt.grid(True)
   plt.xticks(rotation=45)
   plt.tight_layout()


   output_plot_path = os.path.join(repo_path, "codebase_growth.png")
   plt.savefig(output_plot_path, dpi=300)
   plt.close()


   print(f"codebase_growth.png created in {repo_path}")


def process_all_repositories(cloned_repos_dir):
    all_commit_sizes = {}
    for repo_name in os.listdir(cloned_repos_dir):
        repo_path = os.path.join(cloned_repos_dir, repo_name)
        if os.path.isdir(os.path.join(repo_path, ".git")):
            commit_sizes = process_single_repo(repo_path)
            if commit_sizes:  
                all_commit_sizes[repo_name] = commit_sizes
    return all_commit_sizes