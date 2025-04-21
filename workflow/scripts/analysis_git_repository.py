"""This module contains the GitAnalysis class to analyze Git repositories."""
import logging
import input_validator
from datetime import datetime
from pydriller import Repository
from pydriller.metrics.process.change_set import ChangeSet
from pydriller.metrics.process.code_churn import CodeChurn
from pydriller.metrics.process.commits_count import CommitsCount
from pydriller.metrics.process.contributors_count import ContributorsCount
from pydriller.metrics.process.contributors_experience import ContributorsExperience
from pydriller.metrics.process.hunks_count import HunksCount
from pydriller.metrics.process.lines_count import LinesCount

validator = input_validator.Validator()


class GitAnalysis:
    """
    GitAnalysis class to analyze Git repositories.
    """
    def __init__(self, logger=None, repo_path=None):
        self.log = logger or logging.getLogger(__name__)
        self.repo_path = validator.path_dir(repo_path)
        self.date_first_commit = None
        self.date_last_commit = None
        self.count_commits = 0
        self.count_authors = 0
        self.line_changes = dict()
        self.commits = dict()


    def extract_commits(self):
        """
        Extract information from a given Git repository.
        """
        self.log.info("Extracting git repo information: %s", self.repo_path)

        try:
            for commit in Repository(self.repo_path).traverse_commits():
                self.log.debug("Extracting a commit: %s", commit.hash)

                commit_info = {
                    "msg"                  : commit.msg,
                    "author"               : f"{commit.author.name}:{commit.author.email}",
                    "committer"            : "%s:%s" % (commit.committer.name, commit.committer.email),
                    "author_date"          : commit.author_date.strftime("%Y-%m-%dT%H:%M:%S %z"),
                    "author_epoch"         : commit.author_date.timestamp(),
                    "committer_date"       : commit.committer_date.strftime("%Y-%m-%dT%H:%M:%S %z"),
                    "committer_epoch"      : commit.committer_date.timestamp(),
                    "branches"             : list(commit.branches), # list
                    "is_main_branch"       : commit.in_main_branch ,# bool
                    "is_merge"             : commit.merge ,         # bool
                    "parents"              : commit.parents ,       # list
                    "files"                : [],
                    "file_extensions"      : [],
                    "n_deletions"          : commit.deletions,
                    "n_insertions"         : commit.insertions,
                    "n_lines"              : commit.lines,
                    "n_files"              : commit.files,
                    "dmm_unit_size"        : commit.dmm_unit_size ,      # floaot
                    "dmm_unit_complexity"  : commit.dmm_unit_complexity, # float
                    "dmm_unit_interfacing" : commit.dmm_unit_interfacing, # float
                    "is_snakemake": False,
                    "n_snakemake_rule_added": 0,
                    "n_snakemake_rule_removed": 0
                }

                for file in commit.modified_files:
                    file_info = {
                        "old_path": file.old_path,
                        "new_path": file.new_path,
                        "filename": file.filename,
                        "change_type": file.change_type.name,
                        "n_added_lines": file.added_lines,
                        "n_deleted_lines": file.deleted_lines,
                        "methods": file.methods,
                        "changed_methods": file.changed_methods,
                        "n_lines": file.nloc,
                        "complexity": file.complexity,
                        "n_tokens": file.token_count,
                        "is_snakemake": False,
                        "n_snakemake_rule_added": 0,
                        "n_snakemake_rule_removed": 0
                    }

                    file_extension = self._extract_file_extensions(file.filename)
                    if file_extension and file_extension not in commit_info["file_extensions"]:
                        commit_info["file_extensions"].append(file_extension)

                    if file.filename == "Snakefile" or file.filename.endswith(".smk"):
                        file_info["is_snakemake"] = True
                        commit_info["is_snakemake"] = True
                        source_code = file.source_code if file.source_code else ""
                        source_code_before = file.source_code_before if file.source_code_before else ""

                        rules_from_code = self._extract_snakemake_rule_names_from_code(source_code)
                        rules_from_code_before = self._extract_snakemake_rule_names_from_code(source_code_before)

                        if rules_from_code and rules_from_code_before:
                            for rule in rules_from_code:
                                if rule in rules_from_code_before:
                                    rules_from_code_before.remove(rule)
                                else:
                                    file_info["n_snakemake_rule_added"] += 1
                                    commit_info["n_snakemake_rule_added"] += 1

                            for rule in rules_from_code_before:
                                file_info["n_snakemake_rule_removed"] += 1
                                commit_info["n_snakemake_rule_removed"] += 1

                    commit_info["files"].append(file_info)

                if not self.date_first_commit or commit.committer_date < self.date_first_commit:
                    self.date_first_commit = commit.committer_date
                if not self.date_last_commit or commit.committer_date > self.date_last_commit:
                    self.date_last_commit = commit.committer_date

                self.commits[commit.hash] = commit_info

        except Exception as e:
            self.log.error("Error extracting git repo information: %s", e)
            return 0

        self.count_commits = len(self.commits)

        return self.count_commits


    def _extract_file_extensions(self, filename=None):
        """
        Extract the file extension from a file name.
        """
        filename = validator.validate(filename, str, required=True)
        parts = filename.split('.')
        if len(parts) > 1:
            return parts[-1].lower()
        return None


    def _extract_snakemake_rule_names_from_code(self, code):
        """
        Extract Snakemake rules from the code.
        """
        rules = list()
        for line in code.splitlines():
            # remove whitespaces and commit_parents
            line = line.strip()
            if line.startswith("rule"):
                rule_name = (line.split()[1]).split(":")[0]
                rules.append(rule_name)

        return rules


    def get_process_metrics(self, from_commit=None, to_commit=None, since=None, to=None):
        """
        Get the process_metrics between two commits.
        """
        from_commit = validator.format_git_commit_hash(from_commit)
        to_commit   = validator.format_git_commit_hash(to_commit)
        since       = validator.validate(since, datetime, required=False)
        to          = validator.validate(to, datetime, required=False)

        metric = ChangeSet(self.repo_path,
                           from_commit=from_commit, to_commit=to_commit,
                           since=since, to=to)

        changeset_max = metric.max()
        changeset_avg = metric.avg()

        metric = CodeChurn(self.repo_path,
                           from_commit=from_commit, to_commit=to_commit,
                           since=since, to=to)

        codechurn_max = metric.max()
        codechurn_avg = metric.avg()
        codechurn_count = metric.count()
        codechurn_added_removed_lines = metric.get_added_and_removed_lines()

        metric = CommitsCount(self.repo_path,
                              from_commit=from_commit, to_commit=to_commit,
                              since=since, to=to)

        commitscount_files = metric.count()

        metric = ContributorsCount(self.repo_path,
                                   from_commit=from_commit, to_commit=to_commit,
                                   since=since, to=to)

        contributorscount_count = metric.count()
        contributorscount_minor = metric.count_minor()

        metric = ContributorsExperience(self.repo_path,
                                        from_commit=from_commit, to_commit=to_commit,
                                        since=since, to=to)

        contributorsexperience_count = metric.count()

        metric = HunksCount(self.repo_path,
                            from_commit=from_commit, to_commit=to_commit,
                            since=since, to=to)

        hunkscount_files = metric.count()

        metric = LinesCount(self.repo_path,
                            from_commit=from_commit, to_commit=to_commit,
                            since=since, to=to)

        linescount_count = metric.count()
        linescount_count_added   = metric.count_added()
        linescount_max_added     = metric.max_added()
        linescount_avg_added     = metric.avg_added()
        linescount_count_removed = metric.count_removed()
        linescount_max_removed   = metric.max_removed()
        linescount_avg_removed   = metric.avg_removed()

        process_metrics = {
            "changeset_max": changeset_max,
            "changeset_avg": changeset_avg,
            "codechurn_max": codechurn_max,
            "codechurn_avg": codechurn_avg,
            "codechurn_count": codechurn_count,
            "codechurn_added_lines": codechurn_added_removed_lines,
            "codechurn_removed_lines": codechurn_added_removed_lines,
            "commitscount_files": commitscount_files,
            "contributorscount_count": contributorscount_count,
            "contributorscount_minor": contributorscount_minor,
            "contributorsexperience_count": contributorsexperience_count,
            "hunkscount_files": hunkscount_files,
            "linescount_count": linescount_count,
            "linescount_count_added": linescount_count_added,
            "linescount_max_added": linescount_max_added,
            "linescount_avg_added": linescount_avg_added,
            "linescount_count_removed": linescount_count_removed,
            "linescount_max_removed": linescount_max_removed,
            "linescount_avg_removed": linescount_avg_removed
        }

        return process_metrics
