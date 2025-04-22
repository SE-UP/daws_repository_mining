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


    def extract_commits(self, extend_for=""):
        """
        Extract information from a given Git repository.
        """
        self.log.info("Extracting git repo information: %s", self.repo_path)
        extend_for = extend_for.lower()

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
                    "change_types"         : [],
                    "n_deletions"          : commit.deletions,
                    "n_insertions"         : commit.insertions,
                    "n_lines"              : commit.lines,
                    "n_files"              : commit.files,
                    "dmm_unit_size"        : commit.dmm_unit_size ,      # floaot
                    "dmm_unit_complexity"  : commit.dmm_unit_complexity, # float
                    "dmm_unit_interfacing" : commit.dmm_unit_interfacing # float
                }

                snakemake_included_rules = []
                snakemake_included_scripts = []
                snakemake_irregular_rule_files = []
                snakemake_rule_files = []

                if extend_for == "snakemake":
                    commit_info["snakemake_related"] = False
                    commit_info["snakemake_modules_from_code"]        = []
                    commit_info["snakemake_modules_from_code_before"] = []
                    commit_info["snakemake_rules_from_code"]          = []
                    commit_info["snakemake_rules_from_code_before"]   = []
                    commit_info["snakemake_n_modules_added"]   = 0
                    commit_info["snakemake_n_modules_removed"] = 0
                    commit_info["snakemake_n_rules_added"]     = 0
                    commit_info["snakemake_n_rules_removed"]   = 0


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
                        "n_tokens": file.token_count
                    }

                    file_extension = self._get_file_extensions(file.filename)
                    if file_extension and file_extension not in commit_info["file_extensions"]:
                        commit_info["file_extensions"].append(file_extension)

                    if file.change_type.name not in commit_info["change_types"]:
                        commit_info["change_types"].append(file.change_type.name)

                    if extend_for == "snakemake":
                        file_info["snakemake_related"] = False
                        file_info["snakemake_modules_from_code"]        = []
                        file_info["snakemake_modules_from_code_before"] = []
                        file_info["snakemake_rules_from_code"]          = []
                        file_info["snakemake_rules_from_code_before"]   = []
                        file_info["snakemake_n_modules_added"]   = 0
                        file_info["snakemake_n_modules_removed"] = 0
                        file_info["snakemake_n_rules_added"]     = 0
                        file_info["snakemake_n_rules_removed"]   = 0

                        if file.filename.lower() == "snakefile" or file.filename.endswith(".smk"):
                            file_info["snakemake_related"] = True
                            commit_info["snakemake_related"] = True
                            snakemake_rule_files.append(file.new_path)
                            source_code = file.source_code if file.source_code else ""
                            source_code_before = file.source_code_before if file.source_code_before else ""

                            snakemake_irregular_rule_files.extend(
                                self._get_snakemake_irregular_rule_files_from_code(source_code))

                            snakemake_included_rules.extend(
                                self._get_snakemake_included_files_from_code(source_code))

                            rules_from_code = self._get_snakemake_rule_names_from_code(source_code)
                            rules_from_code_before = self._get_snakemake_rule_names_from_code(source_code_before)
                            modules_from_code = self._get_snakemake_module_names_from_code(source_code)
                            modules_from_code_before = self._get_snakemake_module_names_from_code(source_code_before)

                            file_info["snakemake_rules_from_code"] = rules_from_code
                            file_info["snakemake_rules_from_code_before"] = rules_from_code_before
                            file_info["snakemake_modules_from_code"] = modules_from_code
                            file_info["snakemake_modules_from_code_before"] = modules_from_code_before
                            commit_info["snakemake_rules_from_code"].extend(rules_from_code)
                            commit_info["snakemake_rules_from_code_before"].extend(rules_from_code_before)
                            commit_info["snakemake_modules_from_code"].extend(modules_from_code)
                            commit_info["snakemake_modules_from_code_before"].extend(modules_from_code_before)

                            if rules_from_code:
                                for rule in rules_from_code:
                                    if rule not in rules_from_code_before:
                                        file_info["snakemake_n_rules_added"] += 1

                            if rules_from_code_before:
                                for rule in rules_from_code_before:
                                    if rule not in rules_from_code:
                                        file_info["snakemake_n_rules_removed"] += 1

                            if modules_from_code:
                                for module in modules_from_code:
                                    if module not in modules_from_code_before:
                                        file_info["snakemake_n_rules_added"] += 1

                            if modules_from_code_before:
                                for module in modules_from_code_before:
                                    if module not in modules_from_code:
                                        file_info["snakemake_n_rules_removed"] += 1

                        elif file.new_path:
                            if file.new_path.lower().startswith("scripts/") or "/scripts/" in file.new_path.lower():
                                snakemake_included_scripts.append(file.new_path)

                        commit_info["files"].append(file_info)

                if extend_for == "snakemake":
                    commit_info["snakemake_included_rules"]   = list(set(snakemake_included_rules))
                    commit_info["snakemake_included_scripts"] = list(set(snakemake_included_scripts))
                    commit_info["snakemake_irregular_rule_files"] = list(set(snakemake_irregular_rule_files))
                    commit_info["snakemake_rule_files"] = list(set(snakemake_rule_files))
                    commit_info["snakemake_rules_from_code"] = list(set(commit_info["snakemake_rules_from_code"]))
                    commit_info["snakemake_rules_from_code_before"] = list(set(commit_info["snakemake_rules_from_code_before"]))
                    commit_info["snakemake_modules_from_code"] = list(set(commit_info["snakemake_modules_from_code"]))
                    commit_info["snakemake_modules_from_code_before"] = list(set(commit_info["snakemake_modules_from_code_before"]))
                    commit_info["snakemake_n_rules_added"]     = len(set(commit_info["snakemake_rules_from_code"]) - set(commit_info["snakemake_rules_from_code_before"]))
                    commit_info["snakemake_n_rules_removed"]   = len(set(commit_info["snakemake_rules_from_code_before"]) - set(commit_info["snakemake_rules_from_code"]))
                    commit_info["snakemake_n_modules_added"]   = len(set(commit_info["snakemake_modules_from_code"]) - set(commit_info["snakemake_modules_from_code_before"]))
                    commit_info["snakemake_n_modules_removed"] = len(set(commit_info["snakemake_modules_from_code_before"]) - set(commit_info["snakemake_modules_from_code"]))

                if not self.date_first_commit or commit.committer_date < self.date_first_commit:
                    self.date_first_commit = commit.committer_date
                if not self.date_last_commit or commit.committer_date > self.date_last_commit:
                    self.date_last_commit = commit.committer_date

                self.commits[commit.hash] = commit_info

        except Exception as e:
            self.log.error("Error extracting git repo information: %s, line: %s", e, e.__traceback__.tb_lineno)
            return 0

        self.count_commits = len(self.commits)

        return self.count_commits


    def _get_snakemake_irregular_rule_files_from_code(self, code=""):
        """
        Return the list if there are included files having no .smk extension.
        """
        irregular_rule_files = []
        for line in code.splitlines():
            line = line.strip()
            if line.startswith("include:"):
                included_file = line.split(":")[1].strip().replace('"', '').replace("'", "")
                if included_file and not included_file.endswith(".smk"):
                    irregular_rule_files.append(included_file)

        return irregular_rule_files


    def _get_snakemake_included_files_from_code(self, code=""):
        """
        Return the list if there are included files having .smk extension.
        """
        included_files = []
        for line in code.splitlines():
            line = line.strip()
            if line.startswith("include:"):
                included_file = line.split(":")[1].strip().replace('"', '').replace("'", "")
                if included_file and included_file.endswith(".smk"):
                    included_files.append(included_file)

        return included_files


    def _get_file_extensions(self, filename=""):
        """
        Return the file extension from a file name.
        """
        parts = filename.split(".")
        if len(parts) > 1:
            return parts[-1].lower()
        return None


    def _get_snakemake_module_names_from_code(self, code=""):
        """
        Return the list of Snakemake modules from the code.
        """
        modules = list()
        for line in code.splitlines():
            # remove whitespaces and commit_parents
            line = line.strip()
            if line.startswith("module "):
                module_name = (line.split()[1]).split(":")[0]
                modules.append(module_name)

        return modules


    def _get_snakemake_rule_names_from_code(self, code=""):
        """
        Return the list of Snakemake rules from the code.
        """
        rules = list()
        for line in code.splitlines():
            # remove whitespaces and commit_parents
            line = line.strip()
            if line.startswith("rule "):
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
