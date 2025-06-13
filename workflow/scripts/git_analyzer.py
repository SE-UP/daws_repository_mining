"""This module contains the GitAnalysis class to analyze Git repositories."""
import logging
import scripts.input_validator as input_validator
import scripts.util as util
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


    def extract_commits(self, analysis_for=""):
        """
        Extract information from a given Git repository.
        """
        analysis_for = analysis_for.lower()

        try:
            for commit in Repository(self.repo_path, num_workers=16).traverse_commits():
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

                if analysis_for == "snakemake":
                    commit_info = self._extend_commit_info_for_snakemake(commit_info)

                for file in commit.modified_files:
                    file_info = {
                        "old_path": file.old_path,
                        "new_path": file.new_path,
                        "filename": file.filename,
                        "change_type": file.change_type.name,
                        "n_added_lines": file.added_lines,
                        "n_deleted_lines": file.deleted_lines,
                        #"methods": [vars(method) for method in file.methods],
                        #"methods_before": [vars(method) for method in file.methods_before],
                        #"changed_methods": [vars(method) for method in file.changed_methods],
                        "n_lines": file.nloc,
                        "complexity": file.complexity,
                        "n_tokens": file.token_count
                    }

                    file_extension = self._get_file_extensions(file.filename)
                    if file_extension and file_extension not in commit_info["file_extensions"]:
                        commit_info["file_extensions"].append(file_extension)

                    if file.change_type.name not in commit_info["change_types"]:
                        commit_info["change_types"].append(file.change_type.name)

                    if analysis_for == "snakemake":
                        file_info = self._initialize_file_info_for_snakemake(file_info)
                        # TODO: Refactor this method since calculation is wrong
                        commit_info, file_info = self._extract_commit_for_snakemake(commit_info, file_info, file)

                    commit_info["files"].append(file_info)

                if analysis_for == "snakemake":
                    # TODO: Refactor this method since calculation is wrong
                    commit_info = self._post_process_commit_info_for_snakemake(commit_info)

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


    def _post_process_commit_info_for_snakemake(self, commit_info):
        commit_info["snakemake_included_rules"]         = list(set(commit_info["snakemake_included_rules"]))
        commit_info["snakemake_included_scripts"]       = list(set(commit_info["snakemake_included_scripts"]))
        commit_info["snakemake_irregular_rule_files"]   = list(set(commit_info["snakemake_irregular_rule_files"]))
        commit_info["snakemake_rule_files"]             = list(set(commit_info["snakemake_rule_files"]))
        commit_info["snakemake_rules_from_code"]        = list(set(commit_info["snakemake_rules_from_code"]))
        commit_info["snakemake_rules_from_code_before"] = list(set(commit_info["snakemake_rules_from_code_before"]))
        commit_info["snakemake_modules_from_code"]      = list(set(commit_info["snakemake_modules_from_code"]))
        commit_info["snakemake_modules_from_code_before"] = list(set(commit_info["snakemake_modules_from_code_before"]))
        commit_info["snakemake_n_rules_added"]     = len(set(commit_info["snakemake_rules_from_code"]) - set(commit_info["snakemake_rules_from_code_before"]))
        commit_info["snakemake_n_rules_removed"]   = len(set(commit_info["snakemake_rules_from_code_before"]) - set(commit_info["snakemake_rules_from_code"]))
        commit_info["snakemake_n_modules_added"]   = len(set(commit_info["snakemake_modules_from_code"]) - set(commit_info["snakemake_modules_from_code_before"]))
        commit_info["snakemake_n_modules_removed"] = len(set(commit_info["snakemake_modules_from_code_before"]) - set(commit_info["snakemake_modules_from_code"]))
        return commit_info


    def _extract_commit_for_snakemake(self, commit_info, file_info, file):
        is_snakemake_rule_file = (
            file.filename.lower() == "snakefile" or
            file.filename.lower().endswith(".smk") or
            file.filename.lower().endswith(".snakefile") or
            file.filename.lower().endswith(".rules")
        )

        if is_snakemake_rule_file:
            file_info["snakemake_related"] = True
            commit_info["snakemake_related"] = True
            commit_info["snakemake_rule_files"].append(file.new_path)
            source_code = file.source_code if file.source_code else ""
            source_code_before = file.source_code_before if file.source_code_before else ""

            commit_info["snakemake_irregular_rule_files"].extend(
                self._get_snakemake_irregular_rule_files_from_code(source_code))

            commit_info["snakemake_included_rules"].extend(
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
            if file.new_path.lower().startswith("scripts/") or file.new_path.lower().startswith("workflow/scripts/"):
                commit_info["snakemake_included_scripts"].append(file.new_path)
                commit_info["snakemake_related"] = True
                file_info["snakemake_related"] = True

        return commit_info, file_info


    def _extend_commit_info_for_snakemake(self, commit_info):
        commit_info["snakemake_related"] = False
        commit_info["snakemake_modules_from_code"]        = []
        commit_info["snakemake_modules_from_code_before"] = []
        commit_info["snakemake_rules_from_code"]          = []
        commit_info["snakemake_rules_from_code_before"]   = []
        commit_info["snakemake_n_modules_added"]   = 0
        commit_info["snakemake_n_modules_removed"] = 0
        commit_info["snakemake_n_rules_added"]     = 0
        commit_info["snakemake_n_rules_removed"]   = 0
        commit_info["snakemake_included_rules"] = []
        commit_info["snakemake_included_scripts"] = []
        commit_info["snakemake_irregular_rule_files"] = []
        commit_info["snakemake_rule_files"] = []

        return commit_info


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


    def _initialize_file_info_for_snakemake(self, file_info):
        file_info["snakemake_related"] = False
        file_info["snakemake_modules_from_code"]        = []
        file_info["snakemake_modules_from_code_before"] = []
        file_info["snakemake_rules_from_code"]          = []
        file_info["snakemake_rules_from_code_before"]   = []
        file_info["snakemake_n_modules_added"]   = 0
        file_info["snakemake_n_modules_removed"] = 0
        file_info["snakemake_n_rules_added"]     = 0
        file_info["snakemake_n_rules_removed"]   = 0
        return file_info


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


    def generate_event_logs_for_evolution_cycle(self, cycle_info, issues,
                                                 comments, events, pullrequests,
                                                 commits):

        begin_epoch = cycle_info["begin"]["epoch"]
        end_epoch   = cycle_info["end"]["epoch"]

        event_logs = []
        commits_preprocessed = []
        commit_parents = {}
        commits_hashes = []

        for issue in issues:
            created_at = util.str_to_datetime(date_str=issue["created_at"], fmt="%Y-%m-%dT%H:%M:%SZ")
            if not created_at:
                self.log.warning("Issue %s has no created_at date", issue["number"])
                continue

            created_at_epoch = int(created_at.timestamp())
            closed_at = None
            closed_at_epoch = None
            if issue.get("closed_at"):
                closed_at = util.str_to_datetime(date_str=issue["closed_at"], fmt="%Y-%m-%dT%H:%M:%SZ")

                if not closed_at:
                    self.log.warning("Issue %s has no closed_at date", issue["number"])
                    continue

                closed_at_epoch = int(closed_at.timestamp())

            if created_at_epoch >= begin_epoch and created_at_epoch <= end_epoch:
                event_logs.append({
                    "case_id": f"Issue-{issue['number']}",
                    "activity": "Issue Created",
                    "timestamp": created_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "user": issue["user"]["login"]
                })

            if closed_at and closed_at_epoch >= begin_epoch and closed_at_epoch <= end_epoch:
                event_logs.append({
                    "case_id": f"Issue-{issue['number']}",
                    "activity": "Issue Closed",
                    "timestamp": closed_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "user": issue["user"]["login"],
                })

        for comment in comments:
            created_at = util.str_to_datetime(date_str=comment["created_at"], fmt="%Y-%m-%dT%H:%M:%SZ")
            if not created_at:
                self.log.warning("Comment %s has no created_at date", comment["id"])
                continue

            created_at_epoch = int(created_at.timestamp())
            if created_at_epoch >= begin_epoch and created_at_epoch <= end_epoch:
                event_logs.append({
                    "case_id": f"Issue-{comment['issue_url'].split('/')[-1]}",
                    "activity": "Issue Commented",
                    "timestamp": created_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "user": comment["user"]["login"]
                })

        for event in events:
            created_at = util.str_to_datetime(date_str=event["created_at"], fmt="%Y-%m-%dT%H:%M:%SZ")
            if not created_at:
                self.log.warning("Event %s has no created_at date", event["id"])
                continue

            created_at_epoch = int(created_at.timestamp())
            if created_at_epoch >= begin_epoch and created_at_epoch <= end_epoch:
                user = None
                if event["actor"] and "login" in event["actor"]:
                    user = event["actor"]["login"]
                event_logs.append({
                    "case_id": f"Issue-{event['issue']['number']}",
                    "activity": event["event"],
                    "timestamp": created_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "user": user
                })

        for commit_hash in sorted(commits, key=lambda k: commits[k]['committer_epoch']):
            commit = commits[commit_hash]
            if commit_hash in commits_preprocessed:
                continue
            committed_epoch = int(commit["committer_epoch"])
            committed_at = util.epoch_to_str(epoch=committed_epoch)

            commits_hashes.append(commit_hash)

            if "parents" in commit:
                if len(commit["parents"]) > 0:
                    commit_parents[commit_hash] = commit["parents"]

            if committed_epoch >= begin_epoch and committed_epoch <= end_epoch:
                activity = "Committed"
                if cycle_info["n_snakemake_rules_added"] > 0 or cycle_info["n_snakemake_modules_added"] > 0:
                    activity = "Committed-Snakemake"
                event_logs.append({
                    "case_id": f"Commit-{commit_hash[:7]}",
                    "activity": activity,
                    "timestamp": committed_at,
                    "user": commit["committer"]
                })

        for pr in pullrequests:
            created_at = util.str_to_datetime(date_str=pr["created_at"], fmt="%Y-%m-%dT%H:%M:%SZ")
            if not created_at:
                self.log.warning("Pull Request %s has no created_at date", pr["number"])
                continue
            created_at_epoch = int(created_at.timestamp())

            if created_at_epoch >= begin_epoch and created_at_epoch <= end_epoch:
                event_logs.append({
                    "case_id": f"Issue-{pr['number']}",
                    "activity": "Pull Request Opened",
                    "timestamp": created_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "user": pr["user"]["login"]
                })

            # Find the corresponding commits
            merge_commit_sha = pr.get("merge_commit_sha")
            if merge_commit_sha and merge_commit_sha in commit_parents:
                parents = commit_parents[merge_commit_sha]
                if len(parents) == 1:
                    event_logs.append({
                        "case_id": f"Issue-{pr['number']}",
                        "activity": "Committed",
                        "timestamp": commits[parents[0]]["committer_date"],
                        "user": pr["user"]["login"]
                    })
                    commits_preprocessed.append(parents[0])
                elif len(parents) == 2:
                    begin_commit = parents[0]
                    end_commit = parents[1]
                    begin_index = commits_hashes.index(begin_commit)
                    end_index = commits_hashes.index(end_commit)
                    # printlog(f"Merge commit {merge_commit_sha} has parents: begin - {begin_commit} ({begin_index}) and end - {end_commit} ({end_index})")

                    child_commits = []
                    for i in range(begin_index, end_index):
                        child_commits.append(commits_hashes[i])

                    for commit in child_commits:
                        # printlog(f"Commit {commit} is a child of {begin_commit} and {end_commit}")
                        try:
                            event_logs.append({
                                "case_id": f"Issue-{pr['number']}",
                                "activity": "Committed",
                                "timestamp": commits[commit]["committer_date"],
                                "user": pr["user"]["login"]
                            })
                            commits_preprocessed.append(commit)
                        except KeyError:
                            self.log.warning("Commit %s not found in commits", commit)
                            continue

        return sorted(event_logs, key=lambda x: x["timestamp"])


    def extend_repo_info_general(self, repo_info=None, commits=None):
        """
        Extend the repository info from API
        """

        if not repo_info:
            raise ValueError("'repo_info' is required.")

        if not commits:
            raise ValueError("'commits' is required.")

        authors = set()
        committers = set()
        commit_hashes_sorted_by_epoch = sorted(commits, key=lambda k: commits[k]['committer_epoch'])

        repo_info["first_commit_at_epoch"] = util.now(is_epoch=True)
        repo_info["last_commit_at_epoch"] = 0

        for commit_hash in commit_hashes_sorted_by_epoch:
           commit = commits[commit_hash]
           if "author" in commit:
               authors.add(commit["author"])

           if "committer" in commit:
               committers.add(commit["committer"])

           if "committer_epoch" in commit:
               committer_epoch  = commit["committer_epoch"]
               if repo_info["first_commit_at_epoch"] > committer_epoch:
                   repo_info["first_commit_at_epoch"] = committer_epoch

               if repo_info["last_commit_at_epoch"] < committer_epoch:
                   repo_info["last_commit_at_epoch"] = committer_epoch

        repo_info["first_commit_at"] = util.epoch_to_str(
            epoch=repo_info["first_commit_at_epoch"],
            fmt="%Y-%m-%dT%H:%M:%S %z"
        )

        repo_info["last_commit_at"] = util.epoch_to_str(
            epoch=repo_info["last_commit_at_epoch"],
            fmt="%Y-%m-%dT%H:%M:%S %z"
        )

        repo_info["n_authors"] = len(authors)
        repo_info["n_committers"] = len(committers)

        return repo_info


    def extend_repo_info_for_snakemake(self, repo_info=None, commits=None):
        if not repo_info:
            raise ValueError("'repo_info' is required.")

        if not commits:
            raise ValueError("'commits' is required.")

        repo_info["n_commits_snakemake_related"] = 0
        repo_info["n_evolution_cycles"] = 0
        repo_info["evolution_cycles"] = []

        cycle_begin = {}
        cycle_end = {}
        cycle_n_commits = 0
        cycle_n_commits_snakemake_related = 0
        cycle_n_snakemake_rules_added = 0
        cycle_n_snakemake_rules_removed = 0
        cycle_n_snakemake_modules_added = 0
        cycle_n_snakemake_modules_removed = 0
        cycle_n_snakemake_rules_added_on_the_day = 0
        cycle_n_snakemake_rules_removed_on_the_day = 0
        cycle_n_snakemake_modules_added_on_the_day = 0
        cycle_n_snakemake_modules_removed_on_the_day = 0
        is_last_commit_of_the_day = False

        file_extensions = []
        commit_hashes_sorted_by_epoch = sorted(commits, key=lambda k: commits[k]['committer_epoch'])
        for i_commit, commit_hash in enumerate(commit_hashes_sorted_by_epoch):
           commit = commits[commit_hash]

           cycle_n_commits += 1
           if "snakemake_related" in commit and commit["snakemake_related"]:
               repo_info["n_commits_snakemake_related"] += 1

           cycle_current = {
               "hash": commit_hash,
               "epoch": commit["committer_epoch"],
           }

           if cycle_begin == {}:
               cycle_begin = cycle_current
               if cycle_end == {}:
                   cycle_n_commits = 1
               else:
                   cycle_n_commits = 0
           else:
               cycle_end = cycle_current
               if "snakemake_related" in commit and commit["snakemake_related"]:
                   cycle_n_commits_snakemake_related += 1

               added_or_removed = (
                   commit["snakemake_n_rules_added"] -
                   commit["snakemake_n_rules_removed"] or
                   commit["snakemake_n_modules_added"] -
                   commit["snakemake_n_modules_removed"]
               )

               if added_or_removed or is_last_commit_of_the_day:
                   diff_days = None
                   diff_mins = None
                   diff_hours = None
                   begin_epoch = cycle_begin["epoch"]
                   end_epoch   = cycle_end["epoch"]
                   if begin_epoch and end_epoch:
                       diff_days = util.convert_seconds(end_epoch - begin_epoch, unit="d")
                       diff_mins = util.convert_seconds(end_epoch - begin_epoch, unit="m")
                       diff_hours = util.convert_seconds(end_epoch - begin_epoch, unit="h")
                   else:
                       raise ValueError("Invalid epoch values for cycle begin and end.")

                   # Check if the next commit is within 24 hours
                   if i_commit + 1 < len(commits):
                       next_commit_hash = commit_hashes_sorted_by_epoch[i_commit + 1]
                       next_commit_epoch = commits[next_commit_hash]["committer_epoch"]
                       next_diff_days = util.convert_seconds(next_commit_epoch - end_epoch, unit="d")
                       if next_diff_days == 0:
                           cycle_n_snakemake_rules_added_on_the_day += commit["snakemake_n_rules_added"]
                           cycle_n_snakemake_rules_removed_on_the_day += commit["snakemake_n_rules_removed"]
                           cycle_n_snakemake_modules_added_on_the_day += commit["snakemake_n_modules_added"]
                           cycle_n_snakemake_modules_removed_on_the_day += commit["snakemake_n_modules_removed"]
                           if "file_extensions" in commit:
                               file_extensions += commit["file_extensions"]

                           is_last_commit_of_the_day = True
                           continue

                   repo_info["n_evolution_cycles"] += 1
                   cycle_n_snakemake_rules_added = commit["snakemake_n_rules_added"] + cycle_n_snakemake_rules_added_on_the_day
                   cycle_n_snakemake_rules_removed = commit["snakemake_n_rules_removed"] + cycle_n_snakemake_rules_removed_on_the_day
                   cycle_n_snakemake_modules_added = commit["snakemake_n_modules_added"] + cycle_n_snakemake_modules_added_on_the_day
                   cycle_n_snakemake_modules_removed = commit["snakemake_n_modules_removed"] + cycle_n_snakemake_modules_removed_on_the_day
                   if "file_extensions" in commit:
                       file_extensions += commit["file_extensions"]

                   repo_info["evolution_cycles"].append({
                       "begin": cycle_begin,
                       "end": cycle_end,
                       "diff_days": diff_days,
                       "diff_mins": diff_mins,
                       "diff_hours": diff_hours,
                       "n_commits": cycle_n_commits,
                       "n_commits_snakemake_related": cycle_n_commits_snakemake_related,
                       "n_snakemake_rules_added": cycle_n_snakemake_rules_added,
                       "n_snakemake_rules_removed": cycle_n_snakemake_rules_removed,
                       "n_snakemake_modules_added": cycle_n_snakemake_modules_added,
                       "n_snakemake_modules_removed": cycle_n_snakemake_modules_removed,
                       "file_extensions": list(set(file_extensions)),
                   })

                   cycle_begin = cycle_end
                   cycle_n_commits_snakemake_related = 0
                   cycle_n_commits = 0
                   cycle_n_snakemake_rules_added = 0
                   cycle_n_snakemake_rules_removed = 0
                   cycle_n_snakemake_modules_added = 0
                   cycle_n_snakemake_modules_removed = 0
                   cycle_n_snakemake_rules_added_on_the_day = 0
                   cycle_n_snakemake_rules_removed_on_the_day = 0
                   cycle_n_snakemake_modules_added_on_the_day = 0
                   cycle_n_snakemake_modules_removed_on_the_day = 0
                   is_last_commit_of_the_day = False
                   file_extensions = []

        # Store leftover cycle
        if cycle_begin != {} and cycle_end != {} and cycle_begin != cycle_end:
            repo_info["n_evolution_cycles"] += 1
            begin_epoch = cycle_begin["epoch"]
            end_epoch   = cycle_end["epoch"]
            diff_days = util.convert_seconds(end_epoch - begin_epoch, unit="d")
            diff_mins = util.convert_seconds(end_epoch - begin_epoch, unit="m")
            diff_hours = util.convert_seconds(end_epoch - begin_epoch, unit="h")

            repo_info["evolution_cycles"].append({
                "begin": cycle_begin,
                "end": cycle_end,
                "diff_days": diff_days,
                "diff_mins": diff_mins,
                "diff_hours": diff_hours,
                "n_commits": cycle_n_commits,
                "n_commits_snakemake_related": cycle_n_commits_snakemake_related,
                "n_snakemake_rules_added": cycle_n_snakemake_rules_added,
                "n_snakemake_rules_removed": cycle_n_snakemake_rules_removed,
                "n_snakemake_modules_added": cycle_n_snakemake_modules_added,
                "n_snakemake_modules_removed": cycle_n_snakemake_modules_removed,
                "file_extensions": list(set(file_extensions)),
            })

        return repo_info
