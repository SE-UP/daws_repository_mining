"""This module contains the GitProvider class to interact with Git platforms
like GitHub, GitLab, etc."""

import os
import re
import sys
import time
import logging
import importlib
import scripts.input_validator as input_validator
from email.utils import parsedate_to_datetime
from abc import ABC, abstractmethod
import requests

validator = input_validator.Validator()


class GitProviderBase(ABC):
    """
    Abstract base class for Git providers.
    """
    def __init__(self, logger=logging.getLogger(), provider=None, token=None,
                 checkout=False, interval_apicall=1, interval_clone=60,
                 http_timeout=10):
        self.log = logger
        self.api_token      = token
        self.provider       = provider
        self.checkout       = checkout
        self.http_timeout   = http_timeout
        self.per_page       = 100
        self.interval_apicall = interval_apicall
        self.interval_clone   = interval_clone


    @abstractmethod
    def search_repositories(self, query) -> dict:
        """
        Abstract method to search repositories.
        """


    @abstractmethod
    def call_api_repository(self, owner, repo) -> list:
        """
        Abstract method to get the repository details.
        """


    @abstractmethod
    def call_api_issues(self, owner, repo) -> list:
        """
        Abstract method to get the issues of a repository.
        """


    @abstractmethod
    def call_api_pull_request(self, owner, repo, issue_no) -> list:
        """
        Abstract method to get the pull request details of an issue.
        """


    @abstractmethod
    def call_api_issue_comments(self, owner, repo, issue_no) -> list:
        """
        Abstract method to get the comments of an issue.
        """


    @abstractmethod
    def call_api_issue_events(self, owner, repo, issue_no) -> list:
        """
        Abstract method to get the events of an issue.
        """


    @abstractmethod
    def call_api_commit_comments(self, owner, repo) -> list:
        """
        Abstract method to get the commit comments of a repository.
        """


    @abstractmethod
    def call_api_cicd_artifacts(self, owner, repo) -> list:
        """
        Abstract method to get the CI/CD artifacts of a repository.
        """


    @abstractmethod
    def call_api_cicd_workflows(self, owner, repo) -> list:
        """
        Abstract method to get the CI/CD workflows of a repository.
        """


    @abstractmethod
    def call_api_cicd_workflow_runs(self, owner, repo) -> list:
        """
        Abstract method to get the CI/CD workflow runs of a repository.
        """


    @abstractmethod
    def call_api_pages(self, owner, repo) -> list:
        """
        Abstract method to get the pages of a repository.
        """


    @abstractmethod
    def call_api_releases(self, owner, repo) -> list:
        """
        Abstract method to get the releases of a repository.
        """


    @abstractmethod
    def clone_repository(self, clone_dir, owner, repo_name, retry_count=5) -> None:
        """
        Abstract method to clone a repository.
        """


class GitProvider(GitProviderBase):
    """
    GitProvider class to interact with various Git services using the provider.
    """
    def __init__(self, logger=logging.getLogger(), provider=None, token=None,
                 checkout=False, interval_apicall=1, interval_clone=10,
                 http_timeout=10):
        super().__init__(logger=logger, provider=provider, token=token,
                         checkout=checkout, interval_apicall=interval_apicall,
                         interval_clone=interval_clone, http_timeout=http_timeout)
        if not self.api_token:
            raise ValueError("API token is required for Git provider")

        self._provider_instance = self._create_provider_instance()


    def _create_provider_instance(self):
        """
        Create a provider instance based on the provider type.
        """
        if not self.provider:
            raise ValueError("Git provider is required")

        provider_class_name = self.provider.capitalize() + "Provider"
        module = sys.modules[__name__]
        provider_class = getattr(module, provider_class_name, None)

        if not provider_class:
            raise ValueError(f"Unsupported Git provider: {self.provider}")

        self.log.debug(f"provider class name: {provider_class_name}")
        return provider_class(self.log, self.provider, self.api_token,
                              self.checkout, self.interval_apicall,
                              self.interval_clone, self.http_timeout)


    def search_repositories(self, query):
        """
        Search repositories.
        """
        return self._provider_instance.search_repositories(query)


    def call_api_repository(self, owner, repo):
        """
        Call API to get the repository details.
        """
        return self._provider_instance.call_api_repository(owner, repo)


    def call_api_issues(self, owner, repo):
        """
        Call API to get the issues of a repository.
        """
        return self._provider_instance.call_api_issues(owner, repo)


    def call_api_pull_request(self, owner, repo, issue_no):
        """
        Call API to get the pull request details of an issue.
        """
        return self._provider_instance.call_api_pull_request(owner, repo, issue_no)


    def call_api_issue_comments(self, owner, repo, issue_no):
        """
        Call API to get the comments of an issue.
        """
        return self._provider_instance.call_api_issue_comments(owner, repo, issue_no)


    def call_api_issue_events(self, owner, repo, issue_no):
        """
        Call API to get the events of an issue.
        """
        return self._provider_instance.call_api_issue_events(owner, repo, issue_no)


    def call_api_commit_comments(self, owner, repo):
        """
        Call API to get the commit comments of a repository.
        """
        return self._provider_instance.call_api_commit_comments(owner, repo)


    def call_api_cicd_artifacts(self, owner, repo):
        """
        Call API to get the CI/CD artifacts of a repository.
        """
        return self._provider_instance.call_api_cicd_artifacts(owner, repo)


    def call_api_cicd_workflows(self, owner, repo):
        """
        Call API to get the CI/CD workflows of a repository.
        """
        return self._provider_instance.call_api_cicd_workflows(owner, repo)


    def call_api_cicd_workflow_runs(self, owner, repo):
        """
        Call API to get the CI/CD workflow runs of a repository.
        """
        return self._provider_instance.call_api_cicd_workflow_runs(owner, repo)


    def call_api_pages(self, owner, repo):
        """
        Call API to get the pages of a repository.
        """
        return self._provider_instance.call_api_pages(owner, repo)


    def call_api_releases(self, owner, repo):
        """
        Call API to get the releases of a repository.
        """
        return self._provider_instance.call_api_releases(owner, repo)


    def clone_repository(self, clone_dir, owner, repo_name, retry_count=5) -> None:
        """
        Clone a repository from the provider.
        """
        return self._provider_instance.clone_repository(clone_dir, owner, repo_name, retry_count)


class GithubProvider(GitProviderBase):
    """
    Git provider for GitHub.
    """
    def __init__(self, logger=logging.getLogger(), provider=None, token=None,
                 checkout=False, interval_apicall=1, interval_clone=10,
                 http_timeout=10):
        super().__init__(logger=logger, provider=provider, token=token,
                         checkout=checkout, interval_apicall=interval_apicall,
                         interval_clone=interval_clone, http_timeout=http_timeout)
        self.base_url_api   = "https://api.github.com"
        self.base_url_clone = "https://github.com"
        self.http_headers = {
            "Accept": "application/json",
            "Authorization": f"token {self.api_token}"
        }

        self.log.debug("GitHubProvider initializing...: base_url_api: %s",
                       self.base_url_api)


    def _check_rate_limit(self, response_headers=None):
        if not response_headers:
            ratelimit_result = requests.get(
                f"{self.base_url_api}/rate_limit",
                headers=self.http_headers,
                timeout=self.http_timeout)

            if ratelimit_result.status_code != 200:
                error_message = f"Failed to get rate limit: {ratelimit_result.text}"
                raise requests.exceptions.HTTPError(error_message)

            response_headers = ratelimit_result.headers

        if not response_headers:
            raise ValueError("Response headers are required to check rate limit")

        limit = response_headers.get("X-RateLimit-Limit") or 0
        remaining = response_headers.get("X-RateLimit-Remaining") or 0
        epoch_reset = response_headers.get("X-RateLimit-Reset") or 0
        server_time = response_headers.get("Date") or ""
        epoch_now = int(parsedate_to_datetime(server_time).timestamp())
        reset_in_secs = int(epoch_reset) - int(epoch_now)

        self.log.debug("Rate limits: %d/%d, Reset in %d seconds", int(remaining),
                       int(limit), reset_in_secs)

        time.sleep(self.interval_apicall)

        if int(remaining) < 2:
            self.log.info("Rate limit exceeded. Waiting for %d seconds.",
                          reset_in_secs)
            time.sleep(reset_in_secs)


    def _call_api(self, api_url, pager=False, page=1, next_link=False):
        items = []
        total_count = 0
        found_next_link = False

        while True:
            if pager and found_next_link == False:
                api_url += f"&page={page}&per_page={self.per_page}"

            self.log.debug("Calling API (pager=%s, page=%d, next_link=%s): %s",
                           pager, page, next_link, api_url)

            response = requests.get(
                api_url,
                headers=self.http_headers,
                timeout=self.http_timeout)

            if response.status_code != 200:
                error_message = f"Failed to call API: {api_url}: {response.text}"
                raise requests.exceptions.HTTPError(error_message)

            if not response:
                raise ValueError("No response from API: %s", api_url)

            self._check_rate_limit(response.headers)

            item = response.json()
            if type(item) is not list:
                item = [item]

            count_items = len(item)
            items.extend(item)

            current_count   = count_items
            old_total_count = total_count
            total_count     = old_total_count + current_count

            self.log.debug("Retrieved %d+%d=%d items on the page %d from API: %s",
                           current_count, old_total_count, total_count,
                           page, api_url)

            if count_items < self.per_page or not pager:
                break

            page += 1

            if next_link:
                next_link_pattern = r'(?<=<)([\S]*)(?=>; rel="Next")'
                link_header = response.headers.get("Link")
                next_link = None
                if link_header:
                    next_link = re.search(next_link_pattern, link_header, re.IGNORECASE)
                if next_link:
                    found_next_link = True
                    api_url = next_link.group(0)
                    continue
                break

        self.log.debug("items: %s", items)
        return items


    def call_api_repository(self, owner, repo):
        api_url = f"{self.base_url_api}/repos/{owner}/{repo}"
        return self._call_api(api_url)


    def call_api_issues(self, owner, repo):
        api_url = f"{self.base_url_api}/repos/{owner}/{repo}/issues?state=all"
        return self._call_api(api_url, pager=True, page=1, next_link=True)


    def call_api_pull_request(self, owner, repo, issue_no):
        api_url = f"{self.base_url_api}/repos/{owner}/{repo}/pulls/{issue_no}"
        return self._call_api(api_url)


    def call_api_issue_comments(self, owner, repo, issue_no):
        api_url = f"{self.base_url_api}/repos/{owner}/{repo}/issues/{issue_no}/comments?per_page=100"
        return self._call_api(api_url, pager=True, page=1, next_link=False)


    def call_api_issue_events(self, owner, repo, issue_no):
        api_url = f"{self.base_url_api}/repos/{owner}/{repo}/issues/{issue_no}/events?per_page=100"
        return self._call_api(api_url, pager=True, page=1, next_link=False)


    def call_api_commit_comments(self, owner, repo):
        api_url = f"{self.base_url_api}/repos/{owner}/{repo}/comments?per_page=100"
        return self._call_api(api_url, pager=True, page=1, next_link=False)


    def call_api_cicd_artifacts(self, owner, repo):
        api_url = f"{self.base_url_api}/repos/{owner}/{repo}/actions/artifacts?per_page=100"
        response = self._call_api(api_url, pager=True, page=1, next_link=False)
        try:
            return response[0]["artifacts"]
        except KeyError:
            return []


    def call_api_cicd_workflows(self, owner, repo):
        api_url = f"{self.base_url_api}/repos/{owner}/{repo}/actions/workflows?per_page=100"
        response = self._call_api(api_url, pager=True, page=1, next_link=False)
        try:
            return response[0]["workflows"]
        except KeyError:
            return []


    def call_api_cicd_workflow_runs(self, owner, repo):
        api_url = f"{self.base_url_api}/repos/{owner}/{repo}/actions/runs?per_page=100"
        response = self._call_api(api_url, pager=True, page=1, next_link=False)
        try:
            return response[0]["workflow_runs"]
        except KeyError:
            return []


    def call_api_pages(self, owner, repo):
        api_url = f"{self.base_url_api}/repos/{owner}/{repo}/pages/builds?per_page=100"
        return self._call_api(api_url, pager=True, page=1, next_link=False)


    def call_api_releases(self, owner, repo):
        api_url = f"{self.base_url_api}/repos/{owner}/{repo}/releases?per_page=100"
        return self._call_api(api_url, pager=True, page=1, next_link=False)


    def search_repositories(self, query=None):
        self.log.info(f"Searching GitHub repositories with query: {query}")
        items         = []
        total_count   = 0
        current_page  = 1
        current_count = 0

        while True:
            request_url = f"{self.base_url_api}/search/repositories?q={query}&per_page=100&page={current_page}"
            search_results = requests.get(
                request_url,
                headers=self.http_headers,
                timeout=10)

            if search_results.status_code != 200:
                error_message = f"Failed to search repositories: {request_url}: {search_results.text}"
                raise requests.exceptions.HTTPError(error_message)

            response_headers  = search_results.headers
            search_results    = search_results.json()
            total_count       = search_results['total_count']
            total_pages       = int(total_count / 100) + 1
            current_count    += len(search_results['items'])

            items.extend(search_results['items'])

            self.log.debug("Page: %d/%d, Item count: %d/%d", current_page,
                           total_pages, current_count, total_count)

            current_page     += 1

            self._check_rate_limit(response_headers)

            if total_count == 0 or current_count >= total_count:
                break

        return {"provider": "github", "total_count": total_count, "items": items}


    def clone_repository(self, clone_dir, owner, repo_name, retry_count=5) -> None:
        pygit2 = importlib.import_module("pygit2")
        full_name = f"{owner}/{repo_name}"

        if self.checkout == False and os.path.exists(clone_dir):
            raise ValueError(f"Clone directory already exists: {clone_dir}")

        # Retry cloning the repo 5 times after 1 min pause for each if it fails.
        for try_count in range(retry_count):
            try:
                if self.checkout:
                    pygit2.Repository(clone_dir).checkout_head()
                else:
                    pygit2.clone_repository(self.base_url_clone + "/" + full_name + ".git",
                                          clone_dir)
                break
            except Exception as e:
                self.log.error("Failed to clone(or checkout) repo - %s : %s", full_name, e)
                self.log.info("Retrying after 1 min... (%d/5)", try_count)
                time.sleep(self.interval_clone)

        time.sleep(self.interval_clone)
