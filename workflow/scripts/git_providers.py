"""This module contains the GitProvider class to interact with Git platforms
like GitHub, GitLab, etc."""

import os
import re
import sys
import time
import logging
import importlib
from email.utils import parsedate_to_datetime
from abc import ABC, abstractmethod
import requests


class GitProviderBase(ABC):
    """
    Abstract base class for Git providers.
    """
    def __init__(self, logger=logging.getLogger(), provider=None, token=None):
        self.log = logger
        self.api_token = token
        self.provider = provider


    @abstractmethod
    def search_repositories(self, query) -> dict:
        """
        Abstract method to search repositories.
        """


    @abstractmethod
    def get_commit_comments(self, owner=None, repo=None) -> list:
        """
        Abstract method to get the commit comments of a repository.
        """


    @abstractmethod
    def get_releases(self, owner=None, repo=None) -> list:
        """
        Abstract method to get the releases of a repository.
        """


    @abstractmethod
    def get_issues(self, owner=None, repo=None) -> dict:
        """
        Abstract method to get issues of a repository.
        """


    @abstractmethod
    def get_issue_comments(self, owner=None, repo=None, issue_number=None) -> list:
        """
        Abstract method to get the comments of an issue.
        """


    @abstractmethod
    def get_issue_events(self, owner=None, repo=None, issue_number=None) -> list:
        """
        Abstract method to get the events of an issue.
        """


    @abstractmethod
    def get_pullrequest_details(self, owner=None, repo=None, issue_number=None) -> list:
        """
        Abstract method to get the pull request details of an issue.
        """


    @abstractmethod
    def clone_repositories(self, basedir=None, repos=None) -> list:
        """
        Abstract method to clone repositories.
        """


class GitProvider(GitProviderBase):
    """
    GitProvider class to interact with various Git services using the provider.
    """
    def __init__(self, logger=logging.getLogger(), provider=None, token=None):
        super().__init__(logger=logger, provider=provider, token=token)
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
        return provider_class(self.log, self.provider, self.api_token)


    def search_repositories(self, query):
        """
        Search repositories.
        """
        return self._provider_instance.search_repositories(query)


    def get_commit_comments(self, owner=None, repo=None):
        """
        Get the commit comments of a repository.
        """
        return self._provider_instance.get_commit_comments(owner, repo)


    def get_releases(self, owner=None, repo=None):
        """
        Get the releases of a repository.
        """
        return self._provider_instance.get_releases(owner, repo)


    def get_issues(self, owner=None, repo=None):
        """
        Get issues of a repository.
        """
        return self._provider_instance.get_issues(owner, repo)


    def get_issue_comments(self, owner=None, repo=None, issue_number=None):
        """
        Get the comments of an issue.
        """
        return self._provider_instance.get_issue_comments(owner, repo, issue_number)


    def get_issue_events(self, owner=None, repo=None, issue_number=None):
        """
        Get the comments of an issue.
        """
        return self._provider_instance.get_issue_events(owner, repo, issue_number)


    def get_pullrequest_details(self, owner=None, repo=None, issue_number=None):
        """
        Get the pull request details of an issue.
        """
        return self._provider_instance.get_pullrequest_details(owner, repo, issue_number)


    def clone_repositories(self, basedir=None, repos=None):
        """
        Clone repositories using the current provider.
        """
        return self._provider_instance.clone_repositories(basedir, repos)


class GithubProvider(GitProviderBase):
    """
    Git provider for GitHub.
    """
    def __init__(self, logger=logging.getLogger(), provider=None, token=None):
        super().__init__(logger=logger, provider=provider, token=token)
        self.base_url_api   = "https://api.github.com"
        self.base_url_clone = "https://github.com"
        self.wait_sec_api = 0.8
        self.wait_sec_clone = 60
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
                timeout=10)

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

        time.sleep(self.wait_sec_api)

        if int(remaining) < 2:
            self.log.info("Rate limit exceeded. Waiting for %d seconds.",
                          reset_in_secs)
            time.sleep(reset_in_secs)


    def get_commit_comments(self, owner=None, repo=None):
        request_url = f"{self.base_url_api}/repos/{owner}/{repo}/comments?per_page=100"

        all_comments = []
        current_page = 1
        while True:
            request_url_with_page = request_url + f"&page={current_page}"
            response = requests.get(
                request_url_with_page,
                headers=self.http_headers,
                timeout=10)

            if response.status_code != 200:
                error_message = f"Failed to get comments: {request_url_with_page}: {response.text}"
                raise requests.exceptions.HTTPError(error_message)

            if not response:
                raise ValueError("No comments found for %s/%s.",
                                 owner, repo)

            self.log.debug("Got %d comments of %s/%s.", len(response.json()),
                           owner, repo)

            self._check_rate_limit(response.headers)

            count_comments = len(response.json())
            all_comments.extend(response.json())

            if count_comments < 100:
                break

            current_page += 1

        return all_comments


    def get_releases(self, owner=None, repo=None):
        request_url = f"{self.base_url_api}/repos/{owner}/{repo}/releases?per_page=100"

        all_releases = []
        current_page = 1
        while True:
            request_url_with_page = request_url + f"&page={current_page}"
            response = requests.get(
                request_url_with_page,
                headers=self.http_headers,
                timeout=10)

            if response.status_code != 200:
                error_message = f"Failed to get releases: {request_url_with_page}: {response.text}"
                raise requests.exceptions.HTTPError(error_message)

            if not response:
                raise ValueError("No releases found for %s/%s.",
                                 owner, repo)

            self.log.debug("Got %d releases of %s/%s.", len(response.json()),
                           owner, repo)

            self._check_rate_limit(response.headers)

            count_releases = len(response.json())
            all_releases.extend(response.json())

            if count_releases < 100:
                break

            current_page += 1

        return all_releases


    def get_issue_comments(self, owner=None, repo=None, issue_number=None):
        request_url = f"{self.base_url_api}/repos/{owner}/{repo}/issues/{issue_number}/comments?per_page=100"

        all_comments = []
        current_page = 1
        while True:
            request_url_with_page = request_url + f"&page={current_page}"
            response = requests.get(
                request_url_with_page,
                headers=self.http_headers,
                timeout=10)

            if response.status_code != 200:
                error_message = f"Failed to get comments: {request_url_with_page}: {response.text}"
                raise requests.exceptions.HTTPError(error_message)

            if not response:
                raise ValueError("No comments found for %s/%s, issue no. %d.",
                               owner, repo, issue_number)

            self.log.debug("Got %d comments of issue no. %d of %s/%s.", len(response.json()),
                           issue_number, owner, repo)

            self._check_rate_limit(response.headers)

            count_comments = len(response.json())
            all_comments.extend(response.json())

            if count_comments < 100:
                break

            current_page += 1

        return all_comments


    def get_issue_events(self, owner=None, repo=None, issue_number=None):
        request_url = f"{self.base_url_api}/repos/{owner}/{repo}/issues/{issue_number}/events?per_page=100"

        all_events = []
        current_page = 1
        while True:
            request_url_with_page = request_url + f"&page={current_page}"
            response = requests.get(
                request_url_with_page,
                headers=self.http_headers,
                timeout=10)

            if response.status_code != 200:
                error_message = f"Failed to get events: {request_url_with_page}: {response.text}"
                raise requests.exceptions.HTTPError(error_message)

            if not response:
                raise ValueError("No events found for %s/%s, issue no. %d.",
                               owner, repo, issue_number)

            self.log.debug("Got %d events of issue no. %d of %s/%s.", len(response.json()),
                           issue_number, owner, repo)

            self._check_rate_limit(response.headers)

            events = response.json()
            count_events = len(events)
            if count_events > 0:
                for event in events:
                    event["issue"] = {"number": issue_number}
            all_events.extend(events)

            if count_events < 100:
                break

            current_page += 1

        return all_events


    def get_pullrequest_details(self, owner=None, repo=None, issue_number=None):
        request_url = f"{self.base_url_api}/repos/{owner}/{repo}/pulls/{issue_number}"

        response = requests.get(request_url, headers=self.http_headers, timeout=10)

        if response.status_code != 200:
            error_message = f"Failed to get pull request details: {request_url}: {response.text}"
            raise requests.exceptions.HTTPError(error_message)

        if not response:
            raise ValueError("No pull request details of issue no. %d of %s/%s.",
                           issue_number, owner, repo)

        self.log.debug("Got pull request details of issue no. %d of %s/%s.", issue_number, owner, repo)
        self._check_rate_limit(response.headers)

        return response.json()


    def get_issues(self, owner=None, repo=None):
        items         = []
        total_count   = 0
        current_page  = 1
        current_count = 0
        next_link_pattern = r'(?<=<)([\S]*)(?=>; rel="Next")'

        request_url = f"{self.base_url_api}/repos/{owner}/{repo}/issues?state=all&per_page=100&page={current_page}"

        while True:
            issues_results = requests.get(
                request_url,
                headers=self.http_headers,
                timeout=10)

            if issues_results.status_code != 200:
                error_message = f"Failed to get issues: {request_url}: {issues_results.text}"
                raise requests.exceptions.HTTPError(error_message)

            response_headers  = issues_results.headers
            issues_results    = issues_results.json()
            current_count     = len(issues_results)
            old_total_count   = total_count
            new_total_count   = old_total_count + current_count

            self._check_rate_limit(response_headers)

            if not issues_results:
                break

            items.extend(issues_results)

            self.log.debug("Retrieved %d+%d=%d items on the page %d in %s/%s.",
                           current_count, old_total_count, new_total_count,
                           current_page, owner, repo)

            total_count = new_total_count

            link_header = response_headers.get("Link")
            next_link = None

            if link_header:
                next_link = re.search(next_link_pattern, link_header, re.IGNORECASE)

            if next_link:
                current_page += 1
                request_url = next_link.group(0)
                continue

            break

        return {"provider": "github", "total_count": total_count, "items": items}


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


    def clone_repositories(self, basedir=None, repos=None):
        pygit2 = importlib.import_module("pygit2")

        if not basedir:
            raise ValueError("Base directory is required to clone repositories")

        if not repos:
            return []

        cloned_repos = []
        total_repos = len(repos)
        total_cloned = 0

        for full_name in repos:
            total_cloned += 1

            # replace / in full_name with _.
            clonedir = basedir + "/" + full_name.replace("/", "_")

            if os.path.exists(clonedir):
                self.log.info("(%d/%d) Repo already exists: %s", total_cloned,
                              total_repos, full_name)
                cloned_repos.append(full_name)
                continue

            self.log.info("(%d/%d) Cloning repo: %s", total_cloned, total_repos, full_name)

            # Retry cloning the repo 5 times after 1 min pause for each if it fails.
            for try_count in range(5):
                try:
                    pygit2.clone_repository(self.base_url_clone + "/" + full_name + ".git",
                                          clonedir)
                    break
                except Exception as e:
                    self.log.error("Failed to clone repo - %s : %s", full_name, e)
                    self.log.info("Retrying after 1 min... (%d/5)", try_count)
                    time.sleep(self.wait_sec_clone)

            time.sleep(self.wait_sec_clone)

            cloned_repos.append(full_name)

        return cloned_repos
