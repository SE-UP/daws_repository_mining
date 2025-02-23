"""This module contains the GitProvider class to interact with Git platforms
like GitHub, GitLab, etc."""

import os
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
        Abstract method to search repositories in the provider.
        """


    @abstractmethod
    def clone_repositories(self, basedir=None, repos=None, skiplist=None):
        """
        Abstract method to clone repositories from the provider.
        """
        return []


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
        Search repositories using the current provider.
        """
        return self._provider_instance.search_repositories(query)


    def clone_repositories(self, basedir=None, repos=None, skiplist=None):
        """
        Clone repositories using the current provider.
        """
        return self._provider_instance.clone_repositories(basedir, repos, skiplist)


class GithubProvider(GitProviderBase):
    """
    Git provider for GitHub.
    """
    def __init__(self, logger=logging.getLogger(), provider=None, token=None):
        super().__init__(logger=logger, provider=provider, token=token)
        self.base_url_api   = "https://api.github.com"
        self.base_url_clone = "https://github.com"
        self.wait_sec_api = 2
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


    def search_repositories(self, query=None):
        self.log.info(f"Searching GitHub repositories with query: {query}")
        items         = []
        total_count   = 0
        current_page  = 1
        current_count = 0

        while True:
            search_results = requests.get(
                f"{self.base_url_api}/search/repositories?q={query}&per_page=100&page={current_page}",
                headers=self.http_headers,
                timeout=10)

            if search_results.status_code != 200:
                error_message = f"Failed to search repositories: {search_results.text}"
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


    def clone_repositories(self, basedir=None, repos=None, skiplist=None):
        pygit2 = importlib.import_module("pygit2")

        if not basedir:
            raise ValueError("Base directory is required to clone repositories")

        if not repos:
            return []

        cloned_repos = []
        skiplist = skiplist or []
        total_repos = len(repos)
        total_cloned = 0

        for full_name in repos:
            total_cloned += 1
            if full_name in skiplist:
                self.log.info("(%d/%d) Skipping repo: %s", total_cloned, total_repos, full_name)
                continue

            # replace / in full_name with _.
            clonedir = basedir + "/" + full_name.replace("/", "_")

            if os.path.exists(clonedir):
                self.log.info("(%d/%d) Repo already exists: %s", total_cloned,
                              total_repos, full_name)
                cloned_repos.append(full_name)
                continue

            self.log.info("(%d/%d) Cloning repo: %s", total_cloned, total_repos, full_name)

            # Retry cloning the repo 5 times after 1 min pause for each if it fails.
            for _ in range(5):
                try:
                    pygit2.clone_repository(self.base_url_clone + "/" + full_name + ".git",
                                          clonedir)
                    break
                except Exception as e:
                    self.log.error("Failed to clone repo - %s : %s", full_name, e)
                    self.log.info("Retrying after 1 min... (%d/5)", _)
                    time.sleep(self.wait_sec_clone)

            time.sleep(self.wait_sec_clone)

            cloned_repos.append(full_name)

        return cloned_repos
