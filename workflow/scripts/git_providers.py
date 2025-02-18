"""This module contains the GitProvider class to interact with Git platforms
like GitHub, GitLab, etc."""

import sys
import time
import logging
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


class GithubProvider(GitProviderBase):
    """
    Git provider for GitHub.
    """
    def __init__(self, logger=logging.getLogger(), provider=None, token=None):
        super().__init__(logger=logger, provider=provider, token=token)
        self.base_url = "https://api.github.com"
        self.default_wait_time = 2
        self.http_headers = {
            "Accept": "application/json",
            "Authorization": f"token {self.api_token}"
        }

        self.log.debug("GitHubProvider initializing...: base_url: %s",
                       self.base_url)


    def _check_rate_limit(self, response_headers=None):
        if not response_headers:
            return None

        limit = response_headers.get("X-RateLimit-Limit")
        remaining = response_headers.get("X-RateLimit-Remaining")
        epoch_reset = response_headers.get("X-RateLimit-Reset")
        server_time = response_headers.get("Date")
        epoch_now = int(parsedate_to_datetime(server_time).timestamp())
        reset_in_secs = int(epoch_reset) - int(epoch_now)

        self.log.debug("Rate limits: %d/%d, Reset in %d seconds", int(remaining),
                       int(limit), reset_in_secs)

        time.sleep(self.default_wait_time)

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
                f"{self.base_url}/search/repositories?q={query}&per_page=100&page={current_page}",
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
