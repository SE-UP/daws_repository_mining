"""This module contains the GitProvider class to interact with Git platforms
like GitHub, GitLab, etc."""

import sys
import logging
from abc import ABC, abstractmethod


class GitProviderBase(ABC):
    """
    Abstract base class for Git providers.
    """
    def __init__(self, logger, provider=None, token=None, **kwargs):
        self.log = logger
        self.api_token = token
        self.provider = provider
        self._provider_instance = provider
        self.initialize(**kwargs)


    @abstractmethod
    def initialize(self, **kwargs):
        """
        Abstract method to initialize the provider connection.
        """


    @abstractmethod
    def search_repositories(self, query):
        """
        Abstract method to search repositories in the provider.
        """
        return {}


    @abstractmethod
    def get_rate_limit(self):
        """
        Abstract method to get the status of the current rate limits.
        """
        return {}


    @abstractmethod
    def close(self):
        """
        Abstract method to close the provider connection.
        """


class GithubProvider(GitProviderBase):
    """
    Git provider for GitHub.
    """
    def initialize(self, **kwargs):
        self.log.debug("Token: %s", self.api_token)
        self.log.debug("GitProvider: %s", self.provider)

        self.log.debug("GitHubProvider initialize")
        self.log.debug(f"kwargs: {kwargs}")
        self.log.debug(self)

        if not self.api_token:
            raise ValueError("API token is required for Git provider")


    def search_repositories(self, query=""):
        self.log.info(f"Searching GitHub repositories with query: {query}")
        # Garbage data for test before implementing actual API call
        return {"provider": "GitHub", "results": [f"Repo-{query}-1", f"Repo-{query}-2"]}


    def get_rate_limit(self):
        self.log.debug("GitHubProvider get_rate_limit")
        return {"limit": 100, "remaining": 50}


    def close(self):
        self.log.debug("GitHubProvider close")


class GitlabProvider(GitProviderBase):
    """
    Git provider for GitLab.
    """
    def initialize(self, **kwargs):
        self.log.debug("GitLabProvider initialize")
        self.log.debug(f"kwargs: {kwargs}")


    def search_repositories(self, query):
        self.log.info(f"Searching GitLab repositories with query: {query}")
        # Garbage data for test before implementing actual API call
        return {"provider": "GitLab", "results": [f"LabRepo-{query}-1", f"LabRepo-{query}-2"]}


    def get_rate_limit(self):
        self.log.debug("GitLabProvider get_rate_limit")
        return {"limit": 100, "remaining": 50}


    def close(self):
        self.log.debug("GitLabProvider close")


class GitProvider:
    """
    GitProvider class to interact with various Git services using the provider.
    """
    def __init__(self, logger=logging.getLogger(), provider=None, token=None, **kwargs):
        self.log = logger
        self.api_token = token
        self.provider = provider

        self.log.debug("GitProvider: %s", provider)
        self.log.debug("Token: %s", token)

        self._provider_instance = self._create_provider_instance(**kwargs)
        self.initialize(**kwargs)


    def _create_provider_instance(self, **kwargs):
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

        self.log.debug(f"Using Git provider: {provider_class_name}")
        return provider_class(self.log, self.provider, self.api_token, **kwargs)


    def initialize(self, **kwargs):
        """
        Initialize the Git provider connection.
        """
        self._provider_instance.initialize(**kwargs)


    def search_repositories(self, query):
        """
        Search repositories using the current provider.
        """
        return self._provider_instance.search_repositories(query)


    def get_rate_limit(self):
        """
        Get the rate limit status of the current provider.
        """
        return self._provider_instance.get_rate_limit()


    def close(self):
        """
        Close the Git provider connection.
        """
        self._provider_instance.close()
