"""This module contains the Database class and driver classes for the database"""

import sys
import logging
from abc import ABC, abstractmethod

class DatabaseDriverBase(ABC):
    """
    Abstract base class for database drivers.
    """
    def __init__(self, logger):
        self.log = logger


    @abstractmethod
    def initialize(self, **kwargs):
        """
        Abstract method to initialize the database connection.
        """
        pass


    @abstractmethod
    def store_github_repository_search_results(self, data):
        """
        Abstract method to store data for GitHub search/repositories API.
        """
        pass


    @abstractmethod
    def close(self):
        """
        Abstract method to close the database connection.
        """


class MongoDBDriver(DatabaseDriverBase):
    """
    Database driver for MongoDB.
    """
    def initialize(self, **kwargs):
        self.log.info("MongoDBDriver initialize")
        self.log.info(f"kwargs: {kwargs}")
        pass


    def store_github_repository_search_results(self, data):
        self.log.info(f"data: {data}")
        pass


    def close(self):
        self.log.info("MongoDBDriver close")
        pass


class Neo4jDriver(DatabaseDriverBase):
    """
    Database driver for Neo4j.
    """
    def initialize(self, **kwargs):
        self.log.debug("Neo4jDriver initialize")
        self.log.debug(f"kwargs: {kwargs}")
        pass


    def store_github_repository_search_results(self, data):
        self.log.debug(f"data: {data}")
        pass


    def close(self):
        self.log.debug("Neo4jDriver close")
        pass


class Database(DatabaseDriverBase):
    """
    Database class to interact with the database using the driver.
    """
    def __init__(self, logger=logging.getLogger(), engine="neo4j", **kwargs):
        super().__init__(logger)
        self.log = logger
        self._db_driver = self._create_database_driver(engine, logger)
        self.initialize(**kwargs)


    def _create_database_driver(self, engine, logger):
        """
        Create a database driver instance based on the engine type.
        """
        driver_class_name = engine.capitalize() + "Driver"
        module            = sys.modules[__name__]
        driver_class      = getattr(module, driver_class_name, None)

        if not driver_class:
            raise ValueError(f"Unsupported database engine: {engine}")

        self.log.info(f"Using database driver: {driver_class_name}")
        return driver_class(logger)


    def initialize(self, **kwargs):
        """
        Initialize the database connection using the driver.
        """
        self._db_driver.initialize(**kwargs)


    def store_github_repository_search_results(self, data):
        """
        Insert data for GitHub search/repositories API
        """
        self._db_driver.store_github_repository_search_results(data)


    def close(self):
        """
        Close the database connection using the driver.
        """
        self._db_driver.close()
