"""This module contains the Database class and driver classes for the database"""

import sys
import logging
from abc import ABC, abstractmethod

class DatabaseDriverBase(ABC):
    """
    Abstract base class for database drivers.
    """
    def __init__(self, logger=logging.getLogger(), engine=None, git_provider=None):
        self.log = logger
        self.engine = engine
        self.git_provider = git_provider


    @abstractmethod
    def store_search_repositories_results(self, data):
        """
        Abstract method to store the search/repos results in the database.
        """
        pass


    @abstractmethod
    def close(self):
        """
        Abstract method to close the database connection.
        """


class Neo4jDriver(DatabaseDriverBase):
    """
    Database driver for Neo4j.
    """
    def __init__(self, logger=logging.getLogger(), engine=None, git_provider=None):
        super().__init__(logger, engine, git_provider)
        self.log.debug("Neo4jDriver initialize")
        pass

    def store_search_repositories_results(self, data):
        #self.log.debug(f"data: {data}")
        pass


    def close(self):
        self.log.debug("Neo4jDriver close")
        pass


class Database(DatabaseDriverBase):
    """
    Database class to interact with the database using the driver.
    """
    def __init__(self, logger=logging.getLogger(), engine=None, git_provider=None):
        super().__init__(logger, engine, git_provider)
        self._db_driver = self._create_database_driver(engine, logger)


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


    def store_search_repositories_results(self, data):
        """
        Insert data for search/repositories API
        """
        self._db_driver.store_search_repositories_results(data)


    def close(self):
        """
        Close the database connection using the driver.
        """
        self._db_driver.close()
