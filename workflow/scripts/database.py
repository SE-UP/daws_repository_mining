"""This module contains the Database class and driver classes for the database"""

import sys
import logging
import importlib
from abc import ABC, abstractmethod

class DatabaseDriverBase(ABC):
    """
    Abstract base class for database drivers.
    """
    def __init__(self, logger=logging.getLogger(), engine=None, git_provider=None,
                 db_config=None):

        if not db_config:
            raise ValueError("Database configuration is required")

        if not engine:
            raise ValueError("Database engine is required")

        self.log          = logger
        self.engine       = engine
        self.git_provider = git_provider
        self.host         = db_config["host"]
        self.username     = db_config["username"]
        self.password     = db_config["password"]
        self.port         = db_config["port"]
        self.db_name      = db_config["db_name"]


    @abstractmethod
    def store_search_repositories_results(self, data):
        """
        Abstract method to store the search/repos results in the database.
        """


    @abstractmethod
    def close(self):
        """
        Abstract method to close the database connection.
        """


class Neo4jDriver(DatabaseDriverBase):
    """
    Database driver for Neo4j.
    """
    def __init__(self, logger=logging.getLogger(), engine=None, git_provider=None,
                 db_config=None):

        super().__init__(logger, engine, git_provider, db_config)
        self._db_handler = importlib.import_module(self.engine).GraphDatabase
        self._endpoint = f"neo4j://{self.host}:{self.port}"

        self.log.debug(f"Neo4jDriver: endpoint: {self._endpoint}")

        try:
            self._session = self._db_handler.driver(
                self._endpoint,
                auth=(self.username, self.password))

            # DB connection check
            with self._session.session(database=self.db_name) as session:
                session.run("MATCH (n) RETURN n LIMIT 1")

        except Exception as e:
            self.log.error("Error connecting to Neo4j: %s", e)
            raise e


    def store_search_repositories_results(self, data):
        for repo in data["items"]:
            self.log.debug(f"Storing repository: {repo['full_name']}")
            try:
                self._session.execute_query(
                    """
                    MERGE (r:Repository {id: $id})
                    SET r.name = $name, r.full_name = $full_name,
                        r.html_url = $html_url, r.description = $description,
                        r.stargazers_count = $stargazers_count,
                        r.forks_count = $forks_count
                    """,
                    id=repo["id"],
                    name=repo["name"],
                    full_name=repo["full_name"],
                    html_url=repo["html_url"],
                    description=repo["description"],
                    stargazers_count=repo["stargazers_count"],
                    forks_count=repo["forks_count"],
                    database=self.db_name
                )

            except Exception as e:
                self.log.error(f"Error storing data in Neo4j: {e}")
                raise e

    def close(self):
        self.log.debug("Neo4jDriver close")
        self._session.close()


class Database(DatabaseDriverBase):
    """
    Database class to interact with the database using the driver.
    """
    def __init__(self, logger=logging.getLogger(), engine=None,
                 git_provider=None, db_config=None):

        super().__init__(logger, engine, git_provider, db_config)
        self._db_driver = self._create_database_driver(engine, logger, db_config)


    def _create_database_driver(self, engine, logger, db_config):
        """
        Create a database driver instance based on the engine type.
        """
        driver_class_name = engine.capitalize() + "Driver"
        module            = sys.modules[__name__]
        driver_class      = getattr(module, driver_class_name, None)

        if not driver_class:
            raise ValueError(f"Unsupported database engine: {engine}")

        self.log.info(f"Using database driver: {driver_class_name}")
        return driver_class(logger, self.engine, self.git_provider, db_config)


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
