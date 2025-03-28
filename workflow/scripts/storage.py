"""This module contains the Storage class and driver classes for storage"""
import sys
import json
import pathlib
import logging
from abc import ABC, abstractmethod
from typing import Union

class StorageDriverBase(ABC):
    """
    Abstract base class for storage drivers.
    """
    def __init__(self, logger=logging.getLogger(), engine=None,
                 storage_config=None):

        if not engine:
            raise ValueError("Storage engine is required")

        self.log          = logger
        self.engine       = engine
        self.storage_config = storage_config


    @abstractmethod
    def read(self, path=None, from_json=False,
             multiple_lines=False, mode='r') -> Union[str, dict, list]:
        """
        Abstract method to read data from storage.
        """

    @abstractmethod
    def write(self, path=None, data=None, to_json=False, mode='w'):
        """
        Abstract method to write data to storage.
        """


class FileDriver(StorageDriverBase):
    """
    Storage driver for File:
        storage_config: (either rootdir or is_absolute is required)
            rootdir: storage root directory. `path` is treated as relative to this.
            is_absolute: If True, rootdir is absolute path.
            mkdir_ok: If True, create rootdir if not exists.
    """
    def __init__(self, logger=logging.getLogger(), engine=None,
                 storage_config=None):

        super().__init__(logger, engine, storage_config)

        if not storage_config:
            raise ValueError("storage_config is required")

        self.is_absolute = storage_config.get("is_absolute", False)
        self.mkdir_ok = storage_config.get("mkdir_ok", False)
        self.rootdir = storage_config.get("rootdir", None)

        if not self.is_absolute and not self.rootdir:
            raise ValueError("rootdir or is_absolute is required in storage_config")

        self.log.debug("FileDriver configs: %s", storage_config)

        if self.rootdir:
            path = pathlib.Path(self.rootdir)
            if not path.exists():
                if self.mkdir_ok:
                    try:
                        path.mkdir(parents=True)
                        self.log.info("Created rootdir: %s", self.rootdir)
                    except Exception as e:
                        self.log.error("Error creating rootdir: %s", e)
                        raise e
                else:
                    raise ValueError(f"rootdir does not exist: {self.rootdir}")


    def read(self, path=None, from_json=False, multiple_lines=False, mode='r'):
        """
        Read data from file.
        """
        if path is None:
            raise ValueError("path is required")

        path_handler = None
        if self.is_absolute:
            path_handler = pathlib.Path(path)
        else:
            if path.startswith("/"):
                path = path[1:]
            path_handler = pathlib.Path(self.rootdir) / path

        if not path_handler.exists():
            raise FileNotFoundError(f"File not found: {path_handler}")

        absolute_path = str(path_handler)
        with open(absolute_path, mode, encoding='UTF-8') as f:
            if from_json:
                return json.load(f)

            if multiple_lines:
                return [line.strip() for line in f.readlines()]

            return f.read()


    def write(self, path=None, data=None, to_json=False, mode='w'):
        """
        Write data to file.
        """
        if data is None:
            raise ValueError("data is required")

        if path is None:
            raise ValueError("path is required")

        path_handler = None
        if self.is_absolute:
            path_handler = pathlib.Path(path)
        else:
            if path.startswith("/"):
                path = path[1:]
            path_handler = pathlib.Path(self.rootdir) / path

        if not path_handler.parent.exists():
            if self.mkdir_ok:
                path_handler.parent.mkdir(parents=True)
            else:
                raise FileNotFoundError(f"Directory not found: {path_handler.parent}")

        data_type = type(data)
        absolute_path = str(path_handler)
        with open(absolute_path, mode, encoding='UTF-8') as f:
            if data_type == dict or to_json:
                json.dump(data, f, indent=4)
            elif data_type == list:
                for line in data:
                    line = line.strip()
                    f.write(f"{line}\n")
            else:
                f.write(data)


class Storage(StorageDriverBase):
    """
    Storage class to interact with storage using the driver.
    """
    def __init__(self, logger=logging.getLogger(), engine=None,
                 storage_config=None):

        super().__init__(logger, engine, storage_config)
        self._storage_driver = self._create_storage_driver(engine, logger, storage_config)


    def _create_storage_driver(self, engine, logger, storage_config):
        """
        Create a storage driver instance based on the engine type.
        """
        driver_class_name = engine.capitalize() + "Driver"
        module            = sys.modules[__name__]
        driver_class      = getattr(module, driver_class_name, None)

        if not driver_class:
            raise ValueError(f"Unsupported storage engine: {engine}")

        self.log.info(f"Using storage driver: {driver_class_name}")
        return driver_class(logger, self.engine, storage_config)


    def read(self, path=None, from_json=False, multiple_lines=False, mode='r'):
        """
        Read data from storage using the driver.
        """
        return self._storage_driver.read(path, from_json, multiple_lines, mode)

    def write(self, path=None, data=None, to_json=False, mode='w'):
        """
        Write data to storage using the driver.
        """
        return self._storage_driver.write(path, data, to_json, mode)
