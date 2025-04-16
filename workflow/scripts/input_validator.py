"""This module is used to validate input data."""
import os

class Validator:
    """Validator class to validate input data."""

    def __init__(self):
        """Initialize the Validator class."""
        pass


    def validate(self, value=None, expected_type=None, required=True, default=None):
        """Validate the input data based on the expected type and other parameters."""
        if value is None:
            if required:
                raise ValueError("Value is required.")

            if default:
                return default
            return None

        if expected_type is None:
            return value

        if not isinstance(value, expected_type):
            raise ValueError(f"Expected: {expected_type}, actual: {type(value)}.")

        return value


    def path_dir(self, given_path=None, create=False):
        """Validate a directory path. Return the path if it is valid."""
        if given_path is None:
            raise ValueError("path parameter is required.")

        if not os.path.exists(given_path):
            if create:
                try:
                    os.makedirs(given_path, exist_ok=True)
                except Exception as e:
                    raise ValueError("Unable to create %s: %s" % (given_path, str(e)))
            else:
                raise ValueError("%s does not exist." % given_path)

        if not os.path.isdir(given_path):
            raise ValueError("%s is not a directory." % given_path)

        return given_path


    def format_git_fullname(self, fullname=None):
        """Validate the full name of a repository on Git provider's service."""
        if fullname is None:
            raise ValueError("Fullname parameter is required.")

        if not fullname.find("/") > 0:
            raise ValueError("Fullname's format must be <owner>/<repo>.")

        return fullname


    def format_git_commit_hash(self, hash=None):
        """Validate a git commit hash."""
        if hash is None:
            raise ValueError("Hash parameter is required.")

        if not isinstance(hash, str):
            raise ValueError("Hash must be a string.")

        if len(hash) != 40:
            raise ValueError("Hash must be 40 characters long.")

        return hash
