"""

    Github Plugin

    This plugin provides an interface to the Github v3 API.

    The Github Hook extends the HttpHook and accepts both Basic and
    Token Authentication. If both are available, the hook will use
    the specified token, which should be in the following format in
    the extras field: {"token":"XXXXXXXXXXXXXXXXXXXXX"}

        The host value in the Hook should contain the following:
        https://api.github.com/

    The Github Operator provides support for the following endpoints:
        Comments
        Commits
        Commit Comments
        Issue Comments
        Issues
        Members
        Organizations
        Pull Requests
        Repositories

"""

from airflow.plugins_manager import AirflowPlugin
from GithubPlugin.hooks.github_hook import GithubHook
from GithubPlugin.operators.github_to_s3_operator import GithubToS3Operator


class GithubPlugin(AirflowPlugin):
    name = "github_plugin"
    operators = [GithubToS3Operator]
    hooks = [GithubHook]
