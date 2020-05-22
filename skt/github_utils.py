import base64

from github import Github
from contextlib import contextmanager


@contextmanager
def proxy(proxies):
    import os

    env_backup = dict(os.environ)
    os.environ["HTTP_PROXY"] = proxies["http"]
    os.environ["HTTPS_PROXY"] = proxies["https"]
    yield
    os.environ.clear()
    os.environ.update(env_backup)


class GithubUtil:
    def __init__(self, token, proxies=None):
        self._token = token
        self._proxies = proxies

    def query_gql(self, gql):
        import requests
        import json

        endpoint = "https://api.github.com/graphql"
        data = json.dumps({"query": gql})
        headers = {"Authorization": f"Bearer {self._token}"}

        return requests.post(endpoint, data=data, headers=headers, proxies=self._proxies).json()

    def check_path_from_git(self, path) -> bool:
        return path.startswith("https://github.com/")

    def _download_from_git(self, path) -> bytes:
        splited = path.split("/")
        org_name = splited[3]
        repo_name = splited[4]
        ref_name = splited[6]
        target_path = "/".join(splited[7:])

        g = Github(self._token)

        org = g.get_organization(org_name)
        repo = org.get_repo(repo_name)
        content = repo.get_contents(target_path, ref=ref_name)
        file_sha = content.sha
        blob = repo.get_git_blob(file_sha)
        file_data = base64.b64decode(blob.raw_data["content"])

        return file_data

    def download_from_git(self, path) -> bytes:
        if self._proxies:
            with proxy(self._proxies):
                file_data = self._download_from_git(path)
        else:
            file_data = self._download_from_git(path)

        return file_data
