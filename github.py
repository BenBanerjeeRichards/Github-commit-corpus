import requests
import db
import time
import datetime
import urllib.parse
import math
from requests.exceptions import HTTPError

TOKEN = open("creds.txt", "rt").read().replace("\n", "")
MAX_ATTEMPTS = 5
BASE_SLEEP = 2
SLEEP_POW = 2


class GithubApiResponse:

    def __init__(self, data: dict, error: bool, response: requests.Response, request_log_id: int):
        self.data = data
        self.response = response
        self.request_log_id = request_log_id
        self.error = error

    def rate_limit_hit(self) -> bool:
        if not self.error:
            return False

        if not self.response.status_code == 403:
            return False

        return "API rate limit exceeded" in self.data.get("message")


def repos(since=None) -> GithubApiResponse:
    params = {"since": since} if since else {}
    return authenticated_get("https://api.github.com/repositories", params)


def repo(repo_id) -> GithubApiResponse:
    return authenticated_get("https://api.github.com/repositories/{}".format(repo_id))


def commits(repo_id, sha: str = "master") -> GithubApiResponse:
    params = {"sha": sha, "per_page": 100}
    return authenticated_get(
        "https://api.github.com/repositories/{}/commits".format(repo_id), params)


def rate_limit() -> GithubApiResponse:
    return authenticated_get("https://api.github.com/rate_limit")


def authenticated_get(url, params={}) -> GithubApiResponse:
    def do_authenticated_request():
        return requests.get(url, params, headers={"Authorization": "token {}".format(TOKEN)})

    attempts = 0

    while True:
        date = datetime.datetime.utcnow()
        start = int(round(time.time() * 1000.0))
        res = do_authenticated_request()
        end = int(round(time.time() * 1000.0))
        attempts += 1

        full_url = url
        if params:
            full_url = full_url + "?" + urllib.parse.urlencode(params)
        error_body = None
        if res.status_code > 299:
            error_body = res.text

        request_log_id = db.insert_request_log(full_url, date.__str__(), end - start, res.status_code, error_body)

        if 200 <= res.status_code < 400:
            return GithubApiResponse(res.json(), False, res, request_log_id)

        if res.status_code == 403 and "rate" in res.json().get("message"):
            # Don't retry, rate limit hit
            return GithubApiResponse(res.json(), True, res, request_log_id)

        if attempts >= MAX_ATTEMPTS:
            return GithubApiResponse(res.json(), True, res, request_log_id)

        #     Retry requests
        sleep_time = math.pow(BASE_SLEEP, SLEEP_POW)
        time.sleep(sleep_time)
