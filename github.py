import requests
import db
import time
import datetime
import urllib.parse
import math

TOKEN = open("creds.txt").read()
MAX_ATTEMPTS = 5
BASE_SLEEP = 2
SLEEP_POW = 2


def repos(since=None):
    params = {"since": since} if since else {}
    return authenticated_get("https://api.github.com/repositories", params)


def repo(repo_id):
    return authenticated_get("https://api.github.com/repositories/{}".format(repo_id))


def commits(repo_id, sha: str = "master"):
    params = {"sha": sha, "per_page": 100}

    return authenticated_get(
        "https://api.github.com/repositories/{}/commits".format(repo_id), params)


def rate_limit():
    return authenticated_get("https://api.github.com/rate_limit")["resources"]["core"]


def authenticated_get(url, params={}):
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
            full_url = full_url + "/" + urllib.parse.urlencode(params)
        db.insert_request_log(full_url, date.__str__(), end - start, res.status_code)

        if 200 <= res.status_code < 400:
            return res.json()

        if res.status_code == 403 and "rate" in res.json().get("message"):
            # Don't retry, rate limit hit
            res.raise_for_status()
            return

        if res.status_code == 404:
            res.raise_for_status()
            return

        if attempts >= MAX_ATTEMPTS:
            res.raise_for_status()

        #     Retry requests
        sleep_time = math.pow(BASE_SLEEP, SLEEP_POW)
        time.sleep(sleep_time)
