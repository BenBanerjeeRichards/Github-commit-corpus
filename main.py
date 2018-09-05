import github
import db
from typing import Optional
import logging
from multiprocessing import Process, Lock
from sys import stdout
import time
import datetime
import sys

log_format = logging.Formatter('%(asctime)s [%(process)d] %(message)s')
logging.basicConfig(format='%(asctime)s [%(process)d] %(message)s',
                    filename="output.log")
sh = logging.StreamHandler(stdout)
sh.setFormatter(log_format)
logging.getLogger().addHandler(sh)
logging.getLogger().setLevel(logging.DEBUG)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

PROGRESS_FILE = "progress.txt"
NEXT_REPO_LOCK = Lock()
CHECK_RATE_LOCK = Lock()
RATE = None
NUM_REQ_THIS_PERIOD = None
# GH rate is 5000 / hour, add a little leeway
RATE_LIMIT = 4998
PROC_CRASH_WAIT_SEC = 10
MAX_GET_REPO_ID = 50


def set_progress(downloader_id, repo_id, sha):
    db.set_progress(downloader_id, repo_id, sha)


def get_progress(downloader_id) -> (str, str):
    return db.get_progress(downloader_id)


def run_step(downloader_id):
    repo_id, sha = get_progress(downloader_id)
    logging.debug("[RUN STEP] (repo_id, sha) = ({}, {})".format(repo_id, sha))
    if repo_id is None and sha is None:
        # This one takes 2 requests
        if not allow_request(2):
            logging.info("No rate limit remaining, sleeping . Rate = {}".format(RATE.__str__()))
            sleep_until_rate_reset()
            return
        next_repo_id = next_repo()
        if next_repo_id:
            set_progress(downloader_id, next_repo_id, "master")
    else:
        if not allow_request():
            logging.info("No rate limit remaining, sleeping . Rate = {}".format(RATE.__str__()))
            sleep_until_rate_reset()
            return
        logging.info("Downloading commits since {}".format(sha))
        new_sha = get_and_insert_commits(repo_id, sha)
        if new_sha is None:
            logging.info("Reached end of commits for repo, clearing progress")
            set_progress(downloader_id, None, None)
        else:
            set_progress(downloader_id, repo_id, new_sha)


def next_repo():
    try:
        NEXT_REPO_LOCK.acquire()
        return _do_next_repo()
    finally:
        NEXT_REPO_LOCK.release()


def _do_next_repo(attempts=0) -> int:
    if attempts > MAX_GET_REPO_ID:
        raise GithubError("Exceeded max num of attempts to find repo without error, stopping")

    repo_id_from_queue = db.get_next_repo_from_queue()
    if not repo_id_from_queue:
        largest_id = db.largest_repo_id()
        logging.info("No repo in progress or in queue, getting listing from id = {}".format(largest_id))
        repos = github.repos(largest_id)
        db.add_repos_to_queue(repos.data)
        repo_id_from_queue = db.get_next_repo_from_queue()

    logging.info("Got repo id {}, removing from queue and processing".format(repo_id_from_queue))

    repo = github.repo(repo_id_from_queue)
    if repo.rate_limit_hit():
        sleep_until_rate_reset()
        return
    if repo.error:
        logging.exception("Failed to get repo due to API error")
        logging.info("Repo {} has access blocked, skipping".format(repo_id_from_queue))
        db.remove_repo_from_queue(repo_id_from_queue)
        db.add_failed_repo(repo_id_from_queue, repo.request_log_id)
        return _do_next_repo(attempts + 1)

    db.insert_repo(repo.data)
    db.remove_repo_from_queue(repo_id_from_queue)
    return repo.data["id"]


def downloader_main(downloader_id):
    while True:
        try:
            run_step(downloader_id)
        except Exception as e:
            logging.exception("Got exception in downloader {}".format(downloader_id))
            time.sleep(PROC_CRASH_WAIT_SEC)


def get_and_insert_commits(repo_id, sha=None) -> Optional[str]:
    commits_res = github.commits(repo_id, sha)
    if commits_res.error:
        if commits_res.response.status_code == 409 and commits_res.data.get("message") == "Git Repository is empty.":
            return None
        elif commits_res.rate_limit_hit():
            sleep_until_rate_reset()
            return None
        elif commits_res.response.status_code == 404 and sha == "master":
            # No master branch, just use whatever they have
            logging.info("Got 404 with master on repo {}, trying on whatever the branch happens to be".format(repo_id))
            return get_and_insert_commits(repo_id)
        else:
            db.add_failed_commits(repo_id, sha, commits_res.request_log_id)
    commits = commits_res.data

    if len(commits) == 0:
        return None

    if sha:
        commits = commits[1:]

    db.insert_commits(repo_id, commits)

    if len(commits) == 0:
        return None

    return commits[-1].get("sha")


def allow_request(num_reqs=1, check = False):
    global RATE
    global NUM_REQ_THIS_PERIOD

    try:
        CHECK_RATE_LOCK.acquire()

        if not RATE or RATE["reset"] <= time.time():
            RATE = github.rate_limit().data["resources"]["core"]

        if not NUM_REQ_THIS_PERIOD:
            resets_at = datetime.datetime.fromtimestamp(RATE["reset"])
            period_start = datetime.datetime(resets_at.year, resets_at.month, resets_at.day, resets_at.hour - 1,
                                             resets_at.minute, resets_at.second)

            NUM_REQ_THIS_PERIOD = db.num_requests_between(period_start.__str__(), resets_at.__str__())

        if NUM_REQ_THIS_PERIOD + num_reqs > RATE_LIMIT:
            if not check:
                RATE = github.rate_limit().data["resources"]["core"]
                return allow_request(num_reqs, True)
        else:
            NUM_REQ_THIS_PERIOD += num_reqs
            return True
    finally:
        CHECK_RATE_LOCK.release()


def sleep_until_rate_reset():
    rate = github.rate_limit().data["resources"]["core"]
    if rate["remaining"] < 10:
        logging.info("Sleeping until next rate limit at {}, rate = {}".format(rate["reset"], rate))
        sleep_until(rate["reset"])
    else:
        logging.error("Tried to sleep for rate limit but remaining is too high: rate = {}".format(rate))


def sleep_until(timestamp: int):
    now = time.time()
    logging.info("Sleeping until {} now = {} diff = {}".format(timestamp, now, timestamp - now))
    iterations = 0

    while timestamp - now > 0:
        iterations += 1
        if timestamp - now < 10:
            sleep_for = (timestamp - now) + 1
        else:
            sleep_for = 8

        logging.info(
            "Sleeping for {} seconds, now = {}, timestamp = {}, diff = {}".format(sleep_for, now, timestamp, timestamp - now))

        time.sleep(sleep_for)
        now = time.time()


def main():
    if len(sys.argv) != 2:
        print("syntax: main.py [DB PATH]")
        return
    db_path = sys.argv[1]
    db.DB_PATH = db_path

    p1 = Process(target=downloader_main, args=(1,))
    p2 = Process(target=downloader_main, args=(2,))

    p1.start()
    p2.start()

    logging.info("started")
    p1.join()
    p2.join()


if __name__ == '__main__':
    main()


class GithubError(Exception):

    def __init__(self, message):
        self.message = message
        pass
