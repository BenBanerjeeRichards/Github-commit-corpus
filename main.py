import github
import db
from typing import Optional
import logging
from multiprocessing import Process, Lock
from sys import stdout
import time
import datetime

log_format = logging.Formatter('%(asctime)s [%(process)d] %(message)s')
logging.basicConfig(format='%(asctime)s [%(process)d] %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p',
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
            sleep_until_rate_reset()
            return
        next_repo_id = next_repo()
        set_progress(downloader_id, next_repo_id, "master")
    else:
        if not allow_request():
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
        repo_id_from_queue = db.get_next_repo_from_queue()
        if not repo_id_from_queue:
            largest_id = db.largest_repo_id()
            logging.info("No repo in progress or in queue, getting listing from id = {}".format(largest_id))
            repos = github.repos(largest_id)
            db.add_repos_to_queue(repos)
            repo_id_from_queue = db.get_next_repo_from_queue()

        logging.info("Got repo id {}, removing from queue and processing".format(repo_id_from_queue))
        repo = github.repo(repo_id_from_queue)
        db.insert_repo(repo)
        db.remove_repo_from_queue(repo_id_from_queue)
        return repo["id"]
    finally:
        NEXT_REPO_LOCK.release()


def allow_step():
    pass


def downloader_main(downloader_id):
    while True:
        try:
            run_step(downloader_id)
        except Exception as e:
            logging.exception("Got exception in downloader {}".format(downloader_id))
            time.sleep(PROC_CRASH_WAIT_SEC)


def get_and_insert_commits(repo_id, sha=None) -> Optional[str]:
    commits = github.commits(repo_id, sha)
    if len(commits) == 0:
        return None

    if sha:
        commits = commits[1:]

    db.insert_commits(repo_id, commits)

    if len(commits) == 0:
        return None

    return commits[-1].get("sha")


def allow_request(num_reqs=1):
    global RATE
    global NUM_REQ_THIS_PERIOD

    try:
        CHECK_RATE_LOCK.acquire()

        if not RATE or RATE["reset"] >= time.time():
            RATE = github.rate_limit()

        if not NUM_REQ_THIS_PERIOD:
            resets_at = datetime.datetime.fromtimestamp(RATE["reset"])
            period_start = datetime.datetime(resets_at.year, resets_at.month, resets_at.day, resets_at.hour - 1,
                                             resets_at.minute, resets_at.second)

            NUM_REQ_THIS_PERIOD = db.num_requests_between(period_start.__str__(), resets_at.__str__())

        if NUM_REQ_THIS_PERIOD + num_reqs > RATE_LIMIT:
            return False
        else:
            NUM_REQ_THIS_PERIOD += num_reqs
            return True
    finally:
        CHECK_RATE_LOCK.release()


def sleep_until_rate_reset():
    rate = github.rate_limit()
    sleep_until(rate["reset"])


def sleep_until(timestamp: int):
    now = time.time()
    while timestamp - now > 0:
        if timestamp - now < 10:
            time.sleep((timestamp - now) + 1)
        else:
            time.sleep(8)

        now = time.time()


def main():
    p1 = Process(target=downloader_main, args=(1,))
    p2 = Process(target=downloader_main, args=(2,))

    p1.start()
    p2.start()

    logging.info("started")
    p1.join()
    p2.join()


if __name__ == '__main__':
    main()
