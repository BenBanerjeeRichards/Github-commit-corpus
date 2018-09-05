import sqlite3
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = None


def get_db():
    if not DB_PATH:
        raise Exception("No DB path provided")

    conn = sqlite3.connect(DB_PATH)
    return conn, conn.cursor()


def insert_repo(repo: dict):
    query = """
    insert into repo values (?, ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """

    conn, cursor = get_db()
    owner = repo.get("owner")
    owner_id = None if not owner else owner.get("id")
    repo_license = None
    if repo.get("license") and "key" in repo["license"]:
        repo_license = repo["license"]["key"]

    data = (repo["id"], repo.get("name"),
            owner_id,
            repo.get("description"),
            repo.get("created_at"),
            repo.get("updated_at"),
            repo.get("pushed_at"),
            repo.get("size"),
            repo.get("stargazers_count"),
            repo.get("watchers_count"),
            repo.get("language"),
            repo.get("forks_count"),
            1 if repo.get("archived") == "true" else 0,
            repo_license,
            repo.get("network_count"),
            repo.get("subscribers_count"),)

    with conn:
        cursor.execute(query, data)


def insert_person_if_not_exists(person, cache=None) -> int:
    data = (person.get("name"), person.get("email"))
    if cache and data in cache:
        return cache[data]

    conn, cursor = get_db()
    with conn:
        cursor.execute("select * from person where name = ? and email = ?", data)
        existing = cursor.fetchone()
        if not existing:
            cursor.execute("insert into person values (NULL, ?, ?)", data)
            cache[data] = cursor.lastrowid
            return cursor.lastrowid

        p_id = existing[0]
        cache[data] = p_id
        return p_id


def get_committer_and_author_ids(commit, cache=None) -> (int, int):
    committer = commit["committer"]
    author = commit["author"]
    if author["name"] == committer["name"] and author["email"] == committer["email"]:
        p_id = insert_person_if_not_exists(committer, cache)
        return p_id, p_id

    return insert_person_if_not_exists(committer, cache), insert_person_if_not_exists(author, cache)


def insert_commits(repo_id, commits: [dict]):
    cache = {}
    data = []
    for commit in commits:
        committer_id, author_id = get_committer_and_author_ids(commit["commit"], cache)
        data.append((
            repo_id,
            commit.get("sha"),
            commit["commit"].get("message"),
            committer_id,
            author_id,
            commit["commit"]["author"]["date"],
            commit["commit"]["committer"]["date"],
        ))

    conn, cursor = get_db()
    with conn:
        cursor.executemany("insert into \"commit\" values (NULL, ?, ?, ?, ?, ?, ?, ?)", data)


def add_repos_to_queue(repos: [dict]):
    data = []
    for repo in repos:
        data.append((repo["id"],))

    conn, cursor = get_db()
    with conn:
        cursor.executemany("insert into repo_queue values (?)", data)


def remove_repo_from_queue(repo_id: int):
    conn, cursor = get_db()
    with conn:
        cursor.execute("delete from repo_queue where repo_id = ?", (repo_id,))


def get_next_repo_from_queue():
    conn, cursor = get_db()
    with conn:
        cursor.execute("select * from repo_queue")
        repo = cursor.fetchone()
        if not repo:
            return None
        else:
            return repo[0]


def largest_repo_id() -> int:
    conn, cursor = get_db()
    with conn:
        cursor.execute("select max(id) from repo")
        res = cursor.fetchone()
        return res[0]


def insert_request_log(url: str, date: str, time: int, status: int, error_body:str=None) -> int:
    conn, cursor = get_db()
    with conn:
        data = (url, date, time, status, error_body)
        cursor.execute("insert into  request_log values (NULL, ?, ?, ?, ?, ?)", data)
        return cursor.lastrowid


def get_progress(downloader_id) -> (str, str):
    conn, cursor = get_db()
    with conn:
        cursor.execute("select * from progress where downloader_id = ?", (downloader_id,))
        res = cursor.fetchone()
        if not res:
            return None, None
        else:
            return res[1], res[2]


def set_progress(downloader_id, repo_id, sha):
    conn, cursor = get_db()
    with conn:
        cursor.execute("delete from progress where downloader_id = ?", (downloader_id,))
        cursor.execute("insert into progress values (?, ?, ?)", (downloader_id, repo_id, sha))


def num_requests_between(start: str, end: str) -> int:
    query = """
    SELECT count(*)
    FROM request_log
    WHERE
      CAST(strftime('%s', request_started) AS integer) <= CAST(strftime('%s', '{}') AS integer) and
      CAST(strftime('%s', request_started) AS integer) >= CAST(strftime('%s', '{}') AS integer)
    """.format(end, start)

    conn, cursor = get_db()
    with conn:
        cursor.execute(query)
        return cursor.fetchone()[0]


def add_failed_repo(repo_id, request_log_id):
    conn, cursor = get_db()
    with conn:
        cursor.execute("insert into failed_get_repo values (NULL, ?, ?)", (repo_id, request_log_id))


def add_failed_commits(repo_id, sha, request_log_id):
    conn, cursor = get_db()
    with conn:
        cursor.execute("insert into failed_get_commits values (NULL, ?, ?, ?)", (repo_id, sha, request_log_id))

