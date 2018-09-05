drop table if exists repo;
drop table if exists person;
drop table if exists 'commit';
drop table if exists repo_queue;
drop table if exists request_log;
drop table if exists progress;

create table if not exists person (
  id integer primary key autoincrement,
  name  text,
  email text
);

create table if not exists 'commit' (
  id integer primary key autoincrement,
  repo bigint,
  sha text,
  message text,
  author integer,
  committer integer,
  author_datetime datetime,
  committer_datetime datetime,
foreign key (committer) references person(id),
foreign key (author) references person(id)
foreign key (repo) references repo(id)
);

create table if not exists repo (
  id integer primary key,
  name text,
  owner_id integer,
  description text,
  created_at datetime,
  updated_at datetime,
  pushed_at datetime,
  size integer,
  stargazers_count integer,
  watchers_count integer,
  language text,
  forks_count integer,
  archived integer,
  license text,
  network_count integer,
  subscribers_count integer
);

create table if not exists repo_queue (
  repo_id int primary key
);

create table if not exists request_log (
  id integer primary key autoincrement,
  url text,
  request_started datetime,
  time_ms int,
  status_code int,
  error_body text
);


create table progress (
  downloader_id integer primary key,
  repo_id text,
  sha text
);

create table if not exists failed_get_repo(
  id integer primary key autoincrement,
  repo_id integer,
  request_log_id integer,
  foreign key (request_log_id) references request_log (id)
);

create table if not exists failed_get_commits (
  id integer primary key autoincrement,
  repo_id integer,
  sha text,
  request_log_id integer,
  foreign key (repo_id) references repo(id),
  foreign key (request_log_id) references request_log(id)
);