CREATE KEYSPACE IF NOT EXISTS homework
WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };

CREATE TABLE IF NOT EXISTS homework.user_posts
(
  post_id    int,
  user_id    int,
  username   text,
  topic_id   int,
  content    text,
  created_at timestamp,
  PRIMARY KEY(user_id, created_at)
) WITH CLUSTERING ORDER BY (created_at DESC);

CREATE TABLE IF NOT EXISTS homework.topic_posts
(
  post_id    int,
  user_id    int,
  username   text,
  topic_id   int,
  content    text,
  created_at timestamp,
  PRIMARY KEY(topic_id, created_at)
) WITH CLUSTERING ORDER BY (created_at DESC);

INSERT INTO homework.user_posts
  (post_id, user_id, username, topic_id, content, created_at) 
VALUES
  (1, 1, 'user1', 1, 'post content', '2023-12-15 12:00:00');

INSERT INTO homework.user_posts
  (post_id, user_id, username, topic_id, content, created_at) 
VALUES
  (2, 1, 'user1', 1, 'post content', '2023-12-15 15:00:00');

INSERT INTO homework.user_posts
  (post_id, user_id, username, topic_id, content, created_at) 
VALUES
  (3, 1, 'user1', 1, 'post content', '2023-12-15 15:01:00');

INSERT INTO homework.user_posts
  (post_id, user_id, username, topic_id, content, created_at) 
VALUES
  (4, 1, 'user1', 1, 'post content', '2023-12-18 20:00:00');

INSERT INTO homework.user_posts
  (post_id, user_id, username, topic_id, content, created_at) 
VALUES
  (5, 1, 'user1', 1, 'post content', '2023-12-18 21:00:00');



INSERT INTO homework.topic_posts
  (post_id, user_id, username, topic_id, content, created_at) 
VALUES
  (1, 1, 'user1', 1, 'post content', '2023-12-15 12:00:00');

INSERT INTO homework.topic_posts
  (post_id, user_id, username, topic_id, content, created_at) 
VALUES
  (2, 1, 'user1', 1, 'post content', '2023-12-15 15:00:00');

INSERT INTO homework.topic_posts
  (post_id, user_id, username, topic_id, content, created_at) 
VALUES
  (3, 1, 'user1', 1, 'post content', '2023-12-15 15:01:00');

INSERT INTO homework.topic_posts
  (post_id, user_id, username, topic_id, content, created_at) 
VALUES
  (4, 1, 'user1', 1, 'post content', '2023-12-18 20:00:00');

INSERT INTO homework.topic_posts
  (post_id, user_id, username, topic_id, content, created_at) 
VALUES
  (5, 1, 'user1', 1, 'post content', '2023-12-18 21:00:00');

INSERT INTO homework.topic_posts
  (post_id, user_id, username, topic_id, content, created_at) 
VALUES
  (6, 1, 'user1', 1, 'post content', '2023-12-18 18:00:00');

INSERT INTO homework.topic_posts
  (post_id, user_id, username, topic_id, content, created_at) 
VALUES
  (7, 2, 'user2', 1, 'post content', '2023-12-18 19:00:00');

INSERT INTO homework.topic_posts
  (post_id, user_id, username, topic_id, content, created_at) 
VALUES
  (8, 2, 'user2', 1, 'post content', '2023-12-19 00:00:00');


-- Запрос, который бы доставал посты заданного пользователя упорядоченные
-- по времени создания поста - сначала новые. 
-- Запрос должен возвращать post_id, content, дату создания поста.
SELECT 
  post_id,
  content,
  created_at
FROM
  homework.user_posts
WHERE user_id = 1;


-- Запрос, который бы доставал последний пост заданного пользователя.
-- Запрос должен возвращать post_id, content, дату создания поста, username.
SELECT 
  post_id,
  content,
  created_at,
  username
FROM
  homework.user_posts
WHERE user_id = 1
LIMIT 1;


-- Запрос, который бы доставал посты заданного пользователя,
-- опубликованные после заданной даты. Запрос должен возвращать
-- post_id, content, дату создания поста.
SELECT 
  post_id,
  content,
  created_at
FROM
  homework.user_posts
WHERE user_id = 1 
  AND created_at > '2023-12-17';



-- Запрос, который бы доставал пользователей, которые 
-- сделали пост в заданный день по заданной теме, 
-- упорядоченные по времени - сначала новые. 
-- Запрос должен возвращать user_id, username, post_id, время создания поста.
SELECT 
  user_id,
  username,
  post_id,
  created_at
FROM 
  homework.topic_posts
WHERE topic_id = 1
  AND created_at > '2023-12-18'
  AND created_at < '2023-12-19';
