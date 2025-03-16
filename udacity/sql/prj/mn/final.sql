create table if not exists users
(
    user_id    serial                              not null
        constraint users_pk primary key,
    username   varchar(25) unique                  not null,
    last_login timestamp default current_timestamp not null,
    constraint check_username
        check (length(trim(users.username)) > 0)
);


create table if not exists topics
(
    topic_id          serial      not null
        constraint topics_pk primary key,
    topic_name        varchar(30) not null
        constraint check_topic_name
            check (length(trim(topics.topic_name)) > 0),
    topic_description varchar(500)
);

create table if not exists posts
(
    post_id  serial       not null
        constraint posts_pk
            primary key,
    user_id  integer
        constraint posts_users_user_id_fk
            references users
            on delete set null,
    topic_id integer      not null
        constraint posts_topics_topic_id_fk
            references topics
            on delete cascade,
    title    varchar(100) not null,
    content  text,
    url      text,
    created_timestamp timestamp default current_timestamp not null,
    constraint check_content
        check ((url is not null and content is null) or (content is not null and url is null)),
    constraint check_title
        check (length(trim(title)) > 0)
);

create table if not exists comments
(
    comment_id        serial                              not null
        constraint comments_pk
            primary key,
    content           text                                not null,
    parent_id         integer
        constraint comments_comments_comment_id_fk
            references comments
            on delete cascade,
    post_id           integer                             not null
        constraint comments_posts_post_id_fk
            references posts
            on delete cascade,
    user_id           integer
        constraint comments_users_user_id_fk
            references users
            on delete set null,
    created_timestamp timestamp default current_timestamp not null
);

create table if not exists votes
(
    vote_id serial
        constraint votes_pk
            primary key,
    vote    smallint not null
        constraint check_name
            check ((vote = 1) OR (vote = '-1'::integer)),
    user_id integer
        constraint votes_users_user_id_fk
            references users,
    post_id integer  not null
        constraint votes_posts_post_id_fk
            references posts,
    constraint votes_unique
        unique (user_id, post_id)
);

insert into users(username)
select username
from bad_comments
union
select username
from bad_posts
union
select regexp_split_to_table(upvotes, ',') as username
from bad_posts
union
select regexp_split_to_table(downvotes, ',') as username
from bad_posts;

insert into topics(topic_name)
select distinct topic
from bad_posts;

insert into posts (title, url, content, topic_id, user_id)
select left(bp.title, 100), bp.url, bp.text_content, t.topic_id, u.user_id
from bad_posts as bp
         join topics as t on bp.topic = t.topic_name
         join users as u on bp.username = u.username;

insert into comments(content, post_id, user_id)
select bc.text_content, p.post_id, u.user_id
from bad_comments as bc
         join bad_posts as bp on bc.post_id = bp.id
         join posts as p on p.title = left(bp.title, 100)
         left join users as u on bc.username = u.username;



insert into votes (vote, user_id, post_id)
select vote, u.user_id, p.post_id
from (select distinct title as title, regexp_split_to_table(upvotes, ',') as username, 1 as vote
      from bad_posts) as bp
         join posts as p on left(bp.title, 100) = p.title
         join users as u on u.username = bp.username;

insert into votes (vote, user_id, post_id)
select vote, u.user_id, p.post_id
from (select title as title, regexp_split_to_table(downvotes, ',') as username, -1 as vote
      from bad_posts) as bp
         join posts as p on left(bp.title, 100) = p.title
         join users as u on u.username = bp.username;



select count(*)
from users;
select count(*)
from topics;
select count(*)
from posts;
select count(*)
from comments;
select count(*)
from votes;

-- i.	List all the top-level comments (those that don’t have a parent comment) for a given post.
SELECT * FROM comments WHERE post_id = 26890 AND comments.parent_id IS NULL;


--Compute the score of a post, defined as the difference between the number of upvotes and the number of downvotes
SELECT post_id, SUM(CASE WHEN vote = 1 THEN 1 ELSE -1 END) AS score
FROM votes WHERE post_id = 26890 GROUP BY post_id;

