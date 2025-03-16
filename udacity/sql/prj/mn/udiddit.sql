CREATE TABLE bad_posts (
	id SERIAL PRIMARY KEY,
	topic VARCHAR(50),
	username VARCHAR(50),
	title VARCHAR(150),
	url VARCHAR(4000) DEFAULT NULL,
	text_content TEXT DEFAULT NULL,
	upvotes TEXT,
	downvotes TEXT
);

CREATE TABLE bad_comments (
	id SERIAL PRIMARY KEY,
	username VARCHAR(50),
	post_id BIGINT,
	text_content TEXT
);

-- Part I: Investigate the existing schema
-- As a first step, investigate this schema and some of the sample data in the project’s SQL workspace. 

\d bad_posts
\dt bad_posts


\d bad_comments
\dt bad_comments

-- Then, in your own words, outline three (3) specific things that could be improved about this schema. Don’t hesitate to outline more if you want to stand out!

-- Normalization:
-- Issue: The `upvotes` and `downvotes` columns in the bad_posts table are of type TEXT. It might be more appropriate to use numeric types (e.g., INTEGER) to represent the count of upvotes and downvotes.
-- Improvement: `upvotes` and `downvotes` changed to numeric data type (e.g. INTEGER)

-- Data Integrity:
-- Issue: The `post_id` column in the `bad_comments` table refers to the `id` column in the bad_posts table. No FOREIGN KEY constraint.
-- Improvement: Add a foreign key constraint on the `post_id` column in the `bad_comments` table, referencing the id column in the `bad_posts` table.

-- Length Constraint:
-- Issue: The `url` column in the `bad_posts` table has a maximum length of 4000 characters. This is excessive.
-- Improvement: Reduce maximum length to e.g. 200.

-- Guideline #1: Part II: Create the DDL for your new schema
    -- 1.	Guideline #1: here is a list of features and specifications that Udiddit needs in order to support its website and administrative interface:
        -- a.	Allow new users to register:
            -- i.	Each username has to be unique
            -- ii.	Usernames can be composed of at most 25 characters
            -- iii.	Usernames can’t be empty
            -- iv.	We won’t worry about user passwords for this project
            ??
        -- b.	Allow registered users to create new topics:
            -- i.	Topic names have to be unique.
            -- ii.	The topic’s name is at most 30 characters
            -- iii.	The topic’s name can’t be empty
            -- iv.	Topics can have an optional description of at most 500 characters.
        -- c.	Allow registered users to create new posts on existing topics:
            -- i.	Posts have a required title of at most 100 characters
            -- ii.	The title of a post can’t be empty.
            -- iii.	Posts should contain either a URL or a text content, but not both.
            -- iv.	If a topic gets deleted, all the posts associated with it should be automatically deleted too.
            -- v.	If the user who created the post gets deleted, then the post will remain, but it will become dissociated from that user.
        -- d.	Allow registered users to comment on existing posts:
            -- i.	A comment’s text content can’t be empty.
            -- ii.	Contrary to the current linear comments, the new structure should allow comment threads at arbitrary levels.
            -- iii.	If a post gets deleted, all comments associated with it should be automatically deleted too.
            -- iv.	If the user who created the comment gets deleted, then the comment will remain, but it will become dissociated from that user.
            -- v.	If a comment gets deleted, then all its descendants in the thread structure should be automatically deleted too.
    	-- e.	Make sure that a given user can only vote once on a given post:
    	    -- i.	Hint: you can store the (up/down) value of the vote as the values 1 and -1 respectively.
    	    -- ii.	If the user who cast a vote gets deleted, then all their votes will remain, but will become dissociated from the user.
    	    -- iii.	If a post gets deleted, then all the votes for that post should be automatically deleted too.

-- Guideline #2
    -- a.	List all users who haven’t logged in in the last year.
    -- b.	List all users who haven’t created any post.
    -- c.	Find a user by their username.
    -- d.	List all topics that don’t have any posts.
    -- e.	Find a topic by its name.
    -- f.	List the latest 20 posts for a given topic.
    -- g.	List the latest 20 posts made by a given user.
    -- h.	Find all posts that link to a specific URL, for moderation purposes. 
    -- i.	List all the top-level comments (those that don’t have a parent comment) for a given post.
    -- j.	List all the direct children of a parent comment.
    -- k.	List the latest 20 comments made by a given user.
    -- l.	Compute the score of a post, defined as the difference between the number of upvotes and the number of downvotes

-- 3.	Guideline #3: you’ll need to use normalization, various constraints, as well as indexes in your new database schema. You should use named constraints and indexes to make your schema cleaner.

-- 4.	Guideline #4: your new database schema will be composed of five (5) tables that should have an auto-incrementing id as their primary key.


-- in DDL

-- Users TABLE
CREATE TABLE users (
    user_id SERIAL PRIMARY KEY,
    username VARCHAR(25) UNIQUE NOT NULL,  -- a.i.Adding UNIQUE constraint, a.ii. Usernames can be composed of at most 25 characters, a.iii.Usernames can’t be empty
    last_login TIMESTAMP
);

--Topics table
CREATE TABLE topics (
    topic_id SERIAL PRIMARY KEY,
    name VARCHAR(30) UNIQUE NOT NULL, -- b.i. Topic names have to be unique, b.ii. The topic’s name is at most 30 characters, b.iii The topic’s name can’t be empty
    topic_description VARCHAR(500) -- b.iv. Topics can have an optional description of at most 500 characters.)
);

--Posts table
CREATE TABLE posts (
    created_at TIMESTAMP,
    post_id SERIAL PRIMARY KEY,
    title VARCHAR(100) NOT NULL, -- c.i Posts have a required title of at most 100 characters c.ii.	The title of a post can’t be empty.
    text_content TEXT,
    user_id INTEGER REFERENCES users(user_id), --c.v If the user who created the post gets deleted, then the post will remain, but it will become dissociated from that user.
    topic_id INTEGER REFERENCES topics(topic_id) ON DELETE CASCADE, --c.iv If a topic gets deleted, all the posts associated with it should be automatically deleted too.
    url VARCHAR(400),
    CONSTRAINT "url_or_text" CHECK(
        (LENGTH(TRIM("url") > 0 AND LENGTH(TRIM("text_content")) = 0 )) OR -- c.iii. Posts should contain either a URL or a text content
        (LENGTH(TRIM("url") = 0 AND LENGTH(TRIM("text_content")) > 0 ))
    )
)

-- Comments table
CREATE TABLE comments (
    comment_id SERIAL PRIMARY KEY,
    text_content TEXT NOT NULL, -- d.i A comment’s text content can’t be empty.
    user_id INTEGER REFERENCES users(user_id), -- d. iv.If the user who created the comment gets deleted, then the comment will remain, but it will become dissociated from that user.
    post_id INTEGER REFERENCES posts(post_id) ON DELETE CASCADE, --d .iii.	If a post gets deleted, all comments associated with it should be automatically deleted too.
    parent_comment_id INTEGER REFERENCES comments(comment_id) ON DELETE CASCADE -- d.v  If a comment gets deleted, then all its descendants in the thread structure should be automatically deleted too.
);

-- Votes table
CREATE TABLE votes (
    vote_id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(user_id), -- e.ii If the user who cast a vote gets deleted, then all their votes will remain, but will become dissociated from the user.
    post_id INTEGER REFERENCES posts(post_id) ON DELETE CASCADE, -- e.iii.	If a post gets deleted, then all the votes for that post should be automatically deleted too.
    vote_value INTEGER CHECK (vote_value = 1 OR vote_value = -1) -- e.i Hint: you can store the (up/down) value of the vote as the values 1 and -1 respectively.
);
        
-- a.	List all users who haven’t logged in in the last year.
SELECT * FROM users WHERE last_login IS NULL OR last_login < NOW() - INTERVAL '1 year';

-- b.	List all users who haven’t created any post.
SELECT * FROM users WHERE user_id NOT IN (SELECT DISTINCT user_id FROM posts);

-- c.	Find a user by their username.
SELECT * FROM users WHERE username = '<desired_username>';

-- d. List all topics that don't have any posts:
SELECT * FROM topics WHERE topic_id NOT IN (SELECT DISTINCT topic_id FROM posts);

--e. Find a topic by its name:
SELECT * FROM topics WHERE name = '<desired_topic_name>';

--f. List the latest 20 posts for a given topic:
SELECT * FROM posts WHERE topic_id = desired_topic_id ORDER BY created_at DESC LIMIT 20;

-- g.	List the latest 20 posts made by a given user.
SELECT * FROM posts WHERE user_id = desired_user_id ORDER BY created_at DESC LIMIT 20;

-- h.	Find all posts that link to a specific URL, for moderation purposes. 
SELECT * FROM posts WHERE url = 'desired_url';

-- i.	List all the top-level comments (those that don’t have a parent comment) for a given post.
SELECT * FROM comments WHERE post_id = desired_post_id AND parent_comment_id IS NULL;

-- j.	List all the direct children of a parent comment.
SELECT * FROM comments WHERE parent_comment_id = desired_parent_comment_id;

-- k.	List the latest 20 comments made by a given user.
SELECT * FROM comments WHERE user_id = desired_user_id ORDER BY created_at DESC LIMIT 20;

-- l.	Compute the score of a post, defined as the difference between the number of upvotes and the number of downvotes
SELECT post_id, SUM(CASE WHEN vote_value = 1 THEN 1 ELSE -1 END) AS score
FROM votes WHERE post_id = desired_post_id GROUP BY post_id;


-- Part III: Migrate the provided data
-- Now that your new schema is created, it’s time to migrate the data from the provided schema in the project’s SQL Workspace to your own schema. 
-- This will allow you to review some DML and DQL concepts, as you’ll be using INSERT...SELECT queries to do so. Here are a few guidelines to help you in this process:
-- 1.	Topic descriptions can all be empty
-- 2.	Since the bad_comments table doesn’t have the threading feature, you can migrate all comments as top-level comments, i.e. without a parent
-- 3.	You can use the Postgres string function regexp_split_to_table to unwind the comma-separated votes values into separate rows
-- 4.	Don’t forget that some users only vote or comment, and haven’t created any posts. You’ll have to create those users too.
-- 5.	The order of your migrations matter! For example, since posts depend on users and topics, you’ll have to migrate the latter first.
-- 6.	Tip: You can start by running only SELECTs to fine-tune your queries, and use a LIMIT to avoid large data sets. Once you know you have the correct query, you can then run your full INSERT...SELECT query.
-- 7.	NOTE: The data in your SQL Workspace contains thousands of posts and comments. The DML queries may take at least 10-15 seconds to run.
-- 
-- Write the DML to migrate the current data in bad_posts and bad_comments to your new database schema:

-- Create Users
INSERT INTO users (username)
SELECT DISTINCT username
FROM bad_posts
UNION
SELECT DISTINCT username
FROM bad_comments;

-- Create Topics
INSERT INTO topics (name)
SELECT DISTINCT topic
FROM bad_posts;

-- Create posts
INSERT INTO posts (title, text_content, user_id, topic_id, url, created_at)
SELECT
    title,
    text_content,
    u.user_id,
    t.topic_id,
    url,
    current_timestamp  
FROM bad_posts bp
JOIN users u ON bp.username = u.username
JOIN topics t ON bp.topic = t.name;

-- Create comments
INSERT INTO comments (text_content, user_id, post_id)
SELECT
    text_content,
    u.user_id,
    p.post_id
FROM bad_comments bc
JOIN users u ON bc.username = u.username
LEFT JOIN posts p ON bc.post_id = p.post_id; 

-- Create votes
INSERT INTO votes (user_id, post_id, vote_value)
SELECT
    u.user_id,
    p.post_id,
    CAST(vote_value AS INTEGER)
FROM bad_posts bp
JOIN users u ON bp.username = u.username
JOIN regexp_split_to_table(bp.upvotes, ',') v ON TRUE
JOIN posts p ON bp.id = p.post_id;

