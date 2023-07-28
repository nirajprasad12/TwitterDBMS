
CREATE TABLE TwitterUser (
  twitter_handle VARCHAR(255) PRIMARY KEY,
  username VARCHAR(255),
  profile_image_url VARCHAR(MAX),
  joined_on DATE,
  description VARCHAR(MAX),
  follower_count INT,
  following_count INT
);

CREATE TABLE TwitterHeader (
  tweet_id VARCHAR(255) PRIMARY KEY,
  twitter_handle VARCHAR(255) FOREIGN KEY REFERENCES TwitterUser(twitter_handle),
  parent_tweet_id VARCHAR(255),
  tweet_text VARCHAR(MAX),
  created_at DATE,
  location VARCHAR(255)
);

CREATE TABLE ReferencedTweetTable (
    parent_tweet_id varchar(255),
    twitter_handle varchar(255) 
);
