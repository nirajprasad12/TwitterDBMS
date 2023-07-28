select * from twitterheaderarchive;

select * from sys.tables where name like '%archive%';

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

create   procedure [dbo].[twitterSp] as

if exists(select 1 from sys.tables where name = 'twitterHeader')
   begin
   exec ('insert into twitterHeaderArchive select * from twitterHeader');
   end
exec ('delete from twitterHeader');

if exists(select 1 from sys.tables where name = 'tweet_url')
   begin
   exec ('insert into tweetUrlArchive select * from tweet_url');
   end
exec ('delete from tweet_url');

if exists(select 1 from sys.tables where name = 'twitter_user')
   begin
   exec ('insert into twitterUserArchive select * from twitter_user');
   end
exec ('delete from twitter_user');

if exists(select 1 from sys.tables where name = 'edge_df')
   begin
   exec ('insert into edgeArchive select * from edge_df');
   end
exec ('delete from edge_df');

if exists(select 1 from sys.tables where name = 'nodes')
   begin
   exec ('insert into nodeArchive select * from nodes');
   end
exec ('delete from nodes');

if exists(select 1 from sys.tables where name = 'referenced_tweet_table')
   begin
   exec ('insert into referencedTweetArchive select * from referenced_tweet_table');
   end
exec ('delete from referenced_tweet_table');
GO


select count(tweet_id) as cnt_records from tweetUrlArchive;