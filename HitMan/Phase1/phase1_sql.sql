DROP TABLE IF EXISTS `q2tweets`;

CREATE TABLE q2tweets (
uidtime VARCHAR(40) NOT NULL,
tweetid BIGINT,
score INT,
content text CHARACTER SET utf8 COLLATE utf8_unicode_ci,
);

LOAD DATA INFILE '/home/ubuntu/data' INTO TABLE `q2tweets`
CHARACTER SET UTF8
FIELDS TERMINATED BY ','  LINES TERMINATED BY '\n';


CREATE TABLE 'q2tweets' (
useridtime VARCHAR(40) NOT NULL,
tweetid BIGINT,
score INT,
content text CHARACTER SET utf8 COLLATE utf8_unicode_ci
);