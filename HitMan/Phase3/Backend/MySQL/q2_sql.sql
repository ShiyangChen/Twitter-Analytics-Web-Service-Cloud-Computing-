DROP TABLE IF EXISTS `q2tweets`;

CREATE TABLE q2tweets (
`datenuserid` VARCHAR(40) NOT NULL,
`tweets` TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL
);

LOAD DATA LOCAL INFILE 'all3' INTO TABLE `q2tweets`
CHARACTER SET UTF8mb4
FIELDS TERMINATED BY ' ass '  LINES TERMINATED BY ' shit \t\n';

CREATE INDEX iddateindex ON q2tweets (datenuserid);