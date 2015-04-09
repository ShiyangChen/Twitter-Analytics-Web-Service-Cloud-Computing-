
DROP TABLE IF EXISTS `q4tweets`;
CREATE TABLE q4tweets (
`dateLocation` VARCHAR(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
`rank` INT NOT NULL,
`tagnID` TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL
);

LOAD DATA LOCAL INFILE 'q4all' INTO TABLE `q4tweets`
CHARACTER SET UTF8mb4
LINES TERMINATED BY ' shit \n';

CREATE INDEX index_name1 ON q4tweets (dataLocation, rank);