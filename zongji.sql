
CREATE TABLE `util`.`binlogQueue`(
	`code` VARCHAR(255) NOT NULL,
	`logName` VARCHAR(255) NOT NULL,
	`position` BIGINT UNSIGNED NOT NULL,
	PRIMARY KEY (`code`)
) ENGINE = InnoDB;

CREATE USER 'zongji'@'%' IDENTIFIED BY 'password';
GRANT REPLICATION SLAVE, REPLICATION CLIENT, SELECT ON *.* TO 'zongji'@'%';

GRANT INSERT, DELETE ON `util`.* TO 'zongji'@'%';
