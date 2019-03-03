-- +migrate Up

CREATE TABLE `dql` (
  `id` varchar(255) NOT NULL DEFAULT '',
  `service` varchar(64) NOT NULL DEFAULT '',
  `aggregateID` varchar(64) NOT NULL DEFAULT '',
  `version` varchar(20) NOT NULL DEFAULT '',
  `lambdaFunction` varchar(64) NOT NULL DEFAULT '',
  `eventType` varchar(64) NOT NULL DEFAULT '',
  `eventID` varchar(64) NOT NULL DEFAULT '',
  `seq` bigint(20) NOT NULL DEFAULT '0',
  `time` bigint(20) NOT NULL DEFAULT '0',
  `dqlTime` bigint(20) NOT NULL DEFAULT '0',
  `eventMsg` blob NOT NULL,
  `error` blob NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- +migrate Down

DROP TABLE dql;
