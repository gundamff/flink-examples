CREATE TABLE `dataevent` (
  `id` varchar(32) NOT NULL,
  `eventTime` datetime NOT NULL,
  `value` int(11) DEFAULT NULL,
  `insertDbTime` datetime DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`,`eventTime`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
