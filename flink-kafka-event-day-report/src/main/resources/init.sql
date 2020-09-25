CREATE TABLE `dataeventDayReport` (
  `id` varchar(32) NOT NULL,
  `fiveMinutes` datetime NOT NULL,
  `updateTime` datetime DEFAULT CURRENT_TIMESTAMP,
  `h0` int(11) DEFAULT NULL,
  `h1` int(11) DEFAULT NULL,
  `h2` int(11) DEFAULT NULL,
  `h3` int(11) DEFAULT NULL,
  `h4` int(11) DEFAULT NULL,
  `h5` int(11) DEFAULT NULL,
  `h6` int(11) DEFAULT NULL,
  `h7` int(11) DEFAULT NULL,
  `h8` int(11) DEFAULT NULL,
  `h9` int(11) DEFAULT NULL,
  `h10` int(11) DEFAULT NULL,
  `h11` int(11) DEFAULT NULL,
  `h12` int(11) DEFAULT NULL,
  `h13` int(11) DEFAULT NULL,
  `h14` int(11) DEFAULT NULL,
  `h15` int(11) DEFAULT NULL,
  `h16` int(11) DEFAULT NULL,
  `h17` int(11) DEFAULT NULL,
  `h18` int(11) DEFAULT NULL,
  `h19` int(11) DEFAULT NULL,
  `h20` int(11) DEFAULT NULL,
  `h21` int(11) DEFAULT NULL,
  `h22` int(11) DEFAULT NULL,
  `h23` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`,`fiveMinutes`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


DELIMITER $$
CREATE DEFINER=`root`@`localhost` PROCEDURE `savedataeventDayReport`(in id varchar(32),in eventDate DATETIME,in hn varchar(3),in lastValue int )
BEGIN
	declare SQL_FOR_UPDATE varchar(500);
	SET SQL_FOR_UPDATE = CONCAT("insert into `dataeventDayReport`(`id`, `fiveMinutes`,`",hn,"`) values(?, ?, ?) ON duplicate key update`",hn,"`=VALUES(`",hn,"`)");
	set @sql = SQL_FOR_UPDATE;
    PREPARE stmt FROM @sql;
    set @parm1 = id;
	set @parm2 = eventDate;
    set @parm3 = lastValue;
    EXECUTE stmt USING @parm1 , @parm2,@parm3;
	deallocate prepare stmt;
END$$
DELIMITER ;
