CREATE TABLE `test`.`dataevent` (
  `id1` VARCHAR(32) NOT NULL,
  `id2` VARCHAR(32) NOT NULL,
  `id3` VARCHAR(32) NOT NULL,
  `eventTime` DATETIME NULL,
  `value` INT NULL,
  PRIMARY KEY (`id1`, `id2`, `id3`));
