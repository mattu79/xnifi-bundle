CREATE TABLE `user_edits`  (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `username` varchar(255),
  `edits` int(10) NOT NULL,
  `total` int(10) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE = InnoDB;