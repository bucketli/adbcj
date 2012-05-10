
DROP TABLE IF EXISTS simple_values;

CREATE TABLE simple_values (
  int_val int,
  str_val varchar(255)
);

INSERT INTO simple_values (int_val, str_val) values (null, null);
INSERT INTO simple_values (int_val, str_val) values (0, 'Zero');
INSERT INTO simple_values (int_val, str_val) values (1, 'One');
INSERT INTO simple_values (int_val, str_val) values (2, 'Two');
INSERT INTO simple_values (int_val, str_val) values (3, 'Three');
INSERT INTO simple_values (int_val, str_val) values (4, 'Four');

DROP TABLE IF EXISTS updates;
CREATE TABLE updates (id int) type=InnoDB;

DROP TABLE IF EXISTS locks;
CREATE TABLE locks (name varchar(255) primary key not null) type=InnoDB;
INSERT INTO locks(name) VALUES ('lock');

DROP TABLE IF EXISTS table_with_some_values;
CREATE TABLE IF NOT EXISTS table_with_some_values (
  auto_int int(11) NOT NULL AUTO_INCREMENT,
  can_be_null_int int(11) DEFAULT NULL,
  can_be_null_varchar varchar(255) DEFAULT NULL,
  PRIMARY KEY (auto_int)
) ENGINE=InnoDB  DEFAULT CHARSET=UTF8 AUTO_INCREMENT=3 ;


INSERT INTO table_with_some_values (auto_int, can_be_null_int, can_be_null_varchar) VALUES
(1, NULL, NULL),
(2, 42, '42');

DROP TABLE IF EXISTS supporteddatatypes;
CREATE TABLE IF NOT EXISTS `supporteddatatypes` (
  intColumn int(11) NOT NULL,
  varCharColumn varchar(255) NOT NULL,
  bigIntColumn bigint(20) NOT NULL,
  decimalColumn decimal(10,2) NOT NULL,
  dateColumn date NOT NULL,
  doubleColumn double NOT NULL
)  ENGINE=InnoDB  DEFAULT CHARSET=UTF8;


INSERT INTO `adbcjtck`.`supportedDataTypes` (
  intColumn,
  varCharColumn,
  bigIntColumn,
  decimalColumn,
  dateColumn,
  doubleColumn
)
VALUES (
'42', '4242', '42', '42.42', '2012-05-03', '42.42'
);