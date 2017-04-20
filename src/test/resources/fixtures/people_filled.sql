CREATE TABLE people (id INT, name VARCHAR, age INT);

INSERT INTO people (id, name, age) VALUES
	(1, 'Alex', 26),
	(2, 'Lisa', 22),
	(3, e'With\r\n\t special chars\\', 10),
	(4, null, -1);
