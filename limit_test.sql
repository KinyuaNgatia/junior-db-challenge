CREATE TABLE test_limit (id INT PRIMARY KEY, val TEXT);
INSERT INTO test_limit VALUES (1, 'a');
INSERT INTO test_limit VALUES (2, 'b');
INSERT INTO test_limit VALUES (3, 'c');
SELECT * FROM test_limit LIMIT 2;
exit
