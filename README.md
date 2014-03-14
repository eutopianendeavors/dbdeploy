Notes:
- If using MySQL, make sure that the default table engine is InnoDB.
  - In my.cnf make sure "default-storage-engine=innodb" is set.
- If using MySQL, you'll also want to make sure that autocommit is turned off.
  - As root user in mysql, run "SET GLOBAL init_connect='SET autocommit=0';".
  - In my.cnf make sure "init_connect='SET autocommit=0'" is set.
