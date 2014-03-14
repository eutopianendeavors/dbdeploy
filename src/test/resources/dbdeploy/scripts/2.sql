-- DDL changes commit automatically.
DROP PROCEDURE IF EXISTS simple_proc;

DELIMITER //

CREATE PROCEDURE simple_proc (OUT param_1 INTEGER)
BEGIN
    SELECT count(*) INTO param_1 FROM USERS_TABLE;
END//

DELIMITER ;

-- DML changes are wrapped in a transaction so that, if the fail, they rollback.
SET AUTOCOMMIT = 0;
START TRANSACTION;

INSERT INTO database_version (version) VALUES (2);

COMMIT;
