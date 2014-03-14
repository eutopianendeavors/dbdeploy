-- DDL changes commit automatically.
DROP TABLE IF EXISTS database_version;

CREATE TABLE database_version
  (
    version          INTEGER NOT NULL,
    last_updated TIMESTAMP NOT NULL,
    PRIMARY KEY ( version )
  );

CREATE TRIGGER last_updated_trigger BEFORE INSERT ON database_version FOR EACH ROW SET NEW.last_updated = CURRENT_TIMESTAMP;

-- DML changes are wrapped in a transaction so that, if the fail, they rollback.
SET AUTOCOMMIT = 0;
START TRANSACTION;

INSERT INTO database_version (version) VALUES (1);

COMMIT;