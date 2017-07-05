-- DROP DATABASE IF EXISTS rentroll;
-- CREATE DATABASE rentroll;
-- USE rentroll;
-- GRANT ALL PRIVILEGES ON rentroll.* TO 'ec2-user'@'localhost';
set GLOBAL sql_mode='ALLOW_INVALID_DATES';

-- ===========================================
--   TWS - Timed Work Scheduler
-- ===========================================
CREATE TABLE TWS (
    TWSID BIGINT NOT NULL AUTO_INCREMENT,   					-- unique identifier for this work item
    Owner VARCHAR(256) NOT NULL DEFAULT '',     				-- unique identifier of current owner
    OwnerData VARCHAR(256) NOT NULL DEFAULT '', 				-- data needed by the Owner
    WorkerName VARCHAR(256) NOT NULL DEFAULT '',				-- name of registered handler
    ActivateTime DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,	-- time to activate this item
    Node VARCHAR(256) NOT NULL DEFAULT '',						-- which node is executing this
    FLAGS BIGINT NOT NULL DEFAULT 0, 							-- 1<<0 worker active, 1<<1 worker ack, 1<<2 work rescheduled
    DtActivated DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,	-- time when the scheduler last activated the item
    DtCompleted DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,	-- time when the itemindicated that it was complete
    DtCreate DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,		-- when item was initially created
    DtLastUpdate DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,	
    PRIMARY KEY(TWSID)

);
