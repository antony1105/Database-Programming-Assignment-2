CREATE OR REPLACE PACKAGE common
AS
  PROCEDURE upd_error_table(p_err_msg VARCHAR2, p_location VARCHAR2); 
  PROCEDURE ins_run_table(p_run_start TIMESTAMP, p_outcome VARCHAR2, p_err_message VARCHAR2 := NULL);
END;
/

CREATE OR REPLACE PACKAGE BODY common
AS
  PROCEDURE upd_error_table(p_err_msg VARCHAR2, p_location VARCHAR2) 
  IS
  BEGIN
    INSERT INTO error_table
    VALUES
      (
        p_err_msg
        , SYSTIMESTAMP
        , p_location
      );
    COMMIT; 
  END upd_error_table; 
  
  PROCEDURE ins_run_table(p_run_start TIMESTAMP, p_outcome VARCHAR2, p_err_message VARCHAR2 := NULL)
  IS
  BEGIN
    INSERT INTO fss_run_table
      (
        runStart
        , runEnd
        , runOutcome
        , remarks
      )
    VALUES
      (
        p_run_start
        , SYSTIMESTAMP
        , p_outcome
        , p_err_message
      );
  EXCEPTION
    WHEN OTHERS 
    THEN
      common.upd_error_table(SQLERRM, 'ins_run_table');
  END ins_run_table;
END common;
/

exec common.ins_run_table(systimestamp, 'Failed');