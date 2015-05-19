select * from user_jobs;

DECLARE
JobNo user_jobs.job%TYPE;
BEGIN
  dbms_job.submit
    (
      JobNo
    , q'[BEGIN
        pkg_fss_settlement.DailySettlement;     
      EXCEPTION
        WHEN OTHERS
        THEN 
          common.upd_error_table(SQLERRM, 'jobs');        
      END;]'-- where the name of your program goes Note the closing semi-colon
    , SYSDATE   -- this means that the first run will be now
    , 'trunc(SYSDATE + 1) + 19/24'
    ); -- This tells it to run every day at 7PM
END;
/

EXECUTE DBMS_JOB.RUN(1385);

exec dbms_job.remove(1366);

DECLARE
        run_start TIMESTAMP;
      BEGIN
        run_start := SYSTIMESTAMP;
        pkg_fss_settlement.DailySettlement;     
        common.ins_run_table(run_start, 'Success');
      EXCEPTION
        WHEN OTHERS
        THEN 
          common.upd_error_table(SQLERRM, 'jobs');
          common.ins_run_table(run_start, 'Failed', SQLERRM);          
      END;
      /
      
select * from fss_run_table;
    
SELECT DISTINCT location
FROM error_table
WHERE substr(error_timestamp, 1, 9) = substr(sysdate, 1, 9)
AND location = 'DailySettlement';

select substr(sysdate, 1, 9) from dual;

SELECT
      (
        orgBsbNr
        --, orgBankAccount
        --, '13'
       -- , l_sum_credit
       -- , orgAccountTitle
       -- , 'N'
      )
    FROM fss_organisation;
    
select orgbankaccount