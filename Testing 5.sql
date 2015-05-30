select * from user_jobs;

DECLARE
JobNo user_jobs.job%TYPE;
BEGIN
  dbms_job.submit
    (
      JobNo
    , 'pkg_fss_settlement.DailySettlement;'  -- where the name of your program goes Note the closing semi-colon
    , SYSDATE   -- this means that the first run will be now
    , 'trunc(SYSDATE + 1) + 19/24'
    ); -- This tells it to run every day at 7PM
END;
/

EXECUTE DBMS_JOB.RUN(1710);

exec dbms_job.remove(1709);

BEGIN
        pkg_fss_settlement.DailySettlement;
        --pkg_fss_settlement.DailyBankingSummary(to_date(1/2/2025, 'dd/mm/yyyy'));  
END;
/

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
    /

SELECT min(lodgementRef), max(lodgementRef)
    FROM fss_daily_settlement
    WHERE substr(lodgementRef, 1, 8) = to_char(trunc(sysdate, 'DDD'), 'yyyymmdd');