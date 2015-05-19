set serveroutput on;
select * from fss_merchant;
select * from fss_terminal
order by merchantId, terminalType;
select * from fss_terminal_type;
select * from fss_daily_transactions
--where settlementStatus = 'Null'
--where terminalId = 0051001500;
order by transactionNr desc;
select * from fss_reference;
select * from fss_transactions
order by transactionNr;
select * from fss_smartcard;
select * from fss_organisation;
select * from fss_daily_settlement;

begin
  execute immediate('truncate table fss_daily_transactions');
  execute immediate('truncate table fss_daily_settlement');
  COMMIT;
end;
/

CREATE OR REPLACE DIRECTORY WT_11993577 AS '/exports/orcloz';

DECLARE
  l_file  utl_file.file_type;
BEGIN
  l_file := utl_file.fopen ('WT_11993577','U11993577.txt', 'W');
  utl_file.put_line(l_file, 'Test print');
  utl_file.fclose(l_file);
END;
/

DECLARE
  l_file utl_file.file_type;
  l_date VARCHAR2(10) := to_char(sysdate, 'ddmmyyyy');
  l_number_format VARCHAR2(15) := 'FM0999999999';
  l_credit VARCHAR2(15) := to_char(12345, l_number_format);
  l_debit VARCHAR2(15) := to_char(12345, l_number_format);
  l_file_total VARCHAR2(15) := to_char(l_credit - l_debit, l_number_format);
  l_record_count VARCHAR2(10) := to_char(9, 'FM09999');
BEGIN
  dbms_output.put_line(l_file_total || l_credit || l_debit);
  l_file := utl_file.fopen ('WT_11993577','DS_' || l_date || '_WT.dat', 'W');
  utl_file.put_line(l_file, '1' || lpad(' ' , 17) || '01WBC' || lpad(' ' , 7) || rpad('S/CARD BUS PAYMENTS', 25) || '038759' || rpad('INVOICES', 12) || to_char(SYSDATE, 'ddmmyy'));
  utl_file.put_line(l_file, '1015-010270249893');
  utl_file.put_line(l_file, '7999-999' || lpad(' ', 11) || l_file_total || l_credit || l_debit || lpad(' ', 24) || l_record_count);
  utl_file.fclose(l_file);
END;
/

select to_char('12345', '099999999')
from dual;

UPDATE fss_daily_transactions
    SET settlementStatus = 'Checked'
    WHERE transactionNr <= 93048
    AND settlementStatus = 'Null'
    AND merchantId IN 
      (
        SELECT merchantId
        FROM fss_daily_transactions
        WHERE settlementStatus = 'Null'
        HAVING SUM(transactionAmount) >
          (
            SELECT referenceValue * 100
            FROM fss_reference
            WHERE referenceId = 'DMIN'
          )
        GROUP BY merchantId
      );
/
SELECT MAX(transactionNr)
    FROM fss_daily_transactions;
    
SELECT LAST_DAY(runEnd)
FROM fss_run_table
WHERE runOutcome = 'Success' 
AND runId = 
  (
    SELECT MAX(runId)
    FROM fss_run_table
  );
  
CREATE OR REPLACE FUNCTION chk_last_day_of_month
  RETURN VARCHAR2
  IS
    l_last_day DATE;
  BEGIN
    SELECT TO_DATE(LAST_DAY(runEnd), 'dd/mm/yyyy')
    INTO l_last_day
    FROM fss_run_table
    WHERE runOutcome = 'Success' 
    AND runId = 
      (
        SELECT MAX(runId)
        FROM fss_run_table
      );
    IF to_date(SYSDATE, 'dd/mm/yyyy') > l_last_day
    THEN
      RETURN '1';
    ELSE
      RETURN NULL;
    END IF;
    --RETURN l_last_day;
  EXCEPTION
    WHEN OTHERS 
    THEN
      common.upd_error_table(SQLERRM, 'chk_last_day_of_month');
  END;
  /
DECLARE
  l_variable VARCHAR2(50);
BEGIN
  SELECT transactionNr
  INTO l_variable
  FROM fss_daily_transactions;
END;
/

exec chk_last_day_of_month

select chk_last_day_of_month
from dual;

SELECT *
FROM fss_daily_settlement
WHERE ;

select max(lodgementRef)
from fss_daily_settlement;