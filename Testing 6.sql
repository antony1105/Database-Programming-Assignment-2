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
  DELETE FROM fss_run_table
  WHERE trunc(runStart, 'DDD') = trunc(SYSDATE, 'DDD');
  DBMS_JOB.RUN(1505);
end;
/

select max(lodgementRef), min(lodgementRef)
from fss_daily_settlement
where substr(lodgementRef, 1, 8) = to_char(trunc(SYSDATE, 'DDD'), 'yyyymmdd');

select substr('201505060000000012', 1, 8) from dual;

DELETE FROM fss_run_table
WHERE trunc(runStart, 'DDD') = trunc(SYSDATE, 'DDD');

update fss_daily_transactions
set settlementStatus = 'Null';

select te.merchantId, sum(dt.transactionAmount)
from fss_daily_transactions dt
INNER JOIN fss_terminal te
ON dt.terminalId = te.terminalId
group by te.merchantId;

BEGIN
  dbms_output.put_line(TO_CHAR(LAST_DAY(null), 'dd/mm/yyyy'));
END;
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

DECLARE
  l_pixel_number NUMBER := 50;
  l_word VARCHAR2(50) := 'First Settlement Report';
  l_length NUMBER := length(l_word);
  l_side_pixel NUMBER := floor((l_pixel_number - l_length) / 2);
  l_indicator VARCHAR2(1) := ' ';
  l_rpad VARCHAR2(100) := rpad(l_word, l_length + l_side_pixel, l_indicator);
  l_lpad VARCHAR2(100) := lpad(l_rpad, length(l_rpad) + l_side_pixel, l_indicator);
BEGIN
  dbms_output.put_line(l_lpad);
END;
/

select merchantId
from (
  select * from fss_merchant);

SELECT to_char(sysdate, 'YYYYMMDD') || to_char(seq_lodgement_reference.nextval, 'FM0000000')
        , merchantId
        , merchantBsb
        , merchantAccNr
        , transactionAmount
        , merchantTitle
      FROM 
        (
          SELECT m.merchantId AS merchantId
            , m.merchantBankBsb AS merchantBsb
            , m.merchantBankAccNr AS merchantAccNr
            , sum(dt.transactionAmount) AS transactionAmount
            , UPPER(m.merchantAccountTitle) AS merchantTitle
          FROM fss_daily_transactions dt
          INNER JOIN fss_smartcard s
          ON dt.cardId = s.cardId
          INNER JOIN fss_terminal te
          ON dt.terminalId = te.terminalId
          INNER JOIN fss_terminal_type tet
          ON te.terminalType = tet.typeName
          INNER JOIN fss_merchant m
          ON te.merchantId = m.merchantId
          WHERE dt.settlementStatus = 'Checked'
          GROUP BY m.merchantId
            , m.merchantBankBsb
            , m.merchantBankAccNr
            , m.merchantAccountTitle
        );
SELECT m.merchantId AS merchantId
            , m.merchantBankBsb AS merchantBsb
            , m.merchantBankAccNr AS merchantAccNr
            , sum(dt.transactionAmount) AS transactionAmount
            , UPPER(m.merchantAccountTitle) AS merchantTitle
          FROM fss_daily_transactions dt
          INNER JOIN fss_smartcard s
          ON dt.cardId = s.cardId
          INNER JOIN fss_terminal te
          ON dt.terminalId = te.terminalId
          INNER JOIN fss_terminal_type tet
          ON te.terminalType = tet.typeName
          INNER JOIN fss_merchant m
          ON te.merchantId = m.merchantId
          WHERE dt.settlementStatus = 'Checked'
          GROUP BY m.merchantId
            , m.merchantBankBsb
            , m.merchantBankAccNr
            , m.merchantAccountTitle;

select debit
from fss_daily_settlement;

--FUNCTION get_centered_text(p_word VARCHAR2, p_length NUMBER, p_indicator VARCHAR2)
  --RETURN VARCHAR2
  --IS
DECLARE
    l_length NUMBER := length('Wira');
    l_side_pixel NUMBER := floor(('Wira' - l_length) / 2);
    l_rpad VARCHAR2(1000) := rpad('Wira', l_length + l_side_pixel, ' ');
    l_lpad VARCHAR2(1000) := lpad(l_rpad, length(l_rpad) + l_side_pixel, ' ');
  BEGIN
    dbms_output.put_line(l_lpad);
  END;
/
  
SELECT REGEXP_COUNT ('sdge\nhewshhshweh\neehsthsrh\nerterysef', '\n')
FROM dual;