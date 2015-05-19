Set Serveroutput On;

DECLARE
BEGIN
  pkg_fss_settlement.upd_fss_daily_transaction;
END;
/

DECLARE
BEGIN
  pkg_fss_settlement.ins_daily_settlement;
END;
/

DECLARE
BEGIN
  pkg_fss_settlement.DailySettlement;
END;
/

CREATE OR REPLACE PACKAGE pkg_fss_settlement
AS
  PROCEDURE upd_fss_daily_transaction;
  PROCEDURE ins_daily_settlement;
  PROCEDURE DailySettlement;
  PROCEDURE upd_daily_transaction_settled(p_change_value VARCHAR2, p_settlement_status VARCHAR2 := 'Null');
END pkg_fss_settlement;
/

CREATE OR REPLACE PACKAGE BODY pkg_fss_settlement
AS
  g_last_transaction_nr NUMBER;
  g_last_settlement NUMBER;
  gc_credit_banking_flag CONSTANT VARCHAR2(1) := 'F';
  gc_credit_tran_code CONSTANT NUMBER := 50;
  gc_debit_banking_flag CONSTANT VARCHAR2(1) := 'N';
  gc_debit_tran_code CONSTANT NUMBER := 13;
  g_sum_credit NUMBER;
  g_directory_name VARCHAR2(50) := 'WT_11993577';
  g_deskbank_file_name VARCHAR2(50) := 'DS_' || to_char(SYSDATE, 'ddmmyyyy') || 'WT';
  
  FUNCTION get_space_after_name(p_name VARCHAR2)
  RETURN VARCHAR2
  IS
    l_max_length NUMBER;
  BEGIN
    SELECT MAX(LENGTH(merchantTitle))
    INTO l_max_length
    FROM fss_daily_settlement;
  RETURN rpad(p_name, l_max_length, ' ');
  END get_space_after_name;
  
  FUNCTION get_last_lodgement_ref
  RETURN NUMBER
  IS
    l_last_lodgement_ref NUMBER;
  BEGIN
    SELECT MAX(lodgementRef)
    INTO l_last_lodgement_ref
    FROM fss_daily_settlement;
    IF l_last_lodgement_ref IS NOT NULL
    THEN
      RETURN l_last_lodgement_ref;
    ELSE
      RETURN 1;
    END IF;
  EXCEPTION
    WHEN OTHERS 
    THEN
      common.upd_error_table(SQLERRM, 'get_last_lodgement_ref');
  END get_last_lodgement_ref;
  
  FUNCTION get_last_run_date(p_run_outcome VARCHAR2)
  RETURN DATE
  IS
    l_last_date_run DATE;
  BEGIN
    SELECT runEnd
    INTO l_last_date_run
    FROM fss_run_table
    WHERE runOutcome = p_run_outcome 
    AND runId = 
      (
        SELECT MAX(runId)
        FROM fss_run_table
      );
    RETURN l_last_date_run;
  EXCEPTION
    WHEN OTHERS 
    THEN
      common.upd_error_table(SQLERRM, 'get_last_run_date');
  END get_last_run_date;
  
  FUNCTION get_last_day_of_month
  RETURN VARCHAR2
  IS
    l_last_run DATE := get_last_run_date('Success');
  BEGIN
    IF l_last_run IS NOT NULL
    THEN
      RETURN to_date(last_day(l_last_run), 'dd/mm/yyyy');
    ELSE
      RETURN to_date(SYSDATE + 1, 'dd/mm/yyyy');
    END IF;
  EXCEPTION
    WHEN OTHERS 
    THEN
      common.upd_error_table(SQLERRM, 'get_last_day_of_month');
  END;
  
  PROCEDURE upd_daily_transaction_settled(p_change_value VARCHAR2, p_settlement_status VARCHAR2 := 'Null')
  IS
    l_last_day DATE := get_last_day_of_month;
  BEGIN
    IF to_date(SYSDATE, 'dd/mm/yyyy') > l_last_day
    THEN
      UPDATE fss_daily_transactions
      SET settlementStatus = p_change_value
      WHERE to_date(downloadDate, 'dd/mm/yyyy') < l_last_day;
    END IF;
    UPDATE fss_daily_transactions
    SET settlementStatus = p_change_value
    WHERE transactionNr <= g_last_transaction_nr
    AND settlementStatus = p_settlement_status
    AND merchantId IN 
      (
        SELECT merchantId
        FROM fss_daily_transactions
        WHERE settlementStatus = p_settlement_status
        HAVING SUM(transactionAmount) >
          (
            SELECT referenceValue * 100
            FROM fss_reference
            WHERE referenceId = 'DMIN'
          )
        GROUP BY merchantId
      );
    COMMIT;
  EXCEPTION
    WHEN OTHERS 
    THEN
      common.upd_error_table(SQLERRM, 'upd_daily_transaction_settled');
  END upd_daily_transaction_settled;
  
  FUNCTION get_format_bsb(p_value VARCHAR2)
  RETURN VARCHAR2
  IS
  BEGIN
    RETURN substr(p_value, 1,3) || '-' || substr(p_value, 4,6);
  END get_format_bsb;
  
  FUNCTION get_last_transaction_nr
  RETURN NUMBER 
  IS
    l_last_transaction_nr fss_daily_transactions.transactionNr%TYPE;
  BEGIN
    SELECT MAX(transactionNr)
    INTO l_last_transaction_nr
    FROM fss_daily_transactions;
    IF l_last_transaction_nr IS NULL
    THEN
      RETURN 1;
    ELSE
      RETURN l_last_transaction_nr;
    END IF;
  EXCEPTION
    WHEN OTHERS 
    THEN
      common.upd_error_table(SQLERRM, 'get_last_transaction_nr');
  END get_last_transaction_nr;
  
  FUNCTION get_sum_credit
  RETURN NUMBER
  IS
    l_sum_of_credit NUMBER;
  BEGIN
    SELECT SUM(transactionAmount)
    INTO l_sum_of_credit
    FROM fss_daily_transactions
    WHERE settlementStatus = 'Checked';
    RETURN l_sum_of_credit;
  END get_sum_credit;
  
  PROCEDURE upd_fss_daily_transaction
  IS
  BEGIN  
    INSERT INTO fss_daily_transactions
      SELECT t.transactionNr
      , t.downloadDate
      , t.terminalId
      , t.cardId
      , t.transactionDate
      , t.cardOldValue
      , t.transactionAmount
      , t.cardNewValue
      , t.transactionStatus
      , t.errorCode
      , te.merchantId
      , 'Null'
      FROM fss_transactions t
      INNER JOIN fss_terminal te
      ON t.terminalId = te.terminalId
      WHERE transactionNr > g_last_transaction_nr;
    COMMIT;
  EXCEPTION
    WHEN OTHERS 
    THEN
      common.upd_error_table(SQLERRM, 'upd_fss_daily_transaction');
  END upd_fss_daily_transaction;
  
  PROCEDURE ins_daily_settlement
  IS 
  BEGIN
    INSERT INTO fss_daily_settlement
      (
        merchantId
        , merchantBsb
        , merchantAccNum
        , tranCode
        , transaction
        , merchantTitle
        , bankingFlag
      )
      SELECT m.merchantId
        , m.merchantBankBsb
        , m.merchantBankAccNr
        , gc_credit_tran_code
        , SUM(dt.transactionAmount) AS value
        , m.merchantAccountTitle
        , gc_credit_banking_flag
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
    COMMIT;
  EXCEPTION
    WHEN OTHERS 
    THEN
      common.upd_error_table(SQLERRM, 'ins_daily_settlement');
  END ins_daily_settlement;
  
  PROCEDURE ins_debit_settlement
  IS
    l_sum_credit NUMBER := get_sum_credit;
  BEGIN
    IF l_sum_credit IS NOT NULL
    THEN 
      INSERT INTO fss_daily_settlement
        (
          merchantBsb
          , merchantAccNum
          , tranCode
          , transaction
          , merchantTitle
          , bankingFlag
        )
      SELECT orgBsBNr
          , orgBankAccount
          , gc_debit_tran_code
          , l_sum_credit
          , orgAccountTitle
          , gc_debit_banking_flag
      FROM fss_organisation;
    END IF;
    COMMIT;
  EXCEPTION
    WHEN OTHERS 
    THEN
      common.upd_error_table(SQLERRM, 'ins_debit_settlement');
  END ins_debit_settlement;
  
  FUNCTION get_deskbank_header
  RETURN VARCHAR2
  IS
    l_record_type VARCHAR2(3) := '1';
    l_reel_sequence VARCHAR2(3) := '01';
    l_fi_code VARCHAR2(5) := 'WBC';
    l_user VARCHAR2(50) := 'S/CARD BUS PAYMENTS';
    l_user_bsb VARCHAR2(6) := '038759';
    l_description VARCHAR2(50) := 'INVOICES';
    l_processing_date VARCHAR2(6) := to_char(SYSDATE, 'ddmmyy');
  BEGIN
    RETURN l_record_type || lpad(' ' , 17) || l_reel_sequence || l_fi_code || lpad(' ' , 7) || rpad(l_user, 26) || l_user_bsb || rpad('INVOICES', 12) || l_processing_date;
  EXCEPTION
    WHEN OTHERS 
    THEN
      common.upd_error_table(SQLERRM, 'get_deskbank_header');
  END get_deskbank_header;
  
  FUNCTION get_settlement_count(p_lower_lodgement_ref NUMBER, p_higher_lodgement_ref NUMBER := get_last_lodgement_ref)
  RETURN NUMBER
  IS
    l_settlement_count NUMBER;
  BEGIN
    SELECT COUNT(lodgementRef)
    INTO l_settlement_count
    FROM fss_daily_settlement
    WHERE lodgementRef BETWEEN
      p_lower_lodgement_ref AND p_higher_lodgement_ref;
    RETURN l_settlement_count;
  EXCEPTION
    WHEN OTHERS 
    THEN
      common.upd_error_table(SQLERRM, 'get_settlement_count');
  END get_settlement_count;
  
  FUNCTION get_deskbank_footer
  RETURN VARCHAR2
  IS
    l_type VARCHAR2(3) := '7';
    l_filler VARCHAR2(10) := '999-999';
    l_number_format VARCHAR2(15) := 'FM0999999999';
    l_credit VARCHAR2(15) := to_char(get_sum_credit, l_number_format);
    l_debit VARCHAR2(15) := l_credit;
    l_file VARCHAR2(15) := to_char(l_credit - l_debit, l_number_format);
    l_record_count VARCHAR2(10) := to_char(get_settlement_count(g_last_settlement + 1), 'FM099999');
  BEGIN
    RETURN l_type || l_filler || lpad(' ', 12) || l_file || l_credit || l_debit || lpad(' ', 24) || l_record_count;
  EXCEPTION
    WHEN OTHERS 
    THEN
      common.upd_error_table(SQLERRM, 'get_deskbank_footer'); 
  END get_deskbank_footer;

  PROCEDURE ins_deskbank_file
  IS
    l_file  utl_file.file_type;
    l_date VARCHAR2(10) := to_char(SYSDATE, 'ddmmyyyy');
    l_file_name VARCHAR2(30) := 'DS_' || l_date || '_WT.dat';
    l_directory VARCHAR2(20) := 'WT_11993577';
  BEGIN
    l_file := utl_file.fopen (l_directory,l_file_name, 'W');
    utl_file.put_line(l_file, get_deskbank_header);
    utl_file.put_line(l_file, '1015-010270249893' || chr(10) || 'Wira');
    utl_file.put_line(l_file, get_deskbank_footer);
    utl_file.fclose(l_file);
  EXCEPTION
    WHEN OTHERS 
    THEN
      common.upd_error_table(SQLERRM, 'ins_deskbank_file');
      utl_file.fclose(l_file);
  END ins_deskbank_file;
  
  PROCEDURE DailySettlement
  IS
    l_run_start TIMESTAMP := SYSTIMESTAMP;
    l_last_successful_run_date VARCHAR2(50) := trunc(get_last_run_date('Success'), 'DDD');
  BEGIN
    dbms_output.put_line(l_last_successful_run_date || ' ' || sysdate);
    IF l_last_successful_run_date <> trunc(SYSDATE, 'DDD')
    THEN
      g_last_transaction_nr := get_last_transaction_nr;
      dbms_output.put_line('first' || g_last_transaction_nr);
      upd_fss_daily_transaction;
      g_last_transaction_nr := get_last_transaction_nr;
      upd_daily_transaction_settled('Checked');
      dbms_output.put_line('second' || g_last_transaction_nr);
      g_last_settlement := get_last_lodgement_ref;
      dbms_output.put_line('first' || get_last_lodgement_ref);
      ins_daily_settlement;
      ins_debit_settlement;
      ins_deskbank_file;
      dbms_output.put_line(g_last_transaction_nr);
      upd_daily_transaction_settled(SYSTIMESTAMP, 'Checked');
      common.ins_run_table(l_run_start, 'Success');
    END IF;
  COMMIT;
  EXCEPTION
    WHEN OTHERS 
    THEN
      common.upd_error_table(SQLERRM, 'DailySettlement');
      common.ins_run_table(l_run_start, 'Failed', SQLERRM);  
      ROLLBACK;
  END DailySettlement;
END pkg_fss_settlement;