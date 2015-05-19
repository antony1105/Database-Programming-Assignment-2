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
    RETURN l_last_lodgement_ref;
  EXCEPTION
    WHEN OTHERS 
    THEN
      common.upd_error_table(SQLERRM, 'get_last_lodgement_ref');
  END get_last_lodgement_ref;

FUNCTION get_last_day_of_month
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
    RETURN l_last_day;
  EXCEPTION
    WHEN NO_DATA_FOUND
    THEN
      RETURN to_date(SYSDATE + 1, 'dd/mm/yyyy');
    WHEN OTHERS 
    THEN
      common.upd_error_table(SQLERRM, 'chk_last_day_of_month');
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
  RETURN NUMBER IS
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
        merchantBsb
        , merchantAccNum
        , tranCode
        , transaction
        , merchantTitle
        , bankingFlag
      )
      SELECT m.merchantBankBsb
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
      GROUP BY m.merchantBankBsb
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
  
  PROCEDURE DailySettlement
  IS
    run_start TIMESTAMP;
  BEGIN
    run_start := SYSTIMESTAMP;
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
    dbms_output.put_line(g_last_transaction_nr);
    upd_daily_transaction_settled(SYSTIMESTAMP, 'Checked');
    common.ins_run_table(run_start, 'Success');
  COMMIT;
  EXCEPTION
    WHEN OTHERS 
    THEN
      common.upd_error_table(SQLERRM, 'DailySettlement');
      common.ins_run_table(run_start, 'Failed', SQLERRM);  
      ROLLBACK;
  END DailySettlement;
END pkg_fss_settlement;