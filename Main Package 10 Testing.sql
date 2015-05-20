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
  PROCEDURE DailyBankingSummary(p_date DATE);
END pkg_fss_settlement;
/

CREATE OR REPLACE PACKAGE BODY pkg_fss_settlement
AS
  g_last_transaction_nr NUMBER;
  g_last_settlement NUMBER;
  g_credit_sum NUMBER;
  gc_credit_banking_flag CONSTANT VARCHAR2(1) := 'F';
  gc_credit_tran_code CONSTANT NUMBER := 50;
  gc_debit_banking_flag CONSTANT VARCHAR2(1) := 'N';
  gc_debit_tran_code CONSTANT NUMBER := 13;
  g_directory_name VARCHAR2(50) := 'WT_11993577';
  g_deskbank_file_name VARCHAR2(50) := 'DS_' || to_char(SYSDATE, 'ddmmyyyy') || 'WT';
  g_seq_lodgement_ref NUMBER := seq_lodgement_reference.nextval;
  
  CURSOR g_c_settlements(p_last_settlement NUMBER, p_last_lodgement_ref NUMBER)
  IS
    SELECT merchantId
    , merchantBsb
    , merchantAccNum
    , tranCode
    , debit
    , credit
    , merchantTitle
    , bankingFlag
    , lodgementRef
    FROM fss_daily_settlement
    WHERE lodgementRef BETWEEN
      p_last_settlement AND p_last_lodgement_ref;
  
  FUNCTION get_last_lodgement_ref
  RETURN NUMBER
  IS
    l_last_lodgement_ref NUMBER;
  BEGIN
    SELECT max(lodgementRef)
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
    l_last_run_date DATE;
  BEGIN
    SELECT max(runEnd)
    INTO l_last_run_date
    FROM fss_run_table
    WHERE runOutcome = p_run_outcome;
    RETURN l_last_run_date;
  EXCEPTION
    WHEN NO_DATA_FOUND
    THEN
      RETURN SYSDATE - 1;
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
      RETURN trunc(last_day(l_last_run), 'DDD');
    ELSE
      RETURN trunc(SYSDATE + 1, 'DDD');
    END IF;
  EXCEPTION
    WHEN OTHERS 
    THEN
      common.upd_error_table(SQLERRM, 'get_last_day_of_month');
  END;
  
  PROCEDURE upd_daily_transaction_settled(p_change_value VARCHAR2, p_settlement_status VARCHAR2 := 'Null')
  IS
    l_last_day DATE := to_date('30/04/2015', 'dd/mm/yyyy');--get_last_day_of_month;
  BEGIN
    IF trunc(SYSDATE, 'DDD') > l_last_day
    THEN
      UPDATE fss_daily_transactions
      SET settlementStatus = p_change_value
      WHERE trunc(downloadDate, 'DDD') < l_last_day;
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
        HAVING sum(transactionAmount) >
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
  
  FUNCTION get_bsb_format(p_value VARCHAR2)
  RETURN VARCHAR2
  IS
  BEGIN
    RETURN substr(p_value, 1,3) || '-' || substr(p_value, 4,6);
  EXCEPTION
    WHEN OTHERS 
    THEN
      common.upd_error_table(SQLERRM, 'get_format_bsb');
  END get_bsb_format;
  
  FUNCTION get_last_transaction_nr
  RETURN NUMBER 
  IS
    l_last_transaction_nr fss_daily_transactions.transactionNr%TYPE;
  BEGIN
    SELECT max(transactionNr)
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
  
  FUNCTION get_credit_sum
  RETURN NUMBER
  IS
    l_sum_of_credit NUMBER;
  BEGIN
    SELECT sum(transactionAmount)
    INTO l_sum_of_credit
    FROM fss_daily_transactions
    WHERE settlementStatus = 'Checked';
    RETURN l_sum_of_credit;
  END get_credit_sum;
  
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
    --l_lodgement_ref NUMBER := to_char(sysdate, 'YYYYMMDD') || to_char(g_seq_lodgement_ref, 'FM0000000');
  BEGIN
    INSERT INTO fss_daily_settlement
      (
        lodgementRef
        , merchantId
        , merchantBsb
        , merchantAccNum
        , tranCode
        , credit
        , merchantTitle
        , bankingFlag
      )
      SELECT to_char(sysdate, 'YYYYMMDD') || to_char(seq_lodgement_reference.nextval, 'FM0000000')
        , merchantId
        , merchantBsb
        , merchantAccNr
        , gc_credit_tran_code
        , transactionAmount
        , merchantTitle
        , gc_credit_banking_flag
      FROM
        (
          SELECT m.merchantId AS merchantId
          , m.merchantBankBsb AS merchantBsb
          , m.merchantBankAccNr AS merchantAccNr
          , sum(dt.transactionAmount) AS transactionAmount
          , upper(m.merchantAccountTitle) AS merchantTitle
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
    COMMIT;
  EXCEPTION
    WHEN OTHERS 
    THEN
      common.upd_error_table(SQLERRM, 'ins_daily_settlement');
  END ins_daily_settlement;
  
  PROCEDURE ins_debit_settlement
  IS
    --l_lodgement_ref NUMBER := to_char(sysdate, 'YYYYMMDD') || to_char(g_seq_lodgement_ref, 'FM0000000');
  BEGIN
    IF g_credit_sum IS NOT NULL
    THEN 
      INSERT INTO fss_daily_settlement
        (
          lodgementRef
          , merchantBsb
          , merchantAccNum
          , tranCode
          , debit
          , merchantTitle
          , bankingFlag
        )
      SELECT to_char(sysdate, 'YYYYMMDD') || to_char(seq_lodgement_reference.nextval, 'FM0000000')
          , orgBsBNr
          , orgBankAccount
          , gc_debit_tran_code
          , g_credit_sum
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
  
  FUNCTION get_centered_text(p_word VARCHAR2, p_length NUMBER, p_indicator VARCHAR2)
  RETURN VARCHAR2
  IS
    l_length NUMBER := length(p_word);
    l_side_pixel NUMBER := floor((p_word - l_length) / 2);
    l_rpad VARCHAR2(1000) := rpad(p_word, l_length + l_side_pixel, p_indicator);
    l_lpad VARCHAR2(1000) := lpad(l_rpad, length(l_rpad) + l_side_pixel, p_indicator);
  BEGIN
    RETURN l_lpad;
  END get_centered_text;
  
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
    RETURN l_record_type || lpad(' ' , 17) || l_reel_sequence || l_fi_code || lpad(' ' , 7) || rpad(l_user, 26) || l_user_bsb 
      || rpad('INVOICES', 12) || l_processing_date || '\n';
  EXCEPTION
    WHEN OTHERS 
    THEN
      common.upd_error_table(SQLERRM, 'get_deskbank_header');
  END get_deskbank_header;
  
  FUNCTION get_deskbank_body
  RETURN VARCHAR2
  IS
    l_record NUMBER := 1;
    l_trace VARCHAR2(20) := '032-797 001006';
    l_remitter VARCHAR2(16) := 'SMARTCARD TRANS';
    l_gst_tax VARCHAR2(8) := '00000000';
    l_text VARCHAR2(3000);
    l_number_format VARCHAR2(20) := 'FM0999999999';
  BEGIN
    FOR rec_settlements IN g_c_settlements(g_last_settlement + 1, get_last_lodgement_ref)
    LOOP
      l_text := l_text || l_record || get_bsb_format(rec_settlements.merchantBsb) || rpad(rec_settlements.merchantAccNum, 10)
        || rec_settlements.tranCode || to_char(rec_settlements.debit, l_number_format) || to_char(rec_settlements.credit, l_number_format) 
        || rpad(rec_settlements.merchantTitle, 33) || rpad(rec_settlements.bankingFlag, 2) || rec_settlements.lodgementRef || l_trace || l_remitter 
        || l_gst_tax || '\n';
      --dbms_output.put_line(rec_settlements.lodgementRef);
    END LOOP;
    RETURN l_text;
  EXCEPTION
    WHEN OTHERS 
    THEN
      common.upd_error_table(SQLERRM, 'get_deskbank_body');
  END get_deskbank_body;
  
  FUNCTION get_settlement_count(p_lower_lodgement_ref NUMBER, p_higher_lodgement_ref NUMBER := get_last_lodgement_ref)
  RETURN NUMBER
  IS
    l_settlement_count NUMBER;
  BEGIN
    SELECT count(lodgementRef)
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
    l_credit VARCHAR2(15) := to_char(g_credit_sum, l_number_format);
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
  
  FUNCTION get_settlement_report_body(p_settlement_date DATE)
  RETURN VARCHAR2 
  IS
    l_lower_lodgement_ref NUMBER;
    l_higher_lodgement_ref NUMBER;
    l_text VARCHAR2(3000);
  BEGIN
    SELECT min(lodgementRef), max(lodgementRef)
    INTO l_lower_lodgement_ref, l_higher_lodgement_ref
    FROM fss_daily_settlement
    WHERE substr(lodgementRef, 1, 8) = to_char(p_settlement_date, 'yyyymmdd');
    
    FOR rec_settlements IN g_c_settlements(l_lower_lodgement_ref, l_higher_lodgement_ref)
    LOOP
      l_text := l_text || rpad(rec_settlements.merchantId, 12) || rpad(rec_settlements.merchantTitle, 32) || get_bsb_format(rec_settlements.merchantBsb)
        || rpad(rec_settlements.merchantAccNum, 12) ||rec_settlements.lodgementRef || '\n';
    END LOOP;
    RETURN l_text;
  EXCEPTION
    WHEN OTHERS 
    THEN
      common.upd_error_table(SQLERRM, 'get_settlement_report_body');
  END get_settlement_report_body;

  PROCEDURE ins_file(p_file_name VARCHAR2, p_header VARCHAR2, p_body VARCHAR2, p_footer VARCHAR2)
  IS
    l_file  utl_file.file_type;
    l_directory VARCHAR2(20) := 'WT_11993577';
  BEGIN
    l_file := utl_file.fopen (l_directory,p_file_name, 'W');
    utl_file.putf(l_file, p_header);
    utl_file.putf(l_file, p_body);
    utl_file.putf(l_file, p_footer);
    utl_file.fclose(l_file);
  EXCEPTION
    WHEN OTHERS 
    THEN
      common.upd_error_table(SQLERRM, 'ins_file');
      utl_file.fclose(l_file);
  END ins_file;
  
  PROCEDURE DailySettlement
  IS
    l_run_start TIMESTAMP := SYSTIMESTAMP;
    l_last_successful_run_date VARCHAR2(50) := trunc(get_last_run_date('Success'), 'DDD');
    l_date VARCHAR2(10) := to_char(SYSDATE, 'ddmmyyyy');
  BEGIN
    IF l_last_successful_run_date <> trunc(SYSDATE, 'DDD')
    THEN
      g_last_transaction_nr := get_last_transaction_nr;
      --dbms_output.put_line('first' || g_last_transaction_nr);
      upd_fss_daily_transaction;
      g_last_transaction_nr := get_last_transaction_nr;
      upd_daily_transaction_settled('Checked');
      --dbms_output.put_line('second' || g_last_transaction_nr);
      g_last_settlement := get_last_lodgement_ref;
      --dbms_output.put_line('first' || get_last_lodgement_ref);
      ins_daily_settlement;
      g_credit_sum := get_credit_sum;
      ins_debit_settlement;
      ins_file('DS_' || l_date || '_WT.dat', get_deskbank_header, get_deskbank_body, get_deskbank_footer) ;
      --dbms_output.put_line('Check' || g_last_transaction_nr);
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
  
  PROCEDURE DailyBankingSummary(p_date DATE)
  IS
    l_file_name VARCHAR2(50) := '11993577_DBS_' || to_char(p_date, 'ddmmyyyy') || '_WT';
  BEGIN
    ins_file(l_file_name, 'get_settlement_report_header\n', get_settlement_report_body(p_date), 'get_settlement_footer');
  COMMIT;
  EXCEPTION
    WHEN OTHERS 
    THEN
      common.upd_error_table(SQLERRM, 'DailySettlement');
  END DailyBankingSummary;
  
END pkg_fss_settlement;