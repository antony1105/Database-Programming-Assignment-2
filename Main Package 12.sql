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

DECLARE
BEGIN
  pkg_fss_settlement.DailyBankingSummary(trunc(SYSDATE, 'DDD'));
END;
/

DECLARE
BEGIN
  pkg_fss_settlement.send_email('Testing1', 'Will this work' || CHR(13) || CHR(10) || 'Does this work', common.get_string_parameter('EMAIL_RECIPIENT', 'ASS2_RECIPIENT'), 'procedure@uts.edu.au');
END;
/

CREATE OR REPLACE PACKAGE pkg_fss_settlement
AS
  --PROCEDURE upd_fss_daily_transaction;
  --PROCEDURE ins_daily_settlement;
  PROCEDURE send_email(p_date DATE, p_subject VARCHAR2, p_message VARCHAR2);
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
  
  PROCEDURE ins_run_table(p_run_start TIMESTAMP, p_outcome VARCHAR2, p_err_message VARCHAR2 := NULL)
  IS
  BEGIN
    INSERT INTO fss_run_table
      (
        runId
        , runStart
        , runEnd
        , runOutcome
        , remarks
      )
    VALUES
      (
        seq_run_id.nextval
        , p_run_start
        , SYSTIMESTAMP
        , p_outcome
        , p_err_message
      );
  EXCEPTION
    WHEN OTHERS 
    THEN
      common.upd_error_table(SQLERRM, 'ins_run_table');
  END ins_run_table;

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
    l_last_day DATE := get_last_day_of_month;
  BEGIN
    IF trunc(SYSDATE, 'DDD') > l_last_day
    THEN
      UPDATE fss_daily_transactions
      SET settlementStatus = p_change_value
      WHERE trunc(downloadDate, 'DDD') <= l_last_day
      AND settlementStatus = p_settlement_status;
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
        INNER JOIN fss_terminal te
        ON dt.terminalId = te.terminalId
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
    l_word_length NUMBER := length(p_word);
    l_side_pixel NUMBER := floor((p_length - l_word_length) / 2);
    l_center VARCHAR2(1000) := lpad(p_word, l_word_length + l_side_pixel, p_indicator);
  BEGIN
    RETURN l_center;
  EXCEPTION
    WHEN OTHERS
    THEN
      common.upd_error_table(SQLERRM, 'get_centered_text');
  END get_centered_text;

  FUNCTION get_right_aligned(p_right_word VARCHAR2, p_left_word VARCHAR2, p_length NUMBER, p_indicator VARCHAR2)
  RETURN VARCHAR2
  IS
    l_left_word_length NUMBER := length(p_left_word);
    l_right_aligned VARCHAR2(1000) := lpad(p_right_word, p_length - l_left_word_length);
  BEGIN
    RETURN l_right_aligned;
  END get_right_aligned;

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

  FUNCTION get_formatted_number(p_number NUMBER, p_number_format VARCHAR2 := NULL)
  RETURN VARCHAR2
  IS
  BEGIN
    IF p_number IS NULL
    THEN
      RETURN ' ';
    ELSE
      RETURN to_char(p_number, p_number_format);
    END IF;
  EXCEPTION
    WHEN OTHERS
    THEN
      common.upd_error_table(SQLERRM, 'get_number_or_empty_space');
  END get_formatted_number;

  FUNCTION get_settlement_report_header(p_settlement_date DATE, p_length NUMBER)
  RETURN VARCHAR2
  IS
    l_report_name VARCHAR2(50) := 'Daily Banking Report \n';
    l_heading_1 VARCHAR2(50) := 'SMARTCARD SETTLEMENT SYSTEM';
    l_heading_2 VARCHAR2(50) := 'DAILY DESKBANK SUMMARY';
    l_indicator VARCHAR2(1) := ' ';
    l_indicator2 VARCHAR2(1) := '-';
    l_date VARCHAR2(20) := 'Date ' || to_char(p_settlement_date, 'dd-MON-yyyy');
    l_page VARCHAR2(20) := 'Page 1';
    l_column_headings VARCHAR2(200) := rpad('Merchant ID', 15, l_indicator) || rpad('Merchant Name', 30, l_indicator)
      || rpad('Account Number', 22, l_indicator) || rpad('Debit', 12, l_indicator) || 'Credit\n';
    l_divider VARCHAR2(200) := lpad(' ', 11, l_indicator2) || lpad(' ', 30, l_indicator2) || ' ' || lpad(' ', 23, l_indicator2)
      || lpad(' ', 11, l_indicator2) || ' ' || lpad(' ', 11, l_indicator2) || '\n';
  BEGIN
    RETURN l_report_name || get_centered_text(l_heading_1, p_length, l_indicator) || '\n' || get_centered_text(l_heading_2, p_length, l_indicator)
      || '\n' || l_date || get_right_aligned(l_page, l_date, p_length, l_indicator) || '\n \n' || l_column_headings || l_divider;
  EXCEPTION
    WHEN OTHERS
    THEN
      common.upd_error_table(SQLERRM, 'get_settlement_report_header');
  END get_settlement_report_header;

  FUNCTION get_settlement_report_body(p_settlement_date DATE)
  RETURN VARCHAR2
  IS
    l_lower_lodgement_ref NUMBER;
    l_higher_lodgement_ref NUMBER;
    l_text VARCHAR2(5000);
    l_merchant_id VARCHAR2(20);
    l_debit VARCHAR2(20);
    l_credit VARCHAR2(20);
    l_number_format VARCHAR2(20) := 'FM9999999999.90';
    l_indicator VARCHAR2(1) := ' ';
  BEGIN
    SELECT min(lodgementRef), max(lodgementRef)
    INTO l_lower_lodgement_ref, l_higher_lodgement_ref
    FROM fss_daily_settlement
    WHERE substr(lodgementRef, 1, 8) = to_char(p_settlement_date, 'yyyymmdd');

    FOR rec_settlements IN g_c_settlements(l_lower_lodgement_ref, l_higher_lodgement_ref)
    LOOP
      l_merchant_id := get_formatted_number(rec_settlements.merchantId);
      l_debit := get_formatted_number(rec_settlements.debit / 100, l_number_format);
      l_credit := get_formatted_number(rec_settlements.credit / 100, l_number_format);
      l_text := l_text || rpad(l_merchant_id, 12, l_indicator) || rpad(rec_settlements.merchantTitle, 32, l_indicator)
      || get_bsb_format(rec_settlements.merchantBsb) || rpad(rec_settlements.merchantAccNum, 11, l_indicator) || lpad(l_debit, 12, l_indicator)
      || lpad(l_credit, 13, l_indicator) || '\n';
    END LOOP;
    RETURN l_text;
  EXCEPTION
    WHEN OTHERS
    THEN
      common.upd_error_table(SQLERRM, 'get_settlement_report_body');
  END get_settlement_report_body;

  FUNCTION get_settlement_report_footer(p_settlement_date DATE, p_length NUMBER)
  RETURN VARCHAR2
  IS
    l_indicator VARCHAR2(1) := ' ';
    l_number_format VARCHAR2(20) := 'FM9999999999.90';
    --l_divider VARCHAR2(500) := get_right_aligned('------------ ------------', ' ', p_length + 1, l_indicator) || '\n';
    --l_total_balance VARCHAR2(10) := 'BALANCE TOTAL';
  BEGIN
    RETURN get_right_aligned('------------ ------------', ' ', p_length + 1, l_indicator) || '\n' || 'BALANCE TOTAL'
      || get_right_aligned(get_formatted_number(g_credit_sum / 100, l_number_format) || ' ' || get_formatted_number(g_credit_sum / 100, l_number_format)
      , 'BALANCE TOTAL', p_length, l_indicator) || '\n \n' || rpad('Deskbank file Name', 19, l_indicator) || ': DS_'
      || to_char(p_settlement_date, 'ddmmyyyy') || '_WT\n' || rpad('Dispatch Date', 19, l_indicator) || ': ' || to_char(p_settlement_date, 'dd MON yyyy');
  EXCEPTION
    WHEN OTHERS
    THEN
      common.upd_error_table(SQLERRM, 'get_settlement_report_footer');
  END get_settlement_report_footer;

  PROCEDURE ins_file(p_file_name VARCHAR2, p_content VARCHAR2)
  IS
    l_file  utl_file.file_type;
    l_directory VARCHAR2(20) := 'WT_11993577';
  BEGIN
    l_file := utl_file.fopen (l_directory,p_file_name, 'W');
    utl_file.putf(l_file, p_content);
    utl_file.fclose(l_file);
  EXCEPTION
    WHEN OTHERS
    THEN
      common.upd_error_table(SQLERRM, 'ins_file');
      utl_file.fclose(l_file);
  END ins_file;

  PROCEDURE settle_new_transactions;
  PROCEDURE create_deskbank_file;

  PROCEDURE DailySettlement
  IS
    l_run_start TIMESTAMP := SYSTIMESTAMP;
    l_last_successful_run_date VARCHAR2(50) := trunc(get_last_run_date('Success'), 'DDD');
  BEGIN
    IF l_last_successful_run_date <> trunc(SYSDATE, 'DDD')
    THEN
      settle_new_transactions;
      create_deskbank_file;
      DailyBankingSummary(trunc(SYSDATE, 'DDD'));
      ins_run_table(l_run_start, 'Success');
    END IF;
  COMMIT;
  EXCEPTION
    WHEN OTHERS
    THEN
      common.upd_error_table(SQLERRM, 'DailySettlement');
      ins_run_table(l_run_start, 'Failed', SQLERRM);
      ROLLBACK;
  END DailySettlement;

  PROCEDURE settle_new_transactions
  IS
  BEGIN
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
    --dbms_output.put_line('Check' || g_last_transaction_nr);
      upd_daily_transaction_settled(SYSTIMESTAMP, 'Checked');
  EXCEPTION
    WHEN OTHERS
    THEN
      common.upd_error_table(SQLERRM, 'settle_new_transactions');
  END settle_new_transactions;

  PROCEDURE create_deskbank_file
  IS
    l_date VARCHAR2(10) := to_char(SYSDATE, 'ddmmyyyy');
  BEGIN
    ins_file('DS_' || l_date || '_WT.dat', get_deskbank_header || get_deskbank_body || get_deskbank_footer);
  EXCEPTION
    WHEN OTHERS
    THEN
      common.upd_error_table(SQLERRM, 'create_deskbank_file');
  END create_deskbank_file;
  
  FUNCTION get_summary_report(p_date DATE)
  RETURN VARCHAR2
  IS
    l_body VARCHAR2(3000) := get_settlement_report_body(p_date);
    l_count NUMBER := regexp_count(l_body, '\n');
    l_length NUMBER := (length(l_body) - l_count * 2) / l_count;
  BEGIN
    RETURN get_settlement_report_header(p_date, l_length) || get_settlement_report_body(p_date) || get_settlement_report_footer(p_date, l_length);
  END get_summary_report;

  PROCEDURE DailyBankingSummary(p_date DATE)
  IS
    l_file_name VARCHAR2(100) := '11993577_DBS_' || to_char(p_date, 'ddmmyyyy') || '_WT';
    l_subject VARCHAR2(100) := 'Daily Backing Summary Report ' || to_char(p_date, 'dd/mm/yyyy');
    l_message VARCHAR2(500) := 'Sent From the OMS Database by the PL/SQL application\n'
      || 'The Daily Banking Summary Report ' || to_char(p_date, 'dd/mm/yyyy') || ' is in the attached file\n\n'
      || 'Regards\n'
      || 'The OMS Database\n\n'
      || 'This is an automatically generated email so please do not reply\n\n';
  BEGIN
    ins_file(l_file_name, get_summary_report(p_date));
    send_email(p_date, l_subject , l_message);
  COMMIT;
  EXCEPTION
    WHEN OTHERS
    THEN
      common.upd_error_table(SQLERRM, 'DailyBankingSummary');
  END DailyBankingSummary;
  
  PROCEDURE send_email(p_date DATE, p_subject VARCHAR2, p_message VARCHAR2)
  IS
    --p_subject VARCHAR2(50) := 'This is the subject';
    --p_message VARCHAR2(50) := 'Please give me money\n Wira is cool';
    l_recipient VARCHAR2(50) := common.get_string_parameter('EMAIL_RECIPIENT', 'ASS2_RECIPIENT');
    l_sender VARCHAR2(50) := common.get_string_parameter('EMAIL_SENDER', 'ASS2_SENDER');
    con_nl VARCHAR2(2) := CHR(13) || CHR(10);
    l_message VARCHAR2(1000) := regexp_replace(p_message, '\\n', con_nl);
    l_content VARCHAR2(5000) := regexp_replace(get_summary_report(p_date), '\\n', con_nl);
    l_mailhost VARCHAR2(50) := 'postoffice.uts.edu.au';
    mail_conn UTL_SMTP.connection;
    l_proc_name VARCHAR2(50) := 'send_email';
    l_recipient_list VARCHAR2(2000);
    --l_recipient VARCHAR2(80);
    l_counter NUMBER := 0;
    con_email_footer VARCHAR2(250) := 'This is the email footer';
    --
    --
    --
  BEGIN
    dbms_output.put_line(l_content);
    --     v_recipient_list := REPLACE(p_recipient, ' ');  --get rid of any spaces so that it's easier to split up
    mail_conn := UTL_SMTP.open_connection (l_mailhost, 25);
    UTL_SMTP.helo (mail_conn, l_mailhost);
    UTL_SMTP.mail (mail_conn, l_sender);
    -- 
    
    UTL_SMTP.rcpt(mail_conn, l_recipient);
    UTL_SMTP.open_data(mail_conn);
    UTL_SMTP.write_data(mail_conn,'From' || ':' || l_sender || con_nl);
    UTL_SMTP.write_data(mail_conn,'To'|| ':'|| l_recipient || con_nl);
    UTL_SMTP.write_data(mail_conn,'Subject'|| ':'|| p_subject || con_nl);
    --UTL_SMTP.write_data(mail_conn, con_nl || get_smtp_stream(mail_conn, 'Deskbank.txt', 'Test 1 2 3', p_message) || con_nl);
    utl_smtp.write_data(mail_conn, 'Mime-Version: 1.0' || con_nl);
    utl_smtp.write_data(mail_conn, 'Content-Type: multipart/mixed; boundary="Lauries Boundary"' || con_nl || con_nl); 
    
    utl_smtp.write_data(mail_conn, '--Lauries Boundary' || con_nl);
      
    utl_smtp.write_data(mail_conn, 'Content-type: text/plain; charset=us-ascii' || con_nl || con_nl);
      
    utl_smtp.write_data(mail_conn, l_message || con_nl || con_nl);
       
    utl_smtp.write_data(mail_conn, '--Lauries Boundary' || con_nl); 
      
    utl_smtp.write_data(mail_conn, 'Content-Type: application/octet-stream; name="' || 'Testing.txt' || '"' || con_nl); 
    utl_smtp.write_data(mail_conn, 'Content-Transfer-Encoding: 7bit' || con_nl || con_nl);
      
    utl_smtp.write_data(mail_conn, l_content || con_nl || con_nl); 
      
    utl_smtp.write_data(mail_conn, '--Lauries Boundary--' || con_nl);
    UTL_SMTP.write_data(mail_conn, con_nl || con_email_footer || con_nl);
    UTL_SMTP.close_data(mail_conn);
    UTL_SMTP.quit(mail_conn);
  EXCEPTION
    WHEN OTHERS THEN
      common.upd_error_table(SQLERRM, 'send_email');
      UTL_SMTP.close_data(mail_conn);
  END send_email;

END pkg_fss_settlement;
