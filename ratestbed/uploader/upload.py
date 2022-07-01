from ratestbed.uploader import get_conn


def upload_ratestbed_price(data_list):
    query = '''
        INSERT INTO ratestbed.product_daily (algo_id, base_dt, std_pr)
        VALUES (:algo_id, :base_dt, :std_pr)
        ON DUPLICATE KEY
            UPDATE std_pr           = VALUES(std_pr),
                   updated_at       = now();
           '''
    with get_conn('local').transaction():
        get_conn('local').update(query, data_list)


def upload_ratestbed_info(data_list):
    query = '''
        INSERT INTO ratestbed.product_info (algo_id, company_name, algo_name, pass_num, mng_start_dt, disclosure_start_dt,
                                    account_type, account_name, mng_funds, description)
        VALUES (:algo_id, :company_name, :algo_name, :pass_num, :mng_start_dt, :disclosure_start_dt, :account_type, :account_name,
                :mng_funds, :description)
        ON DUPLICATE KEY
            UPDATE company_name        = VALUES(company_name),
                   pass_num            = VALUES(pass_num),
                   mng_start_dt        = VALUES(mng_start_dt),
                   disclosure_start_dt = VALUES(disclosure_start_dt),
                   account_type        = VALUES(account_type),
                   mng_funds           = VALUES(mng_funds),
                   description         = VALUES(description),
                   updated_at          = now();
           '''
    with get_conn('local').transaction():
        get_conn('local').update(query, data_list)
