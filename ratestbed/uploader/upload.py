from ratestbed.uploader import get_conn


def upload_ratestbed_price(data_list):
    query = '''
        INSERT INTO ratestbed.product_daily (base_dt, product_name, account_name, std_pr, group)
        VALUES (:base_dt, :product_name, :account_name, :std_pr, :group)
        ON DUPLICATE KEY
            UPDATE std_pr           = VALUES(std_pr),
                   group            = VALUES(group),
                   updated_at       = now();
           '''
    with get_conn('local').transaction():
        get_conn('local').update(query, data_list)


def upload_ratestbed_info(data_list):
    query = '''
        INSERT INTO ratestbed.product_info (id, company_name, product_name, pass_num, mng_start_dt, disclosure_start_dt,
                                    account_type, account_name, mng_funds, description)
        VALUES (:id, :company_name, :product_name, :pass_num, :mng_start_dt, :disclosure_start_dt, :account_type, :account_name,
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
