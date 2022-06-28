from ratestbed.uploader import get_conn


def upload_ratestbed_price(data_list):
    query = '''
        INSERT INTO ratestbed.product_daily (base_dt, product_name, account_name, std_pr, group)
        VALUES (:base_dt, :product_name, :account_name, :std_pr, :group)
        ON DUPLICATE KEY
            UPDATE std_pr             = VALUES(std_pr),
                   group              = VALUES(group),
                   updated_at         = now();
           '''
    with get_conn('local').transaction():
        get_conn('local').update(query, data_list)
