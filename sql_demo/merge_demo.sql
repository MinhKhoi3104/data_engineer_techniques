MERGE INTO cur.customer AS tgt
USING dmt.customer AS src
    on tgt.customer_id = src.customer_id
WHEN MATCHED THEN 
    UPDATE SET
        tgt.customer_name = src.customer_name,
        tgt.customer_birth = src.customer_birth,
        tgt.customer_hometown = src.customer_hometown,
        tgt.customer_phone_num = src.customer_phone_num,
        tgt.customer_email = src.customer_email,
        tgt.customer_source = src.customer_source,
        tgt.customer_gender = src.customer_gender,
        tgt.is_active = src.is_active,
        tgt.etl_date = src.etl_date
WHEN NOT MATCHED THEN
    INSERT (tgt.customer_id, tgt.customer_name, tgt.customer_birth, tgt.customer_hometown, tgt.customer_phone_num, 
        tgt.customer_email, tgt.customer_source, tgt.customer_gender, tgt.is_active, tgt.etl_date) 
    VALUES (src.customer_id, src.customer_name, src.customer_birth, src.customer_hometown, src.customer_phone_num, 
        src.customer_email, src.customer_source, src.customer_gender, src.is_active,src.etl_date)