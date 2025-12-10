-- Viết câu lệnh FUNCTION với tên FN_BEST_CUSTOMER tìm ra khách hàng có số
-- lượt mua hàng nhiều nhất của tháng
CREATE OR REPLACE FUNCTION FN_BEST_CUSTOMER_KHOI2k4
RETURN VARCHAR2 
AS 
    best_customer VARCHAR2(4000);
BEGIN 
    BEGIN 
        SELECT LISTAGG(CUSTOMER_ID, ',') WITHIN GROUP (ORDER BY CUSTOMER_ID)
        INTO best_customer
        FROM (
            SELECT CUSTOMER_ID,
                   RANK() OVER (ORDER BY COUNT(*) DESC) AS ranking
            FROM SALES_KHOI2k4
            WHERE TO_CHAR(DATE_ORDER,'YYYYMM') = TO_CHAR(SYSDATE,'YYYYMM')
            GROUP BY CUSTOMER_ID
        )
        WHERE ranking = 1;
        
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            best_customer := 'No suitable customer';
    END;

    RETURN best_customer;
END;