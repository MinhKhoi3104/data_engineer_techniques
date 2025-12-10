CREATE OR REPLACE PROCEDURE test_procedure
IS
BEGIN
    -- Tạo bảng nếu chưa tồn tại
    BEGIN
        EXECUTE IMMEDIATE 'CREATE TABLE SALES_SMY (
            STT NUMBER,
            "THÁNG" VARCHAR2(2),
            "NGÀY ĐẶT HÀNG" DATE,
            "SẢN PHẨM BÁN RA" VARCHAR2(4000),
            "TĂNG_GIẢM" NUMBER
        )';
    EXCEPTION
        WHEN OTHERS THEN
            IF SQLCODE = -955 THEN
                NULL;
            ELSE
                RAISE;
            END IF;
    END;
    
    INSERT INTO SALES_SMY (STT, "THÁNG", "NGÀY ĐẶT HÀNG", "SẢN PHẨM BÁN RA", "TĂNG_GIẢM")
    SELECT
        ROWNUM AS STT,
        "THÁNG",
        "NGÀY ĐẶT HÀNG",
        "SẢN PHẨM BÁN RA",
        total_quantity - LAG(total_quantity) OVER (ORDER BY "NGÀY ĐẶT HÀNG") AS "TĂNG_GIẢM"
    FROM (
        SELECT
            TO_CHAR(s.DATE_ORDER, 'MM') AS "THÁNG",
            s.DATE_ORDER AS "NGÀY ĐẶT HÀNG",
            LISTAGG(p.PROD_NAME, ', ') WITHIN GROUP (ORDER BY p.PROD_NAME) AS "SẢN PHẨM BÁN RA",
            SUM(s.quantity) AS total_quantity
        FROM SALES s
        LEFT JOIN PRODUCTS p ON s.PROD_ID = p.PROD_ID
        GROUP BY s.DATE_ORDER
    ) a;
    COMMIT;
END; 
-- call procedure
CALL test_procedure();