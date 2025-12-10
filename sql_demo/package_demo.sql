- Viết package với tên PKG_SALES_ANALYTICS
-- PACKAGE SPEC
CREATE OR REPLACE PACKAGE PKG_SALES_ANALYTICS AS
    -- Function tìm khách hàng mua nhiều nhất trong tháng
    FUNCTION FN_BEST_CUSTOMER RETURN VARCHAR2;
    -- Procedure in dữ liệu từ SALES_SMY theo tham số RPT_DT
    PROCEDURE PROD_SALES_SMY_BY_RPT (p_rpt_dt IN NUMBER);
END PKG_SALES_ANALYTICS;
-- PACKAGE BODY
CREATE OR REPLACE PACKAGE BODY PKG_SALES_ANALYTICS AS
    FUNCTION FN_BEST_CUSTOMER RETURN VARCHAR2 IS
        best_customer VARCHAR2(4000);
    BEGIN
        BEGIN
            SELECT LISTAGG(CUSTOMER_ID, ', ') WITHIN GROUP (ORDER BY CUSTOMER_ID)
            INTO best_customer
            FROM (
                SELECT CUSTOMER_ID,
                       RANK() OVER (ORDER BY COUNT(*) DESC) AS ranking
                FROM SALES
                WHERE TO_CHAR(DATE_ORDER,'YYYYMM') = TO_CHAR(SYSDATE,'YYYYMM')
                GROUP BY CUSTOMER_ID
            )
            WHERE ranking = 1;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                best_customer := 'No suitable customer';
        END;
        RETURN best_customer;
    END FN_BEST_CUSTOMER;

    PROCEDURE PROD_SALES_SMY_BY_RPT (p_rpt_dt IN NUMBER) IS
    BEGIN
        FOR rec IN (
            SELECT STT,
                   "THÁNG",
                   "NGÀY ĐẶT HÀNG",
                   "SẢN PHẨM BÁN RA",
                   "TĂNG_GIẢM"
            FROM SALES_SMY
            WHERE RPT_DT = p_rpt_dt
            ORDER BY STT
        ) LOOP
            DBMS_OUTPUT.PUT_LINE(
                'STT: ' || rec.STT ||
                ', THÁNG: ' || rec."THÁNG" ||
                ', NGÀY ĐẶT HÀNG: ' || TO_CHAR(rec."NGÀY ĐẶT HÀNG",'DD-MM-YYYY') ||
                ', SẢN PHẨM BÁN RA: ' || rec."SẢN PHẨM BÁN RA" ||
                ', TĂNG_GIẢM_: ' || NVL(TO_CHAR(rec."TĂNG_GIẢM"),'0')
            );
        END LOOP;
    END PROD_SALES_SMY_BY_RPT;
END PKG_SALES_ANALYTICS;

-- CHECK PROCEDURE PROD_SALES_SMY_BY_RPT trong PACKAGE
BEGIN
    PKG_SALES_ANALYTICS.PROD_SALES_SMY_BY_RPT(20250901);
END;
-- CHECK FUNCTION FN_BEST_CUSTOMER trong PACKAGE
BEGIN
    DBMS_OUTPUT.PUT_LINE(PKG_SALES_ANALYTICS.FN_BEST_CUSTOMER);
END;
