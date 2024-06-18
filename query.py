

def get_join_tables_query(i_table_name, r_table_name, p_table_name):
    q = f"""SELECT a.receiptId, a.userId, a.dateScanned, b.barcode, b.quantityPurchased, c.brandCode FROM 
        {r_table_name} AS a
        INNER JOIN {p_table_name} AS b 
        ON a.receiptId = b.receiptId
        AND a.purchaseDate = b.purchaseDate
        AND a.userId = b.userId
        INNER JOIN {i_table_name} AS c
        ON b.barcode = c.barcode"""
    return q

def get_bucket_by_month_query(input_table_name):
    q2 = f"""SELECT *,
    CASE
        WHEN dateScanned > '2021-01-01' AND dateScanned <= '2021-02-01' THEN '01-2021'
        WHEN dateScanned > '2021-02-01' AND dateScanned <= '2021-03-01' THEN '02-2021'
        ELSE 'N/A'
    END AS month
    FROM {input_table_name}"""
    return q2

def get_quant_purchased_query(input_table_name):
    q3 = f"""SELECT *, RANK() OVER(PARTITION BY month ORDER BY monthly_purchase_cnt Desc) AS rank FROM (SELECT brandCode, month, sum(quantityPurchased) AS monthly_purchase_cnt FROM {input_table_name}
    GROUP BY brandCode, month
    ORDER BY month)
    WHERE month <> 'N/A'
    """
    return q3

def get_top_n_query(input_table_name, n):
    q4 = f"""SELECT brandCode, month, monthly_purchase_cnt FROM {input_table_name}
        WHERE rank <= {n}"""
    return q4