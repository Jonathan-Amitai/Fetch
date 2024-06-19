import pandas
from sqlalchemy import inspect
import sqlparse
import parse_input


def write_output(engine):
    # These are the tables necessary to calculate the top 5 brands by quantity purchased by month
    for table_name in ["items", "receipts", "purchases"]:
        does_table_exist = inspect(engine).has_table(table_name)
        if not does_table_exist:
            df = parse_input.generate_table(table_name)
            df.to_sql(name=table_name, con=engine)


def get_join_tables_query():
    q = f"""SELECT a.receiptId, a.userId, a.dateScanned, b.barcode, b.quantityPurchased, c.brandCode FROM 
        receipts AS a
        INNER JOIN purchases AS b 
        ON a.receiptId = b.receiptId
        AND a.purchaseDate = b.purchaseDate
        AND a.userId = b.userId
        INNER JOIN items AS c
        ON b.barcode = c.barcode"""
    return q

def get_bucket_by_month_query(q1):
    q2 = f"""SELECT *,
    CASE
        WHEN dateScanned > '2021-01-01' AND dateScanned <= '2021-02-01' THEN '01-2021'
        WHEN dateScanned > '2021-02-01' AND dateScanned <= '2021-03-01' THEN '02-2021'
        ELSE 'N/A'
    END AS month
    FROM ({q1})"""
    return q2

def get_quant_purchased_query(q2):
    q3 = f"""SELECT *, RANK() OVER(PARTITION BY month ORDER BY monthly_purchase_cnt Desc) AS rank FROM 
    (SELECT brandCode, month, sum(quantityPurchased) AS monthly_purchase_cnt FROM ({q2})
    GROUP BY brandCode, month
    ORDER BY month)
    WHERE month <> 'N/A'
    """
    return q3


def get_top_n_query(q3, n):
    q4 = f"""SELECT brandCode, month, monthly_purchase_cnt FROM ({q3})
        WHERE rank <= {n}"""
    return q4

def get_full_top_n_query(n):
    # Query to find the top n brands by quantity purchased. It buckets each record into an individual months, and
    # aggregates all of the purchase records.
    q1 = get_join_tables_query()
    q2 = get_bucket_by_month_query(q1)
    q3 = get_quant_purchased_query(q2)
    q4 = get_top_n_query(q3, n)
    return q4


def get_top_n_brands(engine, n):
    # driver function that actually runs the query and produces the output
    with engine.connect() as connection:
        top_n_query = get_full_top_n_query(n)
        top_n_query = sqlparse.format(top_n_query, reindent=True, keyword_case='upper')
        result = pandas.read_sql(top_n_query, connection)
        print(f"Query to get the top brands by quantity purchased for the past two months:\n{top_n_query}\n")
        return result
