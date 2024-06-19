import data_analysis
import pandas as pd
from sqlalchemy import create_engine
pd.set_option('display.max_columns', 40)
pd.set_option('display.width', 300)
pd.set_option('display.max_rows', 500)
pd.set_option('display.min_rows', 100)
if __name__ == '__main__':
    users_file = "users.json"
    brands_file = "brands.json"
    receipts_file = 'receipts.json'
    top_n = 5
    engine = create_engine("sqlite://")
    data_analysis.write_output(engine)
    top_n_brands = data_analysis.get_top_n_brands(engine, top_n)
    print(f"Top {top_n} brands by monthly purchase count:")
    print(top_n_brands)
    print("\n")
    with engine.connect() as connection:
        q = """WITH month_year AS (SELECT barcode, userId, purchaseDate, strftime('%Y', purchaseDate) AS year,
         strftime('%m', purchaseDate) AS month FROM purchases) 
          SELECT a.month, a.year, a.cnt_barcode, b.cnt_no_barcode FROM (SELECT month, year, count(*) AS cnt_barcode FROM month_year
          WHERE barcode IS NOT NULL
          GROUP BY 1, 2) AS a
          INNER JOIN
          (SELECT month, year, count(*) AS cnt_no_barcode FROM month_year
          WHERE barcode IS NULL
          GROUP BY 1, 2) AS b
          ON a.month = b.month
          AND a.year = b.year
          """
        barcode_cnt = pd.read_sql(q, connection)
        print("Purchase record counts with and without barcodes across different months:")
        print(barcode_cnt)

