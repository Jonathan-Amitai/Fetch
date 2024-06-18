import parse_input
import query
import pandas as pd
from pandasql import sqldf
pd.set_option('display.max_columns', 40)
pd.set_option('display.width', 500)
pd.set_option('display.max_rows', 500)
pd.set_option('display.min_rows', 100)
if __name__ == '__main__':
    users_file = "users.json"
    brands_file = "brands.json"
    receipts_file = 'receipts.json'
    items = parse_input.generate_table("items")
    receipts = parse_input.generate_table("receipts")
    purchases = parse_input.generate_table("purchases")
    # pysqldf = lambda q: sqldf(q, globals())
    # q1 = query.get_join_tables_query("items", "receipts", "purchases")
    # joined_df = pysqldf(q1)
    # # print(joined_df)
    # q2 = query.get_bucket_by_month_query("joined_df")
    # bucketed_df = pysqldf(q2)
    # # print(bucketed_df)
    # q3 = query.get_quant_purchased_query("bucketed_df")
    # ranked_df = pysqldf(q3)
    # # print(ranked_df)
    # q4 = query.get_top_n_query("ranked_df", 5)
    # final_df = pysqldf(q4)
    # print(final_df)
    print(purchases[purchases["quantityPurchased"].isnull()])
