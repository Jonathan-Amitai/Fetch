import parse_input
import pandas as pd

pd.set_option('display.max_columns', 40)
pd.set_option('display.width', 500)
pd.set_option('display.max_rows', 500)
pd.set_option('display.min_rows', 100)
if __name__ == '__main__':
    users = "users.json"
    brands = "brands.json"
    receipts = 'receipts.json'
    # json = parse_input.parse_file(users)
    # df = parse_input.get_users_table(json)
    # json = parse_input.parse_file(brands)
    # df = parse_input.get_brands_table(json)
    json = parse_input.parse_file(receipts)
    df = parse_input.get_purchases_table(json)
    # json = parse_input.parse_file(receipts)
    # df = parse_input.get_items_table(json)
    # json = parse_input.parse_file(receipts)
    # df = parse_input.get_rewards_table(json)
    # json = parse_input.parse_file(receipts)
    # df = parse_input.get_receipts_table(json)
    print(df)
    # print(df.loc[df["barcode"] == "034100573065"])
# See PyCharm help at https://www.jetbrains.com/help/pycharm/
