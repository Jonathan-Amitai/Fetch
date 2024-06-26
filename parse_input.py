"""Parses input from the three json files, converts them into six different tables: users, brands, items, rewards,
purchases, and receipts. A detailed Entity-Relationship Diagram can be found in the Github repository"""

import json
import numpy as np
import pandas as pd


def parse_file(file_path):
    # Cleans the input file a bit so it is in proper json format
    with open(file_path) as fp:
        out = fp.read()
        receipts = out.split("\n")[:-1]
        good_json = "[" + ",".join(receipts) + "]"
        output = json.loads(good_json)
        return output


def convert_date_col(df, col_name, output_col_name=None):
    # converts date columns from epoch time to YYYY-MM-DD HH:MM:SS
    df[col_name] = pd.to_datetime(df[col_name], unit='ms', origin='unix')
    if output_col_name is not None:
        df.rename(columns={col_name: output_col_name}, inplace=True)


def get_users_table(input_json):
    # parses users.json to create the users table
    output_df = pd.json_normalize(input_json, sep="_")
    output_df.rename(columns={"_id_$oid": "userId"}, inplace=True)
    convert_date_col(output_df, "createdDate_$date", output_col_name="dateCreated")
    convert_date_col(output_df, "lastLogin_$date", output_col_name="lastLogin")
    return output_df


def get_brands_table(input_json):
    # parses brands.json to create the brands table
    col_names = ["brand_id", "brand_name", "top_brand", "cpg_ref", "cpg_id"]
    output_df = pd.json_normalize(input_json, sep="_")
    output_df.rename(columns={"_id_$oid": "brandId", "cpg_$id_$oid": "cpg_id", "cpg_$ref": "cpg_ref"}, inplace=True)
    return output_df


def add_ror_cols(purchase_list, rest_of_receipt, col_mapping):
    """Utility function to help parse receipts.json. For each table, after the relevant columns from the
     rewardsReceiptItemList key are selected, the col_mapping dictionary  determines what other columns to add
     (and their output names) for each table that comes from the receipts.json file"""
    for k, v in col_mapping.items():
        purchase_list[k] = rest_of_receipt.get(v)
    return purchase_list


def get_formatting_info(table_name):
    """Returns a list of columns that need to have their date formatted, the column mapping dict to determine what
    columns need to be added to each table, and the final output columns for each table."""
    match table_name.lower():
        case "purchases":
            date_format_cols = ["purchaseDate"]
            col_mapping = {"userId": "userId", "receiptId": "_id.$oid", "purchaseDate": "purchaseDate.$date",
                           "bonusPointsEarned": "bonusPointsEarned", "bonusPointsEarnedReason": "bonusPointsEarnedReason"}
            final_output_cols = ["barcode", "userId", "receiptId", "purchaseDate", "finalPrice", "itemPrice",
                                 "needsFetchReview", "partnerItemId", "preventTargetGapPoints",
                 "quantityPurchased", "userFlaggedBarcode", "userFlaggedNewItem", "userFlaggedPrice",
                 "userFlaggedQuantity", "needsFetchReviewReason", "pointsNotAwardedReason", "userFlaggedDescription",
                 "originalMetaBriteBarcode", "originalMetaBriteDescription", "discountedItemPrice",
                 "originalMetaBriteQuantityPurchased", "pointsEarned", "targetPrice", "originalFinalPrice",
                 "originalMetaBriteItemPrice", "priceAfterCoupon"]
            return date_format_cols, col_mapping, final_output_cols
        case "receipts":
            date_format_cols = ["purchaseDate", "createdDate", "finishedDate", "dateScanned", "modifyDate",
                                "pointsAwardedDate"]
            col_mapping = {"receiptId": "_id.$oid", "userId": "userId", "purchaseDate": "purchaseDate.$date",
                           "createdDate": "createDate.$date", "finishedDate": "finishedDate.$date",
                           "dateScanned": "dateScanned.$date", "modifyDate": "modifyDate.$date",
                           "pointsAwardedDate": "pointsAwardedDate.$date", "totalSpent": "totalSpent",
                           "pointsEarned": "pointsEarned", "bonusPointsEarned": "bonusPointsEarned",
                           "bonusPointsEarnedReason": "bonusPointsEarnedReason", "purchasedItemCount": "purchasedItemCount",
                           "rewardsReceiptStatus": "rewardsReceiptStatus"}
            final_output_cols = ["receiptId", "userId", "purchaseDate", "createdDate", "finishedDate", "dateScanned",
                                 "modifyDate", "pointsAwardedDate", "totalSpent", "pointsEarned", "bonusPointsEarned",
                                 "purchasedItemCount", "rewardsReceiptStatus"]
            return date_format_cols, col_mapping, final_output_cols
        case "items":
            data_format_cols = []
            col_mapping = {}
            final_output_cols = ["barcode", "description", "brandCode", "originalReceiptItemText",
                                 "itemNumber", "competitiveProduct", "metabriteCampaignId"]
            return data_format_cols, col_mapping, final_output_cols
        case "rewards":
            data_format_cols = []
            col_mapping = {"userId": "userId", "receiptId": "_id.$oid"}
            final_output_cols = ["barcode", "receiptId", "pointsPayerId", "rewardsGroup",
                                 "rewardsProductPartnerId", "competitorRewardsGroup"]
            return data_format_cols, col_mapping, final_output_cols
        case _:
            raise ValueError("Table name is invalid")


def parse_receipts_file(input_json, date_format_cols, col_mapping, final_output_cols):
    # function to parse receipts.json in such a way that the output can be used to generate each individual table.
    output_df = pd.DataFrame()
    # cols = set({})
    for purchase in input_json:
        purchase_list = pd.DataFrame()
        receipt = purchase.get("rewardsReceiptItemList")
        if receipt is not None:
            purchase_list = pd.json_normalize(receipt)
        # json.normalize can't be called when one of the values is an array, the data in there has already
        # been stored and is no longer needed. The key is not deleted since the same json needs to be there for
        # parsing multiple tables
        json_f_dict = {k: v for k, v in purchase.items() if k != "rewardsReceiptItemList"}
        rest_of_receipt = pd.json_normalize(json_f_dict).iloc[0]
        # add columns not from rewardsReceiptItemList
        purchase_list = add_ror_cols(purchase_list, rest_of_receipt, col_mapping)
        output_df = pd.concat([output_df, purchase_list], ignore_index=True)
    #convert any date columns that need to be converted
    for col in date_format_cols:
        convert_date_col(output_df, col)
    output_df = output_df[final_output_cols]
    # as a result of these operations, some rows will be duplicates of each other, and some will contain all null
    # values. These need to be removed
    output_df.replace({pd.NaT: None, np.nan: None}, inplace=True)
    output_df.drop_duplicates(inplace=True)
    output_df.dropna(axis="rows", how="all", inplace=True)
    output_df.reset_index(drop=True, inplace=True)
    return output_df


def generate_table(table_name):
    # driver function that actually parses the json and generates the table that corresponds to the input table_name.
    match table_name.lower():
        case "users":
            input_json = parse_file("users.json")
            return get_users_table(input_json)
        case "brands":
            input_json = parse_file("brands.json")
            return get_brands_table(input_json)
        case "items" | "rewards" | "receipts" | "purchases":
            pass
        case _:
            raise ValueError("Table name entered is invalid")
    input_json = parse_file("receipts.json")
    date_format_cols, col_mapping, final_output_cols = get_formatting_info(table_name)
    output_table = parse_receipts_file(input_json, date_format_cols, col_mapping, final_output_cols)
    return output_table



