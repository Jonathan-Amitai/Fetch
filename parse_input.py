import json

import numpy as np
import pandas as pd
from time import strftime
def parse_file(file_path):
    with open(file_path) as fp:
        out = fp.read()
        receipts = out.split("\n")[:-1]
        good_json = "[" + ",".join(receipts) + "]"
        output = json.loads(good_json)
        return output


def clean_user(user):
    for k, v in user.items():
        if isinstance(v, dict): #only happens when there's exactly one kv pair
            value = None
            if "$oid" in v:
                value = v["$oid"]
            elif "$date" in v:
                value = v["$date"]
            user[k] = value #change outer key from dict to just the one value
    return user


def convert_date_col(df, col_name, output_col_name=None):
    df[col_name] = pd.to_datetime(df[col_name], unit='ms', origin='unix')
    if output_col_name is not None:
        df.rename(columns={col_name: output_col_name}, inplace=True)


def get_users_table(json):
    col_names = ["user_id", "active", "date_created", "last_login", "role", "sign_up_source", "state"]
    # output_json = []
    # for user in json:
    #     # user = clean_user(user)
    #     output_json.append(user)
    output_df = pd.json_normalize(json, sep="_")
    # output_df.columns = col_names
    output_df.rename(columns={"_id_$oid": "userId"}, inplace=True)
    convert_date_col(output_df, "createdDate_$date", output_col_name="dateCreated")
    convert_date_col(output_df, "lastLogin_$date", output_col_name="lastLogin")
    return output_df


def get_brands_table(json):
    col_names = ["brand_id", "brand_name", "top_brand", "cpg_ref", "cpg_id"]
    # output_json = []
    # for brand in json:
    #     brand = clean_user(brand)
    #     output_json.append(bra)
    output_df = pd.json_normalize(json, sep="_")
    output_df.rename(columns={"_id_$oid": "brandId", "cpg_$id_$oid": "cpg_id", "cpg_$ref": "cpg_ref"}, inplace=True)
    print(output_df)


def get_purchases_table_test(json):
    col_names = ["barcode", "finalPrice", "itemPrice", "needsFetchReview", "preventTargetGapPoints", "quantityPurchased", "userFlaggedNewItem"]
    output_df = pd.DataFrame()
    for purchase in json:
        # print(purchase)
        if "rewardsReceiptItemList" in purchase:
            purchase_val = purchase["rewardsReceiptItemList"]
            purchase_list = pd.json_normalize(purchase_val)
            output_df = pd.concat([output_df, purchase_list], ignore_index=True)
    #         cols = list(output_df.columns)
    #         for col in cols:
    #             our_set.add(col)
    # print(our_set)
    return output_df


def get_items_table(json):
    output_cols = ["barcode", "description", "brandCode", "originalReceiptItemText", "itemNumber", "competitiveProduct", "metabriteCampaignId"]
    output_df = pd.DataFrame()
    for purchase in json:
        # print(purchase)
        if "rewardsReceiptItemList" in purchase:
            purchase_val = purchase["rewardsReceiptItemList"]
            purchase_list = pd.json_normalize(purchase_val)
            output_df = pd.concat([output_df, purchase_list], ignore_index=True)
    output_df = output_df[output_cols]
    output_df.drop_duplicates(inplace=True)
    output_df.dropna(axis="rows", how="all", inplace=True)
    output_df.reset_index(drop=True, inplace=True)
    return output_df


def get_purchases_table(json):
    col_names = ["finalPrice", "itemPrice", "needsFetchReview", "partnerItemId", "preventTargetGapPoints",
                 "quantityPurchased", "userFlaggedBarcode", "userFlaggedNewItem", "userFlaggedPrice",
                 "userFlaggedQuantity", "needsFetchReviewReason", "pointsNotAwardedReason", "userFlaggedDescription",
                 "originalMetaBriteBarcode", "originalMetaBriteDescription", "discountedItemPrice",
                 "originalMetaBriteQuantityPurchased", "pointsEarned", "targetPrice", "originalFinalPrice",
                 "originalMetaBriteItemPrice", "priceAfterCoupon"]
    output_df = pd.DataFrame()
    for purchase in json:
        # print(f"The purchase is {purchase}")
        purchase_list = pd.Series()
        if "rewardsReceiptItemList" in purchase:
            purchase_val = purchase["rewardsReceiptItemList"]
            purchase_list = pd.json_normalize(purchase_val)
            # print(purchase_list)
            del purchase["rewardsReceiptItemList"]
        rest_of_receipt = pd.json_normalize(purchase).iloc[0]
        # purchase_list = pd.concat([purchase_list, rest_of_receipt], axis=1)
        # print(pu)
        purchase_list["userId"] = rest_of_receipt.get("userId")
        purchase_list["receiptId"] = rest_of_receipt.get("_id.$oid")
        purchase_list["purchaseDate"] = rest_of_receipt.get("purchaseDate.$date")
        # print(f"After adding all other cols: {purchase_list}")
        output_df = pd.concat([output_df, purchase_list], ignore_index=True)
    convert_date_col(output_df, "purchaseDate")
    output_df = output_df[["barcode", "userId", "receiptId", "purchaseDate"] + col_names]
    output_df.replace({pd.NaT: None, np.nan: None}, inplace=True)
    return output_df


def get_rewards_table(json):
    col_names = ["pointsPayerId", "rewardsGroup", "rewardsProductPartnerId", "competitorRewardsGroup"]
    output_df = pd.DataFrame()
    for purchase in json:
        # print(f"The purchase is {purchase}")
        purchase_list = pd.Series()
        if "rewardsReceiptItemList" in purchase:
            purchase_val = purchase["rewardsReceiptItemList"]
            purchase_list = pd.json_normalize(purchase_val)
            # print(purchase_list)
            del purchase["rewardsReceiptItemList"]
        rest_of_receipt = pd.json_normalize(purchase).iloc[0]
        # purchase_list = pd.concat([purchase_list, rest_of_receipt], axis=1)
        # print(pu)
        purchase_list["userId"] = rest_of_receipt.get("userId")
        purchase_list["receiptId"] = rest_of_receipt.get("_id.$oid")
        # print(f"After adding all other cols: {purchase_list}")
        output_df = pd.concat([output_df, purchase_list], ignore_index=True)
    output_df = output_df[["barcode"] + col_names]
    output_df.replace({pd.NaT: None, np.nan: None}, inplace=True)
    output_df.drop_duplicates(inplace=True)
    output_df.dropna(axis="rows", how="all", inplace=True)
    output_df = output_df.loc[output_df["barcode"] != "4011"]
    output_df.reset_index(drop=True, inplace=True)
    return output_df


def get_receipts_table(json):
    output_df = pd.DataFrame()
    # cols = set({})
    for purchase in json:
        # print(f"The purchase is {purchase}")
        purchase_list = pd.DataFrame()
        if "rewardsReceiptItemList" in purchase:
            purchase_val = purchase["rewardsReceiptItemList"]
            purchase_list = pd.json_normalize(purchase_val)
            # print(purchase_list)
            del purchase["rewardsReceiptItemList"]
        rest_of_receipt = pd.json_normalize(purchase).iloc[0]
        # print(rest_of_receipt)
        # for col in list(rest_of_receipt.index):
        #     cols.add(col)
        # purchase_list = pd.concat([purchase_list, rest_of_receipt], axis=1)
        purchase_list["receiptId"] = rest_of_receipt.get("_id.$oid")
        purchase_list["userId"] = rest_of_receipt.get("userId")
        purchase_list["purchaseDate"] = rest_of_receipt.get("purchaseDate.$date")
        purchase_list["createdDate"] = rest_of_receipt.get("createDate.$date")
        purchase_list["finishedDate"] = rest_of_receipt.get("finishedDate.$date")
        purchase_list["dateScanned"] = rest_of_receipt.get("dateScanned.$date")
        purchase_list["modifyDate"] = rest_of_receipt.get("modifyDate.$date")
        purchase_list["pointsAwardedDate"] = rest_of_receipt.get("pointsAwardedDate.$date")
        purchase_list["totalSpent"] = rest_of_receipt.get("totalSpent")
        purchase_list["pointsEarned"] = rest_of_receipt.get("pointsEarned")
        purchase_list["bonusPointsEarned"] = rest_of_receipt.get("bonusPointsEarned")
        purchase_list["bonusPointsEarnedReason"] = rest_of_receipt.get("bonusPointsEarnedReason")
        purchase_list["purchasedItemCount"] = rest_of_receipt.get("purchasedItemCount")
        purchase_list["rewardsReceiptStatus"] = rest_of_receipt.get("rewardsReceiptStatus")
        # print(f"After adding all other cols: {purchase_list}")
        output_df = pd.concat([output_df, purchase_list], ignore_index=True)
    convert_date_col(output_df, "purchaseDate")
    convert_date_col(output_df, "createdDate")
    convert_date_col(output_df, "finishedDate")
    convert_date_col(output_df, "dateScanned")
    convert_date_col(output_df, "modifyDate")
    convert_date_col(output_df, "pointsAwardedDate")
    output_df = output_df[["receiptId", "userId", "purchaseDate", "createdDate", "finishedDate", "dateScanned",
                           "modifyDate", "pointsAwardedDate", "totalSpent", "pointsEarned", "bonusPointsEarned",
                           "purchasedItemCount", "rewardsReceiptStatus"]]
    output_df.replace({pd.NaT: None, np.nan: None}, inplace=True)
    output_df.drop_duplicates(inplace=True)
    output_df.dropna(axis="rows", how="all", inplace=True)
    output_df.reset_index(drop=True, inplace=True)
    return output_df
