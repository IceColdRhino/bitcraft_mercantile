from datetime import (datetime, timezone)
from dotenv import load_dotenv
import gspread
import logging
import numpy as np
import os
import pandas as pd
import requests
import sys
import time

def main():
    # Determine start time
    t_start = datetime.now(timezone.utc)
    logging.info(f"Started at {t_start.strftime('%Y-%m-%d %H:%M %Z')}")

    # Load environment variables
    gc = gspread.oauth()
    load_dotenv()
    claim_url = os.getenv("CLAIM_URL")
    throttle_rate = os.getenv("THROTTLE_RATE")
    spreadsheet_name = os.getenv("SPREADSHEET_NAME")

    if claim_url:
        claim_id = claim_url.split("claims/")[1]
    else:
        logging.error("No CLAIM_URL entry found in .env file.")
        return

    if throttle_rate:
        throttle_rate = float(throttle_rate)
    else:
        logging.error("No THROTTLE_RATE entry found in .env file.")
        return
    
    if not spreadsheet_name:
        logging.error("No SPREADSHEET_NAME entry found in .env file.")
        return
    
    # Initialize Bitjita API client
    data_client = bitjita_client()

    # Grab data of the primary claim of interest
    logging.info("Querying Bitjita for info on specified claim.")
    primary_data = data_client._make_request(f"claims/{claim_id}").get("claim",{})
    primary_name = primary_data.get("name","Unknown")
    primary_region = primary_data.get("regionName","Unknown")
    X0 = primary_data.get("locationX",0)
    Z0 = primary_data.get("locationZ",0)
    logging.info(f"Claim of interested identified as: {primary_name} of the {primary_region} region.")

    # Grab a list of all the items present on the given market
    item_list = data_client._make_request(f"market?hasOrders=true&claimEntityId={claim_id}")
    item_list = item_list.get("data",{})
    item_list = item_list.get("items",[])
    total_N = len(item_list)
    logging.info(f"{total_N:,} items identified on the given market.")

    # Columns for first-pass trimmming of market data
    market_cols_0 = [
        "itemName",
        "itemTypeStr",
        "itemId",
        "itemType",
        "itemVolume",
        "priceThreshold",
        "quantity",
        "claimEntityId",
        "claimName",
        "regionName",
    ]

    logging.info("Beginning deeper analysis of each individual item on the market.")
    logging.info(f"Pausing {throttle_rate} [s] between each item query out of courtesy to Bitjita.")
    logging.info(f"Earliest possible completion in {np.round(total_N*throttle_rate/60,2)} [min]. (Likely longer due to processing time)")
    full_df = pd.DataFrame(data=None)
    # Iterate across all items with orders
    # TODO: Return to normal loop instead of truncation
    for i in range(0,total_N):
        #for i in range(0,50):
        if i%10 == 0:
            logging.info(f"{np.round(100*i/total_N,2)} % Complete: {i:,}/{total_N:,} Items")
        item = item_list[i]

        # Throttling at the user-defined rate
        time.sleep(throttle_rate)

        # Convert item type integer to string
        item_name = item.get("name","Unknown")
        item_type = item.get("itemType",-1)
        item_id = item.get("id",0)
        item_volume = item.get("volume",0)
        if item_type == 0:
            item_type_str = "item"
        elif item_type == 1:
            item_type_str = "cargo"
        else:
            # Skip items with unknown item types
            continue
        
        # Get data on all the trade orders worldwide for the given item
        try:
            item_market = data_client._make_request(f"market/{item_type_str}/{item_id}")
        except:
            logging.error(f"Unable to complete query for {item_type_str} id {item_id}, pausing queries.")
            time.sleep(10)
            logging.info("Continuing, skipping failed query.")
            continue
        item_buys = item_market.get("buyOrders",[])
        item_sells = item_market.get("sellOrders",[])

        # Skip items where there is not a presence of both buy and sell orders
        if len(item_sells)==0 or len(item_buys)==0:
            continue

        # Convert market data list of dicts to pandas DataFrames
        all_buys = pd.DataFrame(item_buys)
        all_sells = pd.DataFrame(item_sells)

        # Add name and type fields
        all_buys["itemName"] = item_name
        all_sells["itemName"] = item_name
        all_buys["itemTypeStr"] = item_type_str.capitalize()
        all_sells["itemTypeStr"] = item_type_str.capitalize()
        all_buys["itemVolume"] = item_volume
        all_sells["itemVolume"] = item_volume

        # Trim market data to only the immediately relevant columns
        all_buys = all_buys[market_cols_0]
        all_sells = all_sells[market_cols_0]

        # Sum columns along quantity which are otherwise identical
        all_buys["quantity"] = all_buys["quantity"].astype(int)
        all_sells["quantity"] = all_sells["quantity"].astype(int)
        all_buys = all_buys.groupby(by=[x for x in market_cols_0 if x!="quantity"],as_index=False)["quantity"].sum()
        all_sells = all_sells.groupby(by=[x for x in market_cols_0 if x!="quantity"],as_index=False)["quantity"].sum()

        # Re-order columns again
        all_buys = all_buys[market_cols_0]
        all_sells = all_sells[market_cols_0]

        # Rename certain columns in preparation for merge
        buy_rename = {
            "priceThreshold":"Buying Price",
            "quantity":"Buying Quantity",
            "claimEntityId":"buyClaimEntityId",
            "claimName": "Buying Claim",
            "regionName": "Buying Region",
        }
        sell_rename = {
            "priceThreshold":"Selling Price",
            "quantity":"Selling Quantity",
            "claimEntityId":"sellClaimEntityId",
            "claimName": "Selling Claim",
            "regionName": "Selling Region",
        }
        all_buys = all_buys.rename(buy_rename,axis=1)
        all_sells = all_sells.rename(sell_rename,axis=1)

        # Perform the merge
        join_columns = [
            "itemName",
            "itemTypeStr",
            "itemId",
            "itemType",
            "itemVolume",
            ]

        merged_df = pd.merge(
            left=all_sells,
            right=all_buys,
            left_on = join_columns,
            right_on = join_columns,
            how="inner",
            )
        
        # Filter out trades that dont' start/end at the claim of interest
        merged_df = merged_df[(merged_df["sellClaimEntityId"]==claim_id) | (merged_df["buyClaimEntityId"]==claim_id)]

        # Calculate profitability
        merged_df["Buying Price"] = merged_df["Buying Price"].astype(int)
        merged_df["Selling Price"] = merged_df["Selling Price"].astype(int)
        merged_df["Trade Quantity"] = merged_df[["Buying Quantity","Selling Quantity"]].min(axis=1)
        merged_df["Unit Profit"] = merged_df["Buying Price"] - merged_df["Selling Price"]

        # Filter out trades that are strictly unprofitable
        merged_df = merged_df[merged_df["Unit Profit"]>0]

        # Add to the main dataframe
        full_df = pd.concat([full_df,merged_df],ignore_index=True)
    logging.info("Unit profitability calculated and filtered.")

    logging.info("Beginning calculation of straight-line distances.")
    claim_list = pd.unique(full_df[["buyClaimEntityId","sellClaimEntityId"]].values.ravel("K"))
    claim_N = len(claim_list)
    logging.info(f"{claim_N:,} identified profitable trade partners.")
    logging.info(f"Pausing {throttle_rate} [s] between each claim query out of courtesy to Bitjita.")
    logging.info(f"Earliest possible completion in {np.round(claim_N*throttle_rate/60,2)} [min].")
    dist_dict = {}
    for i in range(0,claim_N):
        if i%10 == 0:
            logging.info(f"{np.round(100*i/claim_N,2)} % Complete: {i:,}/{claim_N:,} Claims")
        claim_id_iter = claim_list[i]

        # Throttling at the user-defined rate
        time.sleep(throttle_rate)

        # Querying bitjita
        try:
            claim = data_client._make_request(f"claims/{claim_id_iter}").get("claim",{})
        except:
            logging.error(f"Unable to complete query for claim id {claim_id_iter}, pausing queries.")
            time.sleep(10)
            logging.info("Continuing, skipping failed query.")
            continue

        # Calculating distance to primary claim
        claim_X = claim.get("locationX",0)
        claim_Z = claim.get("locationZ",0)

        dist = np.round(np.sqrt(((X0-claim_X)**2) + ((Z0-claim_Z)**2))/3,2)
        dist_dict[claim_id_iter] = dist

    logging.info("Applying calculated distances.")
    for i in range(0,len(full_df)):
        claim_0 = full_df.loc[i,"buyClaimEntityId"]
        claim_1 = full_df.loc[i,"sellClaimEntityId"]

        # Identifying the claim in the trade which is NOT the primary claim of interest
        if claim_0 == claim_id:
            target_claim = claim_1
        else:
            target_claim = claim_0
        
        full_df.loc[i,"Distance"] = dist_dict.get(target_claim,np.inf)

    logging.info("Applying trade capacity limits.")
    player_capacity = {
        "item_slots": 25,
        "cargo_slots": 1,
        "item_slot_size": 6000,
        "cargo_slot_size": 6000,
        }
    vehicle_capacity = {
        "Raft":{
            "item_slots": 6,
            "cargo_slots": 1,
            "item_slot_size": 6000,
            "cargo_slot_size": 60000,
        },
        "Skiff":{
            "item_slots": 24,
            "cargo_slots": 4,
            "item_slot_size": 6000,
            "cargo_slot_size": 60000,
        },
        "Clipper":{
            "item_slots": 30,
            "cargo_slots": 6,
            "item_slot_size": 6000,
            "cargo_slot_size": 60000,
        },
        "Cargo Ship":{
            "item_slots": 30,
            "cargo_slots": 10,
            "item_slot_size": 6000,
            "cargo_slot_size": 60000,
        },
    }
    base_item_qty = (player_capacity["item_slot_size"]*player_capacity["item_slots"])
    base_cargo_qty = player_capacity["cargo_slot_size"]*player_capacity["cargo_slots"]
    full_df["itemVolume"] = full_df["itemVolume"].astype(int)
    for vehicle in vehicle_capacity:
        for i in range(0,len(full_df)):
            if full_df.loc[i,"itemTypeStr"] == "Item":
                qty = (vehicle_capacity[vehicle]["item_slot_size"]*vehicle_capacity[vehicle]["item_slots"])
                qty += base_item_qty
            elif full_df.loc[i,"itemTypeStr"] == "Cargo":
                qty = (vehicle_capacity[vehicle]["cargo_slot_size"]*vehicle_capacity[vehicle]["cargo_slots"])
                qty += base_cargo_qty
            else:
                qty = 0

            qty = int(qty/full_df.iloc[i]["itemVolume"])
            full_df.loc[i,f"{vehicle} Quantity"] = int(min([full_df.iloc[i]["Trade Quantity"],qty]))

        full_df[f"{vehicle} Quantity"] = full_df[f"{vehicle} Quantity"].astype(int)

        full_df[f"{vehicle} Profit"] = full_df[f"{vehicle} Quantity"] * full_df["Unit Profit"]

        full_df[f"{vehicle} Profit per 1k Dist"] = np.round(1000*full_df[f"{vehicle} Profit"]/full_df["Distance"],2)

    # Deleting columns that are no longer necessary
    full_df = full_df.drop(["itemId","itemType","itemVolume","buyClaimEntityId","sellClaimEntityId"],axis=1)

    # Rename particular columns that still have API names
    full_df = full_df.rename({"itemName":"Item","itemTypeStr":"Item Type"},axis=1)

    # Sort DatFrame by cargo ship profitability
    full_df = full_df.sort_values("Cargo Ship Profit per 1k Dist",ascending=False)

    # Determine end time
    t_end = datetime.now(timezone.utc)
    t_diff = int(np.round((t_end-t_start).seconds/60))
    logging.info(f"Analysis completed at {t_end.strftime('%Y-%m-%d %H:%M %Z')}, taking {t_diff} [min].")

    # Replace any infs with nans, and then drop all nans
    full_df = full_df.replace([np.inf, -np.inf], np.nan)
    full_df = full_df.dropna().reset_index(drop=True)

    if len(full_df) > 0:
        logging.info("Beginning upload to google sheets.")

        spreadsheet = gc.open(spreadsheet_name)

        # Updating overview info
        claim_list = pd.unique(full_df[["Buying Claim","Selling Claim"]].values.ravel("K"))
        claim_N = len(claim_list)
        worksheet = spreadsheet.worksheet("Overview")
        worksheet.update_acell("D12", f"{t_start.strftime('%Y-%m-%d %H:%M %Z')}")
        worksheet.update_acell("G12", f"{t_diff} [min]")
        worksheet.update_acell("D14", f"{full_df["Item"].nunique()}")
        worksheet.update_acell("G14", f"{claim_N}")

        # full_df.to_csv("test.csv",index=False)

        worksheet = spreadsheet.worksheet("Profitable Trades")
        worksheet.clear()
        worksheet.delete_rows(3,10000)
        worksheet.update([full_df.columns.values.tolist()] + full_df.values.tolist())

        logging.info("Upload to google sheets completed.")
    else:
        logging.warning("Empty dataset. Something seems to have gone wrong.")

class bitjita_client():
    """
    A client class for making queries to the Bitjita public API.
    """
    def __init__(self):
        self.base_url = "https://bitjita.com/api"

    def _make_request(self,endpoint,params=None):
        try:
            url = f"{self.base_url}/{endpoint}"
            response = requests.get(
                url=url,
                params=params,
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"Bitjita API Request failed: {e}")
            raise

if __name__ == "__main__":
    # Initialize logging to both console and log file
    logging.basicConfig(
        # level=logging.DEBUG,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("bc-mercantile.log", mode="w"),
        ],
        force=True,
    )
    logging.info("===== BitCraft Mercantile Starting =====")
    main()
    logging.info("=== BitCraft Mercantile Shutting Down ===")
    logging.shutdown()