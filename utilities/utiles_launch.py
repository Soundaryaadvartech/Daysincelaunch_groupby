import pandas as pd
import numpy as np
from sqlalchemy.orm import Session
from sqlalchemy import func
from functions.period import process_period_data
from sqlalchemy import and_
from utilities.data_clean import process_beelittle,process_prathiksham,process_zing
from utilities.filter import filter_data_launch

def generate_inventory_summary(db: Session, models, days: int, group_by: str, business: str,filter_jason):
  
    # Validate and set grouping columns
    if group_by.lower() == "item_id":
        grp = ["Item_Id"]
    elif group_by.lower() == "item_name":
        if business.lower() in ["prathiksham", "zing", "adoreaboo"]:
            grp = ["Item_Name", "Item_Type", "Category"]
        elif business.lower() == "beelittle":
            grp = ["Item_Name", "Item_Type", "Product_Type"]
    else:
        raise ValueError("group_by must be either 'item_id' or 'item_name'")

    # Set category column based on business
    colu = "Product_Type" if business.lower() == "beelittle" else "Category"
    
    if business == "beelittle":
        t1 = db.query(models.Item.Item_Id,models.Item.Item_Name,models.Item.Item_Type,models.Item.Item_Code,models.Item.Sale_Price,models.Item.Sale_Discount,
                    models.Item.Current_Stock,models.Item.Is_Public,models.Item.Age,models.Item.Bottom,
                    models.Item.Bundles,models.Item.Colour,models.Item.Fabric,models.Item.Filling,models.Item.Gender,
                    models.Item.Pack_Size,models.Item.Pattern,models.Item.Product_Type,models.Item.Sale,models.Item.Size,
                    models.Item.Sleeve,models.Item.Style,models.Item.Top,models.Item.Weave_Type,models.Item.Weight,models.Item.Width,models.Item.batch,         # Special column with "__Batch"models.Item.__Bottom_Fabric, # Special column with "__Bottom_Fabric"
                    models.Item.brand_name,models.Item.inventory_type, models.Item.launch_date, 
                    models.Item.quadrant,models.Item.relist_date,models.Item.restock_status,models.Item.season, 
                    models.Item.supplier_name, models.Item.print_colour,models.Item.print_size,models.Item.print_theme,models.Item.Discount,    
                    models.Item.print_key_motif,models.Item.print_style).all()
        
        rows = [row._asdict() for row in t1]
        t1 = pd.DataFrame(rows)
        t1 = process_beelittle(t1)
        t1 = filter_data_launch(t1, filter_jason)
        t1 = t1[["Item_Id", "Item_Name", "Item_Type", "Item_Code", "Product_Type","Age","Discount", 
                  "Current_Stock", "launch_date", "Sale_Price", "Sale_Discount", "batch"]]
    elif business == "prathiksham":
        
        t1 = db.query(models.Item.Item_Id,models.Item.Item_Name,models.Item.Item_Type,models.Item.Item_Code,
                    models.Item.Sale_Price,models.Item.Sale_Discount,models.Item.Current_Stock,
                    models.Item.Is_Public,models.Item.Category,models.Item.Colour,models.Item.Fabric,models.Item.Fit,
                    models.Item.Lining,models.Item.Neck,models.Item.Occasion,models.Item.Print,
                    models.Item.Product_Availability,models.Item.Size,models.Item.Sleeve,models.Item.Pack,models.Item.batch,          
                    models.Item.bottom_length,models.Item.bottom_print,models.Item.bottom_type,models.Item.collections,   
                    models.Item.details,models.Item.dispatch_time,models.Item.launch_date,models.Item.new_arrivals,   
                    models.Item.pack_details,models.Item.pocket,models.Item.top_length,models.Item.waistband  
                    ).all()
        rows = [row._asdict() for row in t1]
        t1 = pd.DataFrame(rows)
        t1 = process_prathiksham(t1)
        t1 = filter_data_launch(t1, filter_jason)
        t1 = t1[["Item_Id", "Item_Name", "Item_Type", "Item_Code", "Category", 
                  "Current_Stock", "launch_date", "Sale_Price", "Sale_Discount", "batch"]]


    elif business == "zing":
        t1 = db.query(models.Item.Item_Id,models.Item.Item_Name,models.Item.Item_Type,models.Item.Item_Code,models.Item.Sale_Price,
                    models.Item.Sale_Discount,models.Item.Current_Stock,models.Item.Is_Public,models.Item.Category,
                    models.Item.Colour,models.Item.Fabric,models.Item.Fit,models.Item.Neck,models.Item.Occasion,
                    models.Item.Print,models.Item.Size,models.Item.Sleeve,models.Item.batch,
                    models.Item.details,models.Item.launch_date,models.Item.office_wear_collection,
                    models.Item.print_type,models.Item.quadrant,models.Item.style_type 
                    ).all()
        rows = [row._asdict() for row in t1]
        t1 = pd.DataFrame(rows)
        t1 = process_zing(t1)
        t1 = filter_data_launch(t1, filter_jason)
        t1 = t1[["Item_Id", "Item_Name", "Item_Type", "Item_Code", "Category", 
                  "Current_Stock", "launch_date", "Sale_Price", "Sale_Discount", "batch"]]
    
    elif business == "adoreaboo":
        t1 = db.query(models.Item.Item_Id, models.Item.Item_Name,models.Item.Item_Type, models.Item.Item_Code,
            models.Item.Sale_Price, models.Item.Sale_Discount, models.Item.Current_Stock, models.Item.Is_Public,
            models.Item.Category, models.Item.Age, models.Item.Bottom, models.Item.Colour, models.Item.Fabric,
            models.Item.Gender, models.Item.Neck_Closure, models.Item.Neck_Type, models.Item.Occassion,
            models.Item.Pack_Size, models.Item.Print_Collections, models.Item.Print_Pattern, models.Item.Print_Size,
            models.Item.Printed_Pattern, models.Item.Sleeve, models.Item.Top, models.Item.Weave_Type, models.Item.age_category,
            models.Item.batch, models.Item.bottom_fabric, models.Item.launch_date, models.Item.print_size, models.Item.product_category,
            models.Item.product_type).all()
        rows = [row._asdict() for row in t1]
        t1 = pd.DataFrame(rows)
        t1 = filter_data_launch(t1, filter_jason)
        t1 = t1[["Item_Id", "Item_Name", "Item_Type", "Item_Code", "Category", 
                  "Current_Stock", "launch_date", "Sale_Price", "Sale_Discount", "batch"]]
        t1["launch_date"] = pd.to_datetime(t1["launch_date"])
    else:
        return "Invalid business name"
    

    # Only fetch needed attributes for dt dataframe
    if business.lower() == "prathiksham":
        dt_attrs = ["Item_Id", "Item_Name", "Item_Type", "Category", 
                    "Colour", "Fabric", "Fit", "Lining", "Neck", "Size", "Sleeve", "Pack"]
    elif business.lower() == "zing":
        dt_attrs = ["Item_Id", "Item_Name", "Item_Type", "Category", 
                    "Colour", "Fabric", "Fit", "Neck", "Occasion", "Print", "Size", "Sleeve"]
    elif business.lower() == "adoreaboo":
        dt_attrs = [
            "Item_Id", "Item_Name", "Item_Type","Category", "Age","Bottom", "Colour", "Fabric",
            "Gender", "Neck_Closure","Neck_Type",  "Occassion", "Pack_Size", "Print_Collections", "Print_Pattern",
            "Print_Size", "Printed_Pattern", "Sleeve", "Top", "Weave_Type","age_category", "bottom_fabric",
            "print_size", "product_category", "product_type"
        ]
    else:  # beelittle
        dt_attrs = ["Item_Id", "Item_Name", "Item_Type", "Product_Type","Age", "Bottom", "Bundles", "Fabric",
                   "Filling", "Gender", "Pack_Size", "Pattern", "Size", "Sleeve", "Style", 
                   "Top", "Weave_Type", "Weight", "Width"]
    
    # Fetch attributes for dt only if needed for variations
    if group_by.lower() == "item_id":
            dt_items = db.query(*[getattr(models.Item, attr) for attr in dt_attrs]).all()
            dt_values = [list(item) for item in dt_items]  # Convert each ORM object to a list of values
            dt = pd.DataFrame(dt_values, columns=dt_attrs)
    else:
        dt = ""
    # Batch all database queries together
    sales = db.query(models.Sale.Item_Id, models.Sale.Date, models.Sale.Quantity, models.Sale.Total_Value).all()
    viewsatc = db.query(models.ViewsAtc.Item_Id, models.ViewsAtc.Date, models.ViewsAtc.Items_Viewed, models.ViewsAtc.Items_Addedtocart).all()
    first_sold_dates = db.query(models.Sale.Item_Id, func.min(models.Sale.Date).label("First_Sold_Date")).group_by(models.Sale.Item_Id).all()
    last_sold_dates = db.query(models.Sale.Item_Id, func.max(models.Sale.Date).label("Last_Sold_Date")).group_by(models.Sale.Item_Id).all()
    
    # Convert to dataframes
    t2 = pd.DataFrame(sales, columns=["Item_Id", "Date", "Quantity", "Total_Value"])
    t3 = pd.DataFrame(viewsatc, columns=["Item_Id", "Date", "Items_Viewed", "Items_Addedtocart"])
    t4 = pd.DataFrame(first_sold_dates, columns=["Item_Id", "First_Sold_Date"])
    t5 = pd.DataFrame(last_sold_dates, columns=["Item_Id", "Last_Sold_Date"])
    
    # Preprocess data types in one batch to avoid redundant conversions
    t1["launch_date"] = pd.to_datetime(t1["launch_date"])
    t1["Item_Id"] = t1["Item_Id"].astype(int)
    t1["Sale_Price"] = t1["Sale_Price"].astype(int)
    t1["Current_Stock"] = t1["Current_Stock"].astype(int)
    t1["Sale_Discount"] = t1["Sale_Discount"].astype(float)
    t2["Date"] = pd.to_datetime(t2["Date"])
    t3["Date"] = pd.to_datetime(t3["Date"])
    t3["Item_Id"] = t3["Item_Id"].astype(int)
    t5["Item_Id"] = t5["Item_Id"].astype(int)
    t5["Last_Sold_Date"] = pd.to_datetime(t5["Last_Sold_Date"])
    t2["Total_Value"] = t2["Total_Value"].astype(float)
    # Merge first sold date
    t1 = pd.merge(t1, t4, how="left", on="Item_Id")
    t1["launch_date"] = t1["launch_date"].fillna(t1["First_Sold_Date"])
    
    # Pre-calculate all-time aggregations to avoid redundant calculations
    if group_by.lower() == "item_id":
        temp_t2 = t2.groupby("Item_Id").agg({"Quantity": "sum"}).rename(columns={"Quantity": "Alltime_Total_Quantity"}).reset_index()
        t3_total = t3.groupby("Item_Id").agg({
            "Items_Viewed": "sum",
            "Items_Addedtocart": "sum"
        }).rename(columns={
            "Items_Addedtocart": "Alltime_Items_Addedtocart",
            "Items_Viewed": "Alltime_Items_Viewed"
        }).reset_index()
    else:
        # For item_name grouping, add the grouping columns first
        t2_with_groups = pd.merge(t2, t1[['Item_Id'] + grp].drop_duplicates(), on='Item_Id', how='left')
        t3_with_groups = pd.merge(t3, t1[['Item_Id'] + grp].drop_duplicates(), on='Item_Id', how='left')
        print(t2_with_groups[t2_with_groups.Item_Id==1077])
        
        temp_t2 = t2_with_groups.groupby(grp).agg({"Quantity": "sum"}).rename(columns={"Quantity": "Alltime_Total_Quantity"}).reset_index()
        t3_total = t3_with_groups.groupby(grp).agg({
            "Items_Viewed": "sum",
            "Items_Addedtocart": "sum"
        }).rename(columns={
            "Items_Addedtocart": "Alltime_Items_Addedtocart",
            "Items_Viewed": "Alltime_Items_Viewed"
        }).reset_index()
    
    # Create variations function optimized for performance
    
    
    # Optimized item summary function
    def get_item_summary(t1, t2, t3, start_offset, end_offset):
        print(start_offset,end_offset)
        
        t1['Start_Date'] = t1['launch_date'] + pd.to_timedelta(start_offset, unit='D')
        
        
        t1['End_Date'] = t1['launch_date'] + pd.to_timedelta(end_offset, unit='D')
        t1['Period_Days'] = end_offset - start_offset
        
        get_lst = grp + ['Start_Date', 'End_Date','Item_Id']
        
    
        
        # Filter data based on date range
        join_cols = 'Item_Id' if group_by.lower() == "item_id" else grp
        
        # Use vectorized operations for 
        print("1",t1.info())
        t2_merge = pd.merge(t2, t1[['Start_Date', 'End_Date','Item_Id']], on="Item_Id", how='inner')
        t3_merge = pd.merge(t3, t1[['Start_Date', 'End_Date','Item_Id']], on="Item_Id", how='inner')
        
        
        
        t2_filtered = t2_merge[(t2_merge['Date'] >= t2_merge['Start_Date']) & (t2_merge['Date'] < t2_merge['End_Date'])]
        t3_filtered = t3_merge[(t3_merge['Date'] >= t3_merge['Start_Date']) & (t3_merge['Date'] < t3_merge['End_Date'])]
        
        

        t2_agg = t2_filtered.groupby("Item_Id", as_index=False)[['Quantity', 'Total_Value']].sum()
        
        t3_agg = t3_filtered.groupby("Item_Id", as_index=False)[['Items_Viewed', 'Items_Addedtocart']].sum()
        
        
        
        period_df = pd.merge(t2_agg[["Item_Id","Quantity","Total_Value"]], t3_agg[["Item_Id","Items_Viewed","Items_Addedtocart"]], on="Item_Id", how='outer')

        agg_dict = {
            'Item_Id': 'first',
            'Item_Code': 'first',
            'Current_Stock': 'sum',
            'launch_date': 'min',
            'Period_Days': 'first',
            'Sale_Price': 'mean',
            'Sale_Discount': 'mean',
            'Quantity': 'sum',
            'Total_Value': 'sum',
            "Items_Viewed": 'sum',
            "Items_Addedtocart": 'sum'
    
        }


        

        if group_by.lower() == "item_id":
            period_df = pd.merge(t1, period_df, on="Item_Id", how='left')
            period_df = period_df.drop(columns = ["Item_Name","Item_Type",colu])
        else:
            period_df = pd.merge(t1, period_df, on="Item_Id", how='left')

            period_df = period_df.groupby(grp, as_index=False).agg(agg_dict)




        
        # Fill NA values all at once
        period_df = period_df.fillna(0)
        
        return period_df
    
    
    
    # Generate summaries for both periods
    first_period_df = get_item_summary(t1, t2, t3, 0, days)
 
    second_period_df = get_item_summary(t1, t2, t3, days, 2 * days)

    # Process both periods
    first_period_results = process_period_data(t1,t2,t3,t4,t5,temp_t2,t3_total,dt,colu,days,first_period_df,"first_period",group_by,grp)
    
    
    second_period_results = process_period_data(t1,t2,t3,t4,t5,temp_t2,t3_total,dt,colu,days,second_period_df,"second_period",group_by,grp)
   
    
    # Ensure both dataframes have necessary columns for item_id grouping
    
    
    # Define common columns for the combined results
    common_cols = ["Item_Id",
        "Item_Name", "Item_Type", colu,"Variations","Sale_Discount", "launch_date", 
        "days_since_launch", "Projected_Days_to_Sellout", "Days_Sold_Out_Past", 
        "Current_Stock", "Total_Stock", "Current_Stock_Value", "Total_Stock_Value", 
        "Sale_Price", "Sale_Price_After_Discount", "Alltime_Total_Quantity",
        "Alltime_Total_Quantity_Value", "Alltime_Perday_Quantity", "Alltime_Items_Viewed",
        "Alltime_Perday_View", "Alltime_Items_Addedtocart", "Alltime_Perday_ATC",
        "Total_Stock_Sold_Percentage" 
    ]
    

  
    
    # Get period-specific columns
    first_period_specific_cols = [col for col in first_period_results.columns 
                                 if col.startswith("first_period") or 
                                 (col.startswith("Predicted_Quantity_Next") and "first_period" in col)]
    
    second_period_specific_cols = [col for col in second_period_results.columns 
                                  if col.startswith("second_period") or 
                                  (col.startswith("Predicted_Quantity_Next") and "second_period" in col)]
    
    # Create combined results using a single merge operation when possible
    print(first_period_results.columns)
    combined_results = first_period_results[common_cols].copy()
    
    # Add first period specific columns
    for col in first_period_specific_cols:
        combined_results[col] = first_period_results[col]
    
    # Add second period specific columns with a single merge
    join_cols = ['Item_Id'] if group_by.lower() == "item_id" else grp
    second_period_cols = join_cols + second_period_specific_cols
    combined_results = pd.merge(combined_results, second_period_results[second_period_cols], on=join_cols, how='left')
    combined_results = combined_results.loc[:, ~combined_results.columns.duplicated()]
    # Final formatting - do this in bulk
    # Round numeric columns
    numeric_cols = combined_results.select_dtypes(include=['number']).columns
    
    combined_results[numeric_cols] = combined_results[numeric_cols].round(2)
    
    # Format date columns if they exist
    if "launch_date" in combined_results.columns and not combined_results["launch_date"].empty:
        if pd.api.types.is_datetime64_any_dtype(combined_results["launch_date"]):
            combined_results["launch_date"] = combined_results["launch_date"].dt.strftime('%Y-%m-%d')
    
    # Sort by primary grouping column
    
    combined_results = combined_results.sort_values(by="Item_Id").reset_index(drop=True)

    return combined_results