import pandas as pd
import pymysql
import numpy as np
from datetime import datetime
import datetime 
import json
from sqlalchemy import func
from utilities.filter import filter_data
from utilities.data_clean import process_beelittle,process_prathiksham,process_zing
from utilities.group import group_by_bee,group_by_dic_prathisham,group_by_dic_zing




def agg_grp(db, models, business, filter_dict, data_dict, groupby_dict):  # Filter for query, Data aggregation methods, Group-by conditions
    if business == "beelittle":
        group_by_dic = group_by_bee
        t1 = db.query(models.Item.Item_Id,models.Item.Item_Name,models.Item.Item_Type,models.Item.Item_Code,models.Item.Sale_Price,models.Item.Sale_Discount,
                    models.Item.Current_Stock,models.Item.Is_Public,models.Item.Age,models.Item.Bottom,
                    models.Item.Bundles,models.Item.Colour,models.Item.Fabric,models.Item.Filling,models.Item.Gender,
                    models.Item.Pack_Size,models.Item.Pattern,models.Item.Product_Type,models.Item.Sale,models.Item.Size,
                    models.Item.Sleeve,models.Item.Style,models.Item.Top,models.Item.Weave_Type,models.Item.Weight,models.Item.Width,models.Item.batch,         # Special column with "__Batch"models.Item.__Bottom_Fabric, # Special column with "__Bottom_Fabric"
                    models.Item.brand_name,models.Item.inventory_type, models.Item.launch_date, 
                    models.Item.quadrant,models.Item.relist_date,models.Item.restock_status,models.Item.season, 
                    models.Item.supplier_name, models.Item.print_colour,models.Item.print_size,models.Item.print_theme,     
                    models.Item.print_key_motif,models.Item.print_style).all()
        
        rows = [row._asdict() for row in t1]
        t1 = pd.DataFrame(rows)
        t1 = process_beelittle(t1)
        
    elif business == "prathiksham":
        group_by_dic = group_by_dic_prathisham
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

    elif business == "zing":
        group_by_dic = group_by_dic_zing

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


    else:
        return "Invalid business name"
    

    t1["launch_date"] = t1["launch_date"].replace(0, np.nan)
    t1["Sale_Price"] = t1.Sale_Price.astype("float")
    t1["Sale_Discount"] = t1.Sale_Discount.astype("float")
    t1["Current_Stock"] = t1.Current_Stock.astype("float")
    t1["launch_date"] = pd.to_datetime(t1["launch_date"])

    query2 = db.query(func.min(models.Sale.Date).label('First_Sold_Date'),models.Sale.Item_Id
                    ).group_by(models.Sale.Item_Id)
    result = query2.all()
    t2 = pd.DataFrame(result, columns=["First_Sold_Date", "Item_Id"])
    
    def filter(db, models,t1, filter_dict,query3,query4):
        t1, query3, query4 = filter_data(db, models, t1, filter_dict, query3, query4)
        print("t1",t1.columns)
        df = pd.merge(t1,t2,how="left",on="Item_Id")

        df["launch_date"]=df["launch_date"].fillna(df["First_Sold_Date"])

        df = df.drop(columns = "First_Sold_Date")

        # Get the current date
        current_date = pd.Timestamp.now().date()

        # Calculate the days since launch
        df["Days_Since_Launch"] = (pd.Timestamp(current_date) - df["launch_date"]).dt.days

        result_query3 = query3.all()
        result_query4 = query4.all()

        # Convert the results from query3 to DataFrame
        t3 = pd.DataFrame(result_query3, columns=["Item_Id", "Total_Quantity", "Total_Value"])

        # Convert the results from query4 to DataFrame
        t4 = pd.DataFrame(result_query4, columns=["Item_Id", "Total_Item_Viewed", "Total_Item_Atc"])

        df = pd.merge(df,t3,how="left",on="Item_Id")

        df["Total_Quantity"] = df.Total_Quantity.astype("float")
        df["Total_Value"] = df.Total_Value.astype("float")
        t4["Item_Id"] = t4["Item_Id"].astype("int")
        df = pd.merge(df,t4,how="left",on="Item_Id")
        df["Total_Item_Viewed"] = df.Total_Item_Viewed.astype("float")
        df["Total_Item_Atc"] = df.Total_Item_Atc.astype("float")
        df["Per_Day_Value"] = df["Total_Value"] / df["Days_Since_Launch"]
        df["Per_Day_Quantity"] = df["Total_Quantity"] / df["Days_Since_Launch"]
        df["Per_Day_View"] = df["Total_Item_Viewed"] / df["Days_Since_Launch"]
        df["Per_Day_atc"] = df["Total_Item_Atc"] / df["Days_Since_Launch"]
        df["Days_Until_Stockout"] = df["Current_Stock"] / df["Per_Day_Quantity"]
        df["Conversion_Percentage"] = df["Total_Quantity"] / df["Total_Item_Atc"] *100
        
        df["Launch_Count"] = df["launch_date"]
        for col in df.select_dtypes(exclude=np.number).columns:
            df[col] = df[col].fillna("None")

        # Fill numeric columns with 0 (or another appropriate numeric value)
        for col in df.select_dtypes(include=np.number).columns:
            df[col] = df[col].fillna(0)
        df["launch_date"] = df["launch_date"].replace(0, np.nan)
            
        df["launch_date"] = pd.to_datetime(df["launch_date"],errors='coerce')
        print("df",df.columns)
        return df
    
    def Columns_to_Choose(df,data_dict):
        dimensions = data_dict.get("Dimension", [])
        aggregations = data_dict.get("Aggregation", [])
        col_choose = dimensions + aggregations
        df1 = df[col_choose]  # Store selected columns in df1
        
    
        return df1

    def extract_groupby_agg(groupby_dict, df1):
        groupby_columns = groupby_dict.get("groupby", [])
        
        for col in groupby_columns:
            if col not in df1.columns:
                print(f"Error: The column '{col}' specified for grouping is not present in the DataFrame. Please verify the column names.")
        
        return groupby_columns

    def agg_col(df1,groupby_columns):
        col = df1.columns.tolist()

        for i in groupby_columns:
            col.remove(i)
        agg_dict = {}
        for column in col:
            if column in group_by_dic:
                agg_dict[column] = group_by_dic[column]
        return agg_dict
    
    
    query3 = db.query(models.Sale.Item_Id,
                      func.sum(models.Sale.Quantity).label("Total_Quantity"),
                      func.sum(models.Sale.Total_Value).label("Total_Value")).group_by(models.Sale.Item_Id)

    
    query4 = db.query(models.ViewsAtc.Item_Id,
                      func.sum(models.ViewsAtc.Items_Viewed).label('Total_Item_Viewed'),
                      func.sum(models.ViewsAtc.Items_Addedtocart).label('Total_Item_Atc')).group_by(models.ViewsAtc.Item_Id)
   

    df = filter(db, models,t1, filter_dict, query3, query4)
    

    df1 = Columns_to_Choose(df,data_dict)
    

    group_columns = extract_groupby_agg(groupby_dict, df1)
    

    agg_dict = agg_col(df1,group_columns)

    grouped_df = df1.groupby(group_columns).agg(agg_dict)

    grouped_df = grouped_df.reset_index()

    if "launch_date" in grouped_df.columns.tolist():
        grouped_df["launch_date"] = grouped_df["launch_date"].dt.strftime('%Y-%m-%d')

    return grouped_df

   









