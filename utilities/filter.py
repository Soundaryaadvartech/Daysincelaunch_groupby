import pandas as pd
from sqlalchemy import func
def filter_data(db,models,t1, filter_dict,query3,query4):
    filtered_t1 = t1.copy()

    if not filter_dict:
        # If no filter is provided, return the original dataframe with the default queries
        return filtered_t1,query3,query4
    for col, values in filter_dict.items():
        if not values:
            continue

        # Skip Date column validation as it's used in SQL filtering
        if col not in filtered_t1.columns and col != "Date":
            print(f"Warning: Column '{col}' not found in DataFrame. Filter for this column will be ignored.")
            continue

        if col in filtered_t1.columns:
            valid_values = [val for val in values if val in filtered_t1[col].values]

            if not valid_values:
                print(f"Warning: No valid filter values for column '{col}'. Filter for this column will be ignored.")
                continue

            filtered_t1 = filtered_t1[filtered_t1[col].isin(valid_values)]


    # Modify SQL queries if 'Date' key is in filter_dict
    if "Date" in filter_dict and len(filter_dict["Date"]) == 2:
        start_date, end_date = filter_dict["Date"]
        where_condition = (
            models.Sale.Date.between(start_date, end_date) 
            if hasattr(models.Sale, 'Date') 
            else None
        )
        # Modify the query3 for date filter in Sale table
        query3 = db.query(
            models.Sale.Item_Id,
            func.sum(models.Sale.Quantity).label("Total_Quantity"),
            func.sum(models.Sale.Total_Value).label("Total_Value")
        )
        
        query3 = query3.filter(where_condition)
        query3 = query3.group_by(models.Sale.Item_Id)

        where_condition = (
            models.ViewsAtc.Date.between(start_date, end_date) 
            if hasattr(models.ViewsAtc, 'Date') 
            else None
        )

        # Modify the query4 for date filter in Viewsatc table
        query4 = db.query(
            models.ViewsAtc.Item_Id,
            func.sum(models.ViewsAtc.Items_Viewed).label('Total_Item_Viewed'),
            func.sum(models.ViewsAtc.Items_Addedtocart).label('Total_Item_Atc')
        )
       
        query4 = query4.filter(where_condition)
        query4 = query4.group_by(models.ViewsAtc.Item_Id)


    return filtered_t1, query3, query4

def filter_data_launch(t1, filter_jason):
    filtered_t1 = t1.copy()

    if not filter_jason:
        # If no filter is provided, return the original dataframe with the default queries
        return filtered_t1
    for col, values in filter_jason.items():
        if not values:
            continue

        # Skip Date column validation as it's used in SQL filtering
        if col not in filtered_t1.columns and col != "Date":
            print(f"Warning: Column '{col}' not found in DataFrame. Filter for this column will be ignored.")
            continue

        if col in filtered_t1.columns:
            valid_values = [val for val in values if val in filtered_t1[col].values]

            if not valid_values:
                print(f"Warning: No valid filter values for column '{col}'. Filter for this column will be ignored.")
                continue

            filtered_t1 = filtered_t1[filtered_t1[col].isin(valid_values)]

    return filtered_t1

