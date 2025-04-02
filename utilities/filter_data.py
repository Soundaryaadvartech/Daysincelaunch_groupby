import pandas as pd
import numpy as np
from sqlalchemy.orm import Session
from utilities.data_clean import process_beelittle,process_prathiksham,process_zing

def get_filter_data(db: Session, models, business: str):
    if business == "beelittle":
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
        t1 = t1.drop(columns=["Item_Id","Item_Code","Sale_Price","Sale_Discount","Current_Stock","Pack_Size","Sale","brand_name","inventory_type","launch_date","quadrant","supplier_name","season","Print_Style_1","Print_Style_2","Print_Theme_1","Print_Theme_2","Print_Key_Motif_1","Print_Key_Motif_2","Print_Colour_1","Print_Colour_2","relist_date","restock_status","Weight","Width","Weave_Type","Filling","Width","Weight"])

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
        t1 = t1.drop(columns=["Item_Id","Item_Code","Sale_Price","Sale_Discount","Current_Stock","Product_Availability","details","dispatch_time","launch_date","pack_details"])

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
        t1 = t1.drop(columns=["Item_Id","Item_Code","Sale_Price","Sale_Discount","Current_Stock","details","launch_date","office_wear_collection"])
    
    elif business == "adoreaboo":
        t1 = db.query(models.Item.Item_Id, models.Item.Item_Name,models.Item.Item_Type, models.Item.Item_Code,
        models.Item.Sale_Price, models.Item.Sale_Discount, models.Item.Current_Stock, models.Item.Is_Public,
        models.Item.Category, models.Item.Age, models.Item.Bottom, models.Item.Colour, models.Item.Fabric,
        models.Item.Gender, models.Item.Neck_Closure, models.Item.Neck_Type, models.Item.Occasion,
        models.Item.Pack_Size, models.Item.Print_Collections, models.Item.Print_Pattern, models.Item.Print_Size,
        models.Item.Printed_Pattern, models.Item.Sleeve, models.Item.Top, models.Item.Weave_Type, models.Item.age_category,
        models.Item.batch, models.Item.bottom_fabric, models.Item.launch_date, models.Item.print_size, models.Item.product_category,
        models.Item.product_type)
        rows = [row._asdict() for row in t1]
        t1 = pd.DataFrame(rows)
        t1 = t1.drop(columns=["Item_Id","Item_Code","Sale_Price","Sale_Discount","Current_Stock","launch_date"])


    return t1