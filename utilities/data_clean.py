import pandas as pd
def process_beelittle(t1):
    
    # Mapping dictionaries for replacements
    restock_status_map = {"DISCONTINUE": "DISCONTINUED", "DIS CONTINUED": "DISCONTINUED", "CONTINUE": "CONTINUED"}
    age_map = {
        "NB": "Newborn", "0-3m,Newborn": "Newborn,0-3m", "1-2 yr": "1-2yr", "3-6M": "3-6m", 
        "6-12M": "6-12m", "1.5-2yr": "18-24m", "1-1.5yr": "12-18m"
    }
    fabric_map = {
        "cotton": "Cotton", "Mulmul": "MulMul", "Crinkle fabric": "Crinkle", 
        "Organic Cotton,Muslin": "Muslin,Organic Cotton", "Velvet,Cotton": "Cotton,Velvet", "Tissue,Crepe": "Crepe,Tissue"
    }
    pack_size_map = {"1-pack": "1-Pack"}
    product_type_map = {
        "Hoodie,Sweater": "Hoodie, Sweater", "Sweater,Hoodie": "Hoodie, Sweater",
        "Cap Mittens Booties": "Cap, Mittens, Booties", "Jabla,Top": "Jabla, Top",
        "Top,Jabla": "Jabla, Top", "Sweater , Pant": "Sweater and Pant"
    }

    # Replace __Restock_Status
    t1["restock_status"] = t1["restock_status"].replace(restock_status_map)

    # Helper function to split by commas and handle multi-designs
    def process_style_theme_motif(item, label):
        items = item.split(", ") if item else []  # Split by comma
        if len(items) == 2 or len(items) == 1:
            return items  # Keep as list for explode
        else:
            return f"Multi-{label} Design"

    # Process 'Print Style'
    t1["print_style"] = t1["print_style"].apply(lambda x: process_style_theme_motif(x, "Style"))
    t1['print_style'] = t1['print_style'].apply(lambda x: x if isinstance(x, list) else [x])
    t1[['Print_Style_1', 'Print_Style_2']] = pd.DataFrame(t1['print_style'].tolist(), index=t1.index)
    t1["Print_Style_2"]=t1['Print_Style_2'].fillna('None')
    t1.drop(columns=['print_style'], inplace=True)

    # Process 'Print Theme'
    t1["print_theme"] = t1["print_theme"].apply(lambda x: process_style_theme_motif(x, "Theme"))
    t1['print_theme'] = t1['print_theme'].apply(lambda x: x if isinstance(x, list) else [x])
    t1[['Print_Theme_1', 'Print_Theme_2']] = pd.DataFrame(t1['print_theme'].tolist(), index=t1.index)
    t1["Print_Theme_2"]=t1['Print_Theme_2'].fillna('None')
    t1=t1.drop(columns=['print_theme'])

    # Process 'Print Key Motif'
    t1["print_key_motif"] = t1["print_key_motif"].apply(lambda x: process_style_theme_motif(x, "Key Motif"))
    t1['print_key_motif'] = t1['print_key_motif'].apply(lambda x: x if isinstance(x, list) else [x])
    t1[['Print_Key_Motif_1', 'Print_Key_Motif_2']] = pd.DataFrame(t1['print_key_motif'].tolist(), index=t1.index)
    t1["Print_Key_Motif_2"]=t1['Print_Key_Motif_2'].fillna('None')
    t1=t1.drop(columns=['print_key_motif'])
    
    t1["Colour"] = t1["Colour"].apply(lambda x: x if isinstance(x, list) else [x])  # Convert string to list
    t1["Colour"] = t1["Colour"].apply(lambda x: x[:2] if len(x) >= 2 else x + ["None"])  # Ensure two elements
    t1[['Colour_1', 'Colour_2']] = pd.DataFrame(t1['Colour'].tolist(), index=t1.index)  # Expand into columns
    t1.drop(columns=['Colour'], inplace=True)  # Drop original column


    t1["print_colour"] = t1["print_colour"].apply(lambda x: process_style_theme_motif(x, "print_colour"))
    t1['print_colour'] = t1['print_colour'].apply(lambda x: x if isinstance(x, list) else [x])
    t1[['Print_Colour_1', 'Print_Colour_2']] = pd.DataFrame(t1['print_colour'].tolist(), index=t1.index)
    t1["Print_Colour_2"]=t1['Print_Colour_2'].fillna('None')
    t1=t1.drop(columns=['print_colour'])
    

    # Replace in 'Age', 'Fabric', 'Pack_Size', 'Product_Type'
    t1["Age"] = t1["Age"].replace(age_map)
    t1["Fabric"] = t1["Fabric"].replace(fabric_map)
    t1["Pack_Size"] = t1["Pack_Size"].replace(pack_size_map)
    t1["Product_Type"] = t1["Product_Type"].replace(product_type_map)

    return t1


def process_prathiksham(t1):
    cat = {'Dress,Kurta':'Kurta,Dress'}
    t1["Category"] = t1["Category"].replace(cat)
    col = {'Teal,White':'Teal,White'}
    t1["Colour"] = t1["Colour"].replace(col)
    fab = {'Cotton,Kota Doria':'Kota Doria,Cotton'}
    t1["Fabric"] = t1["Fabric"].replace(fab)
    nec = {'Collar,V-Neck':'V-Neck,Collar','Collar,V Neck':'V-Neck,Collar','V Neck,Collar':'V-Neck,Collar',
            'Collar V Neck':'V-Neck,Collar'}
    t1["Neck"] = t1["Neck"].replace(nec)
    occ = {'Brunch Wear,Casual Wear':'Casual Wear,Brunch Wear','Office Wear,Regular Wear':'Regular Wear,Office Wear'
    ,'Casual Wear,Regular Wear':'Regular Wear,Casual Wear'}
    t1["Occasion"] = t1["Occasion"].replace(occ)
    pri = {'Handblock,Stripes':'Stripes,Handblock','Floral,Stripes':'Stripes,Floral','Floral,Solid':'Solid,Floral',
    'Embroidered,Solid':'Solid,Embroidered'}
    t1["Print"] = t1["Print"].replace(pri)
    sle = {'Sleeveless,Elbow Fit':'Sleeveless,Elbow Fit'}
    t1["Sleeve"] = t1["Sleeve"].replace(sle)
    pro = {'Mad e to order':'Made To Order'}
    t1["Product_Availability"] = t1["Product_Availability"].replace(pro)

    return t1

def process_zing(t1):
    def safe_split(x):
        if pd.isna(x):  # Check if value is NaN or None
            return ''
        return ', '.join(sorted(str(item).strip() for item in str(x).split(',') if item.strip()))
    
    t1['Colour'] = t1['Colour'].apply(safe_split)
    t1['Fit'] = t1['Fit'].apply(safe_split)
    t1['Neck'] = t1['Neck'].apply(safe_split)
    t1['Occasion'] = t1['Occasion'].apply(safe_split)
    t1['Print'] = t1['Print'].apply(safe_split)
    t1['print_type'] = t1['print_type'].apply(safe_split)
    
    sleeve_dict = {
        'Three-Quarter Sleeves': 'Three-Quarter Sleeves',
        'Three Quarter Sleeves': 'Three-Quarter Sleeves',
        'Three Quarters Sleeves': 'Three-Quarter Sleeves',
        'Three Quarter Sleeve': 'Three-Quarter Sleeves',
        'Sleeveless': 'Sleeveless',
        'Elbow Sleeves': 'Elbow Sleeves',
        'Elbow Sleeve': 'Elbow Sleeves',
        'Half-Sleeve': 'Half Sleeve',
        'Half Sleeve': 'Half Sleeve',
        'Full Sleeves': 'Full Sleeves',
        'Full Sleeve': 'Full Sleeves',
        'Short Sleeves': 'Short Sleeves',
        'Short Sleeve': 'Short Sleeves',
        'Short': 'Short Sleeves',
        'Sleeveless': 'Sleeveless'
    }
    t1["Sleeve"] = t1["Sleeve"].replace(sleeve_dict)
    t1["Category"] = t1["Category"].replace({"Co-ord,Kurta Set":"Kurta Set,Co-ord"})
    
    return t1


