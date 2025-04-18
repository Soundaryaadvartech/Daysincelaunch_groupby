import traceback
import io
import json
import asyncio
from pandas import DataFrame
from typing import Optional
from fastapi.responses import StreamingResponse
from fastapi import APIRouter, Depends, status, HTTPException 
from sqlalchemy.orm import Session
from sqlalchemy import distinct
from fastapi.responses import JSONResponse
from database.database import get_db, get_business_name
from utilities.utiles_launch import generate_inventory_summary
from utilities.generic_utils import get_dynamic_db, get_models
from utilities.filter_data import get_filter_data
from pydantic import BaseModel
from io import StringIO
from utilities.columns_to_choose import get_column_names
from utilities.utlis_groupby import agg_grp



class FilterDataRequest1(BaseModel):
    filter_jason: dict

class FilterDataRequest2(BaseModel):
    filter_dict: dict
    data_dict: dict
    groupby_dict: dict

async def run_in_thread(fn, *args):
    """Run blocking function in separate thread to avoid blocking FastAPI"""
    return await asyncio.to_thread(fn, *args)

router = APIRouter()
@router.post("/inventory_summary")
async def inventory_summary(business: str,filter_request: FilterDataRequest1, days: Optional[int] = None, group_by: Optional[str] = None, db:Session = Depends(get_dynamic_db)):
    try:
        days = days or 60
        group_by = group_by or "Item_Id"
        models = get_models(business)
        print(filter_request.filter_jason)
        business = get_business_name(business)
        summary_df: DataFrame = await run_in_thread(generate_inventory_summary, db, models, days, group_by, business,filter_request.filter_jason)

        # Convert DataFrame to CSV and stream it
        stream = io.StringIO()
        summary_df.to_csv(stream, index=False)
        stream.seek(0)

        return StreamingResponse(
            stream, 
            media_type="text/csv", 
            headers={"Content-Disposition": f"attachment; filename={business}_inventory_summary.csv"}
        )
    except Exception:
        traceback.print_exc()
        return  JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content = {"message":"Something went wrong"}
        )
    
@router.post("/aggregation")
async def inventory_summary(business: str, filter_request: FilterDataRequest2, db: Session = Depends(get_dynamic_db)):
    try:
        
        models = get_models(business)
        business = get_business_name(business)
        
        # Call the aggregation function and pass necessary arguments
        summary_df: DataFrame = await run_in_thread(agg_grp,db, models, business,
            filter_request.filter_dict,  # Filter for query
            filter_request.data_dict,  # Data aggregation methods
            filter_request.groupby_dict)  # Group-by conditions

        
        csv_buffer = StringIO()
        summary_df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)

        return StreamingResponse(csv_buffer, media_type="text/csv", headers={"Content-Disposition": "attachment; filename=inventory_summary.csv"})
    except Exception as e:
        traceback.print_exc()
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"message": "Something went wrong", "error": str(e)}
        )
    
@router.post("/get_filter_data")
async def get_table(business: str, db: Session = Depends(get_dynamic_db)):
    try:
        print(f"Fetching filter data for business: {business}")  # Log business name
        print(business)
        models = get_models(business)
        business = get_business_name(business)
        print(f"Using models: {models}")  # Log models
        filter_data = await run_in_thread(get_filter_data, db, models, business)

        if filter_data.empty:  
            print("No data found!")  # Log empty response
            return JSONResponse(status_code=204, content={"message": "No data available"})

        print("Data fetched successfully!")  # Log success

        csv_buffer = StringIO()
        filter_data.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)

        return StreamingResponse(csv_buffer, media_type="text/csv", headers={"Content-Disposition": "attachment; filename=filter_data.csv"})

    except Exception as e:
        print(f"Error occurred: {e}")  # Log errors
        traceback.print_exc()
        return JSONResponse(status_code=500, content={"message": "Something went wrong"})
    
@router.post("/get_column_names")
async def get_table(business: str, db: Session = Depends(get_dynamic_db)):
    try:
        print(f"Fetching filter data for business: {business}")  # Log business name
        
        models = get_models(business)
        print(f"Using models: {models}")  # Log models
        business = get_business_name(business)
        column_name = await run_in_thread(get_column_names, db, models, business)

        if column_name.empty:  
            print("No data found!")  # Log empty response
            return JSONResponse(status_code=204, content={"message": "No data available"})

        print("Data fetched successfully!")  # Log success

        csv_buffer = StringIO()
        column_name.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)

        return StreamingResponse(csv_buffer, media_type="text/csv", headers={"Content-Disposition": "attachment; filename=column_name.csv"})

    except Exception as e:
        print(f"Error occurred: {e}")  # Log errors
        traceback.print_exc()
        return JSONResponse(status_code=500, content={"message": "Something went wrong"})
