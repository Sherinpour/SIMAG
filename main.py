from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import Optional, List
import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from smart_name_matcher import SmartNameProcessor, Settings
import logging
# from datetime import datetime

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(
    title="SIMAG API",
    description="API for Smart Name Matching and Database Connection",
    version="1.0.0"
)


# ✅ ---------------- PYDANTIC MODELS ----------------
class SimilarNamePair(BaseModel):
    """Model for a pair of similar names"""
    name1: str = Field(..., description="First name")
    post1: str = Field(..., description="First post")
    org1: str = Field(..., description="First organization")
    org_type1: str = Field(..., description="First organization type")
    company1: str = Field(..., description="First company title")
    holding1: str = Field(..., description="First holding title")
    name2: str = Field(..., description="Second name")
    post2: str = Field(..., description="Second post")
    org2: str = Field(..., description="Second organization")
    org_type2: str = Field(..., description="Second organization type")
    company2: str = Field(..., description="Second company title")
    holding2: str = Field(..., description="Second holding title")
    similarity_score: float = Field(..., description="Similarity percentage")


class SimilarNamesResponse(BaseModel):
    """Response model for similar names"""
    total_pairs: int = Field(..., description="Total number of similar pairs")
    pairs: List[SimilarNamePair] = Field(..., description="List of similar pairs")


class ProcessRequest(BaseModel):
    """Request model for processing names"""
    id: int = Field(..., description="Event ID to filter data")
    name_threshold: Optional[float] = Field(0.80, description="Name similarity threshold")
    last_weight: Optional[float] = Field(0.5, description="Last name weight")
    first_weight: Optional[float] = Field(0.2, description="First name weight")
    org_weight: Optional[float] = Field(0.3, description="Organization weight")
    min_freq: Optional[int] = Field(3, description="Minimum frequency for stop names")


# ✅ ---------------- DATABASE FUNCTIONS ----------------
def get_db_connection():
    """Create and return database connection"""
    logger.info("🔵 [STEP 1] Starting database connection setup...")
    
    server = os.getenv('DB_SERVER')
    database = os.getenv('DB_NAME')
    username = os.getenv('DB_USERNAME')
    password = os.getenv('DB_PASSWORD')
    
    logger.info(f"🔵 [STEP 2] Environment variables loaded - Server: {server}, Database: {database}, Username: {username}")
    
    if not all([server, database, username, password]):
        logger.error("❌ [ERROR] Database credentials not found in environment variables")
        raise ValueError("Database credentials not found in environment variables")
    
    logger.info("🔵 [STEP 3] Building connection string...")
    conn_str = (
        f"mssql+pyodbc://{quote_plus(username)}:{quote_plus(password)}"
        f"@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
    )
    
    logger.info("🔵 [STEP 4] Creating SQLAlchemy engine...")
    try:
        engine = create_engine(conn_str)
        logger.info("✅ [SUCCESS] Engine created successfully (connection not yet established)")
        return engine
    except Exception as e:
        logger.error(f"❌ [ERROR] Failed to create engine at STEP 4: {e}", exc_info=True)
        raise


def fetch_data_from_db(event_id: int):
    """Fetch data from database and return as DataFrame"""
    logger.info(f"🟢 [FETCH_STEP 1] Preparing query for EventId={event_id}...")
    
    # Get database name from environment variables
    database = os.getenv('DB_NAME')
    if not database:
        error_msg = "❌ [ERROR] DB_NAME not found in environment variables"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    logger.info(f"🟢 [FETCH_STEP 1.5] Using database name from env: {database}")
    
    query = text(f"""
    SELECT [ID],
           [FirstName],
           [LastName],
           [BankTitle],
           [Post],
           [OrganizationTitle],
           [OrganizationTypeTitle],
           [CompanyTitle],
           [HoldingTitle]
    FROM [{database}].[dbo].[vw_Guest_AI]
    WHERE [EventId] = :event_id
    """)
    
    engine = None
    try:
        logger.info(f"🟢 [FETCH_STEP 2] Calling get_db_connection() for EventId={event_id}...")
        engine = get_db_connection()
        logger.info(f"✅ [FETCH_STEP 3] Engine obtained successfully for EventId={event_id}")
        
        # ✅ First, check if view exists and has any data
        logger.info(f"🔍 [CHECK] Checking if view [{database}].[dbo].[vw_Guest_AI] exists and has data...")
        try:
            check_query = text(f"SELECT COUNT(*) as total_count FROM [{database}].[dbo].[vw_Guest_AI]")
            count_df = pd.read_sql(check_query, engine)
            total_count = count_df.iloc[0]['total_count'] if not count_df.empty else 0
            logger.info(f"🔍 [CHECK] Total records in view: {total_count}")
        except Exception as check_error:
            logger.warning(f"⚠️ [WARNING] Could not check view total count: {check_error}")
            logger.warning(f"⚠️ [WARNING] Continuing with main query despite check failure...")
        
        # ✅ Check available EventIds
        logger.info(f"🔍 [CHECK] Checking available EventIds in view...")
        try:
            eventid_query = text(f"SELECT DISTINCT [EventId] FROM [{database}].[dbo].[vw_Guest_AI] ORDER BY [EventId]")
            eventid_df = pd.read_sql(eventid_query, engine)
            available_eventids = eventid_df['EventId'].tolist() if not eventid_df.empty else []
            logger.info(f"🔍 [CHECK] Available EventIds in view: {available_eventids}")
            logger.info(f"🔍 [CHECK] Requested EventId={event_id} is {'✅ FOUND' if event_id in available_eventids else '❌ NOT FOUND'} in available EventIds")
        except Exception as eventid_error:
            logger.warning(f"⚠️ [WARNING] Could not check available EventIds: {eventid_error}")
            logger.warning(f"⚠️ [WARNING] Continuing with main query despite EventId check failure...")
        
        logger.info(f"🟢 [FETCH_STEP 4] Attempting to establish connection and execute query for EventId={event_id}...")
        logger.info(f"🟢 [FETCH_STEP 4] Query: SELECT ... FROM [{database}].[dbo].[vw_Guest_AI] WHERE [EventId] = {event_id}")
        logger.info(f"🟢 [FETCH_STEP 4] This is where the actual database connection happens (pd.read_sql)...")
        df = pd.read_sql(query, engine, params={'event_id': event_id})
        logger.info(f"✅ [FETCH_STEP 5] Query executed successfully! Fetched {len(df)} records from database for EventId={event_id}")
        
        if df.empty:
            logger.warning(f"⚠️ [WARNING] Query returned EMPTY result for EventId={event_id}")
            logger.warning(f"⚠️ [WARNING] This means no records match the EventId={event_id} in the view")
        else:
            logger.info(f"✅ [SUCCESS] Query returned {len(df)} records with columns: {list(df.columns)}")
            # Show first few rows info
            logger.info(f"📊 [DATA] First row sample: {df.iloc[0].to_dict() if len(df) > 0 else 'N/A'}")
        
        # ✅ Save fetched data to file for inspection
        # COMMENTED: Disabled Excel file saving
        # timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        # try:
        #     output_file = f"fetched_data_EventId_{event_id}_{timestamp}.xlsx"
        #     df.to_excel(output_file, index=False, engine='openpyxl')
        #     logger.info(f"💾 [SAVE] Fetched data saved to file: {output_file}")
        #     logger.info(f"💾 [SAVE] File contains {len(df)} rows and {len(df.columns)} columns")
        #     logger.info(f"💾 [SAVE] Columns: {list(df.columns)}")
        #     
        #     # If empty, also save a diagnostic file
        #     if df.empty:
        #         logger.warning(f"⚠️ [WARNING] Saving empty result - this means EventId={event_id} has no matching records")
        #         # Try to get sample data without EventId filter to see what's available
        #         try:
        #             sample_query = text("SELECT TOP 10 * FROM [GEMS].[dbo].[vw_Guest_AI]")
        #             sample_df = pd.read_sql(sample_query, engine)
        #             sample_file = f"sample_data_all_EventIds_{timestamp}.xlsx"
        #             sample_df.to_excel(sample_file, index=False, engine='openpyxl')
        #             logger.info(f"💾 [SAMPLE] Sample data (first 10 rows, all EventIds) saved to: {sample_file}")
        #             if 'EventId' in sample_df.columns:
        #                 logger.info(f"💾 [SAMPLE] EventIds in sample: {sample_df['EventId'].unique().tolist()}")
        #         except Exception as sample_error:
        #             logger.warning(f"⚠️ [WARNING] Could not fetch sample data: {sample_error}")
        #     else:
        #         # Show summary statistics
        #         logger.info(f"📊 [DATA] Data summary:")
        #         logger.info(f"📊 [DATA] - Total rows: {len(df)}")
        #         logger.info(f"📊 [DATA] - Columns with data: {[col for col in df.columns if not df[col].isna().all()]}")
        #         logger.info(f"📊 [DATA] - Columns all null: {[col for col in df.columns if df[col].isna().all()]}")
        # except Exception as save_error:
        #     logger.warning(f"⚠️ [WARNING] Could not save fetched data to Excel file: {save_error}")
        #     # Try CSV as fallback
        #     try:
        #         output_file_csv = f"fetched_data_EventId_{event_id}_{timestamp}.csv"
        #         df.to_csv(output_file_csv, index=False, encoding='utf-8-sig')
        #         logger.info(f"💾 [SAVE] Fetched data saved to CSV file: {output_file_csv}")
        #         logger.info(f"💾 [SAVE] File contains {len(df)} rows and {len(df.columns)} columns")
        #         logger.info(f"💾 [SAVE] Columns: {list(df.columns)}")
        #     except Exception as csv_error:
        #         logger.error(f"❌ [ERROR] Could not save to CSV either: {csv_error}")
        
        # Show summary statistics (without saving to file)
        if not df.empty:
            logger.info(f"📊 [DATA] Data summary:")
            logger.info(f"📊 [DATA] - Total rows: {len(df)}")
            logger.info(f"📊 [DATA] - Columns with data: {[col for col in df.columns if not df[col].isna().all()]}")
            logger.info(f"📊 [DATA] - Columns all null: {[col for col in df.columns if df[col].isna().all()]}")
        
        return df
    except HTTPException:
        # Re-raise HTTP exceptions as-is
        raise
    except ValueError as ve:
        # Configuration errors should be raised
        logger.error(f"❌ [ERROR] Configuration error in fetch_data_from_db for EventId={event_id}: {ve}")
        raise HTTPException(status_code=400, detail=f"Configuration error: {str(ve)}")
    except Exception as e:
        logger.error(f"❌ [ERROR] Error occurred in fetch_data_from_db for EventId={event_id}")
        logger.error(f"❌ [ERROR] Error type: {type(e).__name__}")
        logger.error(f"❌ [ERROR] Error message: {e}")
        logger.error(f"❌ [ERROR] Full traceback:", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    finally:
        if engine:
            try:
                logger.info(f"🟡 [CLEANUP] Disposing engine for EventId={event_id}...")
                engine.dispose()
            except Exception as cleanup_error:
                logger.warning(f"⚠️ [WARNING] Error during engine cleanup: {cleanup_error}")


# ✅ ---------------- API ENDPOINTS ----------------
@app.post("/find-similar-names", response_model=SimilarNamesResponse)
async def process_and_find_similar(request: ProcessRequest):
    """
    Fetch data from database, process names and find similar names
    
    This endpoint:
    1. Fetches data from database
    2. Processes names (removes prefixes, corrects text)
    3. Finds similar names
    4. Returns result as JSON
    """
    try:
        # ✅ 1. Fetch data from database
        logger.info(f"Fetching data from database for EventId={request.id}...")
        try:
            df = fetch_data_from_db(request.id)
        except HTTPException:
            # Re-raise HTTP exceptions (already properly formatted)
            raise
        except Exception as db_error:
            logger.error(f"❌ [ERROR] Database fetch failed for EventId={request.id}: {db_error}")
            logger.error(f"❌ [ERROR] Error type: {type(db_error).__name__}")
            raise HTTPException(status_code=500, detail=f"Failed to fetch data from database: {str(db_error)}")
        
        if df.empty:
            logger.warning(f"⚠️ [WARNING] No data found for EventId={request.id}, returning empty result")
            return SimilarNamesResponse(
                total_pairs=0,
                pairs=[]
            )
        
        # ✅ 2. Processor settings
        try:
            settings = Settings(
                name_threshold=request.name_threshold,
                last_name_weight=request.last_weight,
                first_name_weight=request.first_weight,
                org_weight=request.org_weight
            )
        except Exception as settings_error:
            logger.error(f"❌ [ERROR] Failed to create settings: {settings_error}")
            raise HTTPException(status_code=400, detail=f"Invalid settings: {str(settings_error)}")
        
        # ✅ 3. Create processor and load data
        logger.info("Initializing name processor...")
        try:
            processor = SmartNameProcessor(settings=settings)
            processor.df = df
            processor.input_file_format = 'dataframe'  # Mark as in-memory dataframe
        except Exception as processor_error:
            logger.error(f"❌ [ERROR] Failed to initialize processor: {processor_error}")
            raise HTTPException(status_code=500, detail=f"Failed to initialize processor: {str(processor_error)}")
        
        # ✅ 4. Process names
        logger.info("Processing names (removing prefixes, correcting text)...")
        try:
            processor.process_names()
        except Exception as process_error:
            logger.error(f"❌ [ERROR] Failed to process names: {process_error}")
            logger.warning(f"⚠️ [WARNING] Continuing with unprocessed names...")
            # Continue with unprocessed names instead of failing completely
        
        # ✅ 5. Extract stop names
        logger.info("Extracting stop first names...")
        try:
            processor.extract_stop_first_names(min_frequency=request.min_freq)
        except Exception as stop_error:
            logger.warning(f"⚠️ [WARNING] Failed to extract stop names: {stop_error}")
            logger.warning(f"⚠️ [WARNING] Continuing without stop names filter...")
            # Continue without stop names if extraction fails
        
        # ✅ 6. Find similar names
        logger.info("Finding similar names...")
        try:
            df_result = processor.find_similar_names(output_path=None)  # Don't save to file
        except Exception as find_error:
            logger.error(f"❌ [ERROR] Failed to find similar names: {find_error}")
            logger.error(f"❌ [ERROR] Error type: {type(find_error).__name__}")
            raise HTTPException(status_code=500, detail=f"Failed to find similar names: {str(find_error)}")
        
        # ✅ 7. Convert to JSON
        if df_result.empty:
            logger.info(f"ℹ️ [INFO] No similar names found for EventId={request.id}")
            return SimilarNamesResponse(
                total_pairs=0,
                pairs=[]
            )
        
        # Convert DataFrame to list of dictionaries
        pairs = []
        try:
            for _, row in df_result.iterrows():
                try:
                    pair = SimilarNamePair(
                        name1=str(row.get("نام اول", "")),
                        post1=str(row.get("پست اول", "")),
                        org1=str(row.get("سازمان اول", "")),
                        org_type1=str(row.get("نوع سازمان اول", "")),
                        company1=str(row.get("عنوان شرکت اول", "")),
                        holding1=str(row.get("عنوان هولدینگ اول", "")),
                        name2=str(row.get("نام دوم", "")),
                        post2=str(row.get("پست دوم", "")),
                        org2=str(row.get("سازمان دوم", "")),
                        org_type2=str(row.get("نوع سازمان دوم", "")),
                        company2=str(row.get("عنوان شرکت دوم", "")),
                        holding2=str(row.get("عنوان هولدینگ دوم", "")),
                        similarity_score=float(row.get("درصد تشابه", 0.0))
                    )
                    pairs.append(pair)
                except Exception as pair_error:
                    logger.warning(f"⚠️ [WARNING] Failed to process one pair: {pair_error}")
                    logger.warning(f"⚠️ [WARNING] Skipping this pair and continuing...")
                    continue
        except Exception as conversion_error:
            logger.error(f"❌ [ERROR] Failed to convert results to JSON format: {conversion_error}")
            raise HTTPException(status_code=500, detail=f"Failed to convert results: {str(conversion_error)}")
        
        logger.info(f"✅ [SUCCESS] Found {len(pairs)} similar name pairs")
        
        return SimilarNamesResponse(
            total_pairs=len(pairs),
            pairs=pairs
        )
        
    except HTTPException:
        # Re-raise HTTP exceptions as-is (already properly formatted)
        raise
    except Exception as e:
        logger.error(f"❌ [ERROR] Unexpected error in process_and_find_similar: {e}", exc_info=True)
        logger.error(f"❌ [ERROR] Error type: {type(e).__name__}")
        raise HTTPException(status_code=500, detail=f"Unexpected processing error: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

