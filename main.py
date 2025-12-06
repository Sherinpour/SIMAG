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
    server = os.getenv('DB_SERVER')
    database = os.getenv('DB_NAME')
    username = os.getenv('DB_USERNAME')
    password = os.getenv('DB_PASSWORD')
    
    if not all([server, database, username, password]):
        raise ValueError("Database credentials not found in environment variables")
    
    conn_str = (
        f"mssql+pyodbc://{quote_plus(username)}:{quote_plus(password)}"
        f"@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
    )
    
    return create_engine(conn_str)


def fetch_data_from_db(event_id: int):
    """Fetch data from database and return as DataFrame"""
    query = text("""
    SELECT [ID],
           [FirstName],
           [LastName],
           [BankTitle],
           [Post],
           [OrganizationTitle],
           [OrganizationTypeTitle],
           [CompanyTitle],
           [HoldingTitle]
    FROM [GEMS].[dbo].[vw_Guest_AI]
    WHERE [EventId] = :event_id
    """)
    
    engine = None
    try:
        engine = get_db_connection()
        logger.info("Connected to database successfully!")
        
        df = pd.read_sql(query, engine, params={'event_id': event_id})
        logger.info(f"Fetched {len(df)} records from database for EventId={event_id}")
        
        return df
    except Exception as e:
        logger.error(f"Error fetching data from database: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    finally:
        if engine:
            engine.dispose()


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
        df = fetch_data_from_db(request.id)
        
        if df.empty:
            return SimilarNamesResponse(
                total_pairs=0,
                pairs=[]
            )
        
        # ✅ 2. Processor settings
        settings = Settings(
            name_threshold=request.name_threshold,
            last_name_weight=request.last_weight,
            first_name_weight=request.first_weight,
            org_weight=request.org_weight
        )
        
        # ✅ 3. Create processor and load data
        logger.info("Initializing name processor...")
        processor = SmartNameProcessor(settings=settings)
        processor.df = df
        processor.input_file_format = 'dataframe'  # Mark as in-memory dataframe
        
        # ✅ 4. Process names
        logger.info("Processing names (removing prefixes, correcting text)...")
        processor.process_names()
        
        # ✅ 5. Extract stop names
        logger.info("Extracting stop first names...")
        processor.extract_stop_first_names(min_frequency=request.min_freq)
        
        # ✅ 6. Find similar names
        logger.info("Finding similar names...")
        df_result = processor.find_similar_names(output_path=None)  # Don't save to file
        
        # ✅ 7. Convert to JSON
        if df_result.empty:
            return SimilarNamesResponse(
                total_pairs=0,
                pairs=[]
            )
        
        # Convert DataFrame to list of dictionaries
        pairs = []
        for _, row in df_result.iterrows():
            pair = SimilarNamePair(
                name1=row["نام اول"],
                post1=row["پست اول"],
                org1=row["سازمان اول"],
                org_type1=row["نوع سازمان اول"],
                company1=row["عنوان شرکت اول"],
                holding1=row["عنوان هولدینگ اول"],
                name2=row["نام دوم"],
                post2=row["پست دوم"],
                org2=row["سازمان دوم"],
                org_type2=row["نوع سازمان دوم"],
                company2=row["عنوان شرکت دوم"],
                holding2=row["عنوان هولدینگ دوم"],
                similarity_score=float(row["درصد تشابه"])
            )
            pairs.append(pair)
        
        logger.info(f"Found {len(pairs)} similar name pairs")
        
        return SimilarNamesResponse(
            total_pairs=len(pairs),
            pairs=pairs
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in process_and_find_similar: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Processing error: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

