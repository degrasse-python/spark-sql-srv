from fastapi import (FastAPI,
                     Depends,
                     HTTPException,
                     status)
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pydantic import BaseModel
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("DataConcatenator").getOrCreate()

# Create a FastAPI instance
app = FastAPI()

# Define OAuth 2.0 configuration
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/token")

# Verify username and password for authentication
def verify_credentials(username: str, password: str):
    correct_username = "your_username"
    correct_password = "your_password"
    if username == correct_username and password == correct_password:
        return True
    else:
        return False

# Define token response model
class Token(BaseModel):
    access_token: str
    token_type: str

# Generate access token
def generate_token(username: str, password: str):
    if verify_credentials(username, password):
        return Token(access_token=username, token_type="bearer")
    else:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

# Define the route to generate access token
@app.post("/token", response_model=Token)
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    token = generate_token(form_data.username, form_data.password)
    return token

# Define the route to receive and concatenate data
@app.post("/concatenate")
async def concatenate_data(data: str,
                            token: str = Depends(oauth2_scheme)):
    # Check the validity of the access token
    if token != "your_username":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid access token",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Load existing data from Spark SQL database
    existing_df = spark.read.parquet("path/to/existing/database.parquet")

    # Create a DataFrame from the incoming data
    incoming_df = spark.createDataFrame([(data,)], ["data"])

    # Concatenate the data frames
    concatenated_df = existing_df.union(incoming_df)

    # Write the concatenated DataFrame back to the Spark SQL database
    concatenated_df.write.mode("append").parquet("path/to/existing/database.parquet")

    return {"message": "Data concatenated successfully."}
