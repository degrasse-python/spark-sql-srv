from pyspark.sql import SparkSession
from fastapi import (FastAPI,
                     Depends,
                     HTTPException,
                     status)
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pydantic import BaseModel
from pyspark.sql.functions import col
from jose import JWTError, jwt
from datetime import datetime, timedelta
from passlib.context import CryptContext

# Create a SparkSession
spark = SparkSession.builder.appName("SparkSQLServer").getOrCreate()

# Create a FastAPI instance
app = FastAPI()

# Define OAuth 2.0 configuration
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/token")

# Sample user credentials for verification
USER_CREDENTIALS = {"username": "admin", "password": "password"}

# JWT settings
SECRET_KEY = "your-secret-key"  # Change this to a secure secret key
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

# Password hashing
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# Verify username and password for authentication
def verify_credentials(username: str, password: str):
    if username == USER_CREDENTIALS["username"] and verify_password(password, USER_CREDENTIALS["password"]):
        return True
    else:
        return False

# Verify password
def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)

# Hash password
def get_password_hash(password):
    return pwd_context.hash(password)

# Define token response model
class Token(BaseModel):
    access_token: str
    token_type: str

# Generate access token
def generate_token(username: str, password: str):
    if verify_credentials(username, password):
        access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
        access_token_subject = username
        access_token = create_access_token(
            data={"sub": access_token_subject}, expires_delta=access_token_expires
        )
        return Token(access_token=access_token, token_type="bearer")
    else:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

# Create access token
def create_access_token(data: dict, expires_delta: timedelta = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

# Decode access token
def decode_access_token(token: str):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username = payload.get("sub")
        return username
    except JWTError:
        return None

# Define the route to generate access token
@app.post("/token", response_model=Token)
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    token = generate_token(form_data.username, form_data.password)
    return token

# Example route to interact with the Spark SQL server
@app.get("/query", dependencies=[Depends(oauth2_scheme)])
async def run_query(token: str, query: str):
    # Check the validity of the access token
    if not verify_token(token):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid access token",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Execute the provided query against the Spark
    result = spark.sql(query)

    # Convert the result to a list of dictionaries for JSON response
    result_json = [row.asDict() for row in result.collect()]

    return {"result": result_json}

# Verify access token
def verify_token(token: str):
    username = decode_access_token(token)
    # Your token verification logic here
    # You can check if the token is valid and belongs to a valid user
    # For simplicity, we're only checking if the username exists
    if username:
        return True
    else:
        return False

# Main function to run the Spark SQL server
if __name__ == "__main__":
    # Start the Spark session
    spark_session = spark.builder.appName("SparkSQLServer").getOrCreate()

    # TODO Spark SQL operations here
    # 

    # Stop the Spark session
    spark_session.stop()

