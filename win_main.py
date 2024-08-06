from fastapi import FastAPI
from fastapi import Form, UploadFile
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from app.process import process_upload
from dotenv import load_dotenv
from loguru import logger
from uuid6 import uuid7
import os
import sys
import uvicorn
import sys

config = {
    "handlers": [
        {
            "sink": sys.stdout,
            "colorize": True,
            "format": "<green>{time:YYYY-MM-DD at HH:mm:ss}</green>|<blue><level>{level}</level></blue>|<yellow>{name}:{function}:{line}</yellow>|<cyan><b>{message}</b></cyan>",
            "level": "INFO",
        },
        {
            "sink": "file.log",
            "serialize": True,
            "backtrace": True,
            "diagnose": True,
            "level": "ERROR",
        },
    ],
}
logger.configure(**config)
load_dotenv()

app = FastAPI()

# Configuring CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow requests from any origin
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/api/v2/upload")
def handle_upload(email: str = Form(...), file: UploadFile = Form(...), group_by: str = Form(...)):
    logger.info(f"Received email: {email}")
    logger.info(f"Received file: {file.filename}")
    logger.info(f"Received group_by: {group_by}")
    
    base_name, base_ext = os.path.splitext(file.filename)
    os.makedirs(os.getenv("LOCATION"), exist_ok=True)
    base_filename = os.getenv("LOCATION") + base_name + str(uuid7()) + base_ext
    logger.info("Got file:: {}".format(base_filename))
    
    try:
        contents = file.file.read()
        logger.info("Writing to disk")
        with open(base_filename, "wb") as f:
            f.write(contents)
        logger.info("Written to disk")
    except Exception:
        logger.exception("Exception while uploading the file")
        return {"message": "There was an error uploading the file"}
    finally:
        file.file.close()
    
    # Processing the uploaded file
    (
        success,
        msg,
        url,
        skipped,
        not_existing,
        invalid,
        invalid_s3_url,
        total
    ) = process_upload(base_filename, email, group_by)
    
    # Returning JSON response
    return JSONResponse(
        {
            "success": success,
            "msg": msg,
            "email": email,
            "pre-signed": url,
            "details": {
                "total": total,
                "skipped": skipped,
                "not_existing": not_existing,
                "invalid_access": invalid,
                "invalid_s3": invalid_s3_url
            },
        }
    )

if __name__ == '__main__':
    port = sys.argv[1] if len(sys.argv) > 1 else 3000
    uvicorn.run(app, port=int(port))
