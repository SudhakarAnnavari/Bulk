from fastapi import FastAPI, Form, UploadFile
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from loguru import logger
from uuid6 import uuid7
import os
import sys
import subprocess
import json

import uvicorn

# Configure logging
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
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["POST"],
    allow_headers=["*"],
)

import subprocess
import json
from loguru import logger

def run_subprocess(filename, email, group_by):
    script_path = os.path.abspath('processcall.py')
    command = [sys.executable, script_path, filename, email, group_by]
    logger.info(f"Running subprocess: {' '.join(command)}")

    try:
        result = subprocess.run(command, capture_output=True, text=True)
        if result.returncode != 0:
            logger.error(f"Subprocess failed with error: {result.stderr}")
            raise Exception(result.stderr)

        # Split output by newline and find the line with JSON data
        output_lines = result.stdout.splitlines()
        json_output = None
        for line in output_lines:
            line = line.strip()
            if line.startswith('{') and line.endswith('}'):
                json_output = line
                break
        
        if json_output is None:
            raise ValueError("No valid JSON found in subprocess output")

        logger.info(f"Subprocess output: {json_output}")
        return json.loads(json_output)

    except subprocess.CalledProcessError as e:
        logger.error(f"Subprocess failed with exit code {e.returncode}: {e.stderr}")
        raise Exception(result.stderr)
    except (json.JSONDecodeError, ValueError) as e:
        logger.error(f"Failed to decode JSON output: {result.stdout}")
        raise Exception(f"Failed to decode JSON output: {result.stdout}")


@app.post("/api/v2/upload")
async def handle_upload(email: str = Form(...), file: UploadFile = Form(...), group_by: str = Form(...)):
    logger.info(f"Received email: {email}")
    logger.info(f"Received file: {file.filename}")
    logger.info(f"Received group_by: {group_by}")

    base_name, base_ext = os.path.splitext(file.filename)
    os.makedirs(os.getenv("LOCATION"), exist_ok=True)
    base_filename = os.path.join(os.getenv("LOCATION"), base_name + str(uuid7()) + base_ext)
    logger.info(f"Got file:: {base_filename}")

    try:
        contents = await file.read()
        logger.info("Writing to disk")
        with open(base_filename, "wb") as f:
            f.write(contents)
        logger.info("Written to disk")
    except Exception:
        logger.exception("Exception while uploading the file")
        return {"message": "There was an error uploading the file"}
    
    try:
        result = run_subprocess(base_filename, email, group_by)
    except Exception as e:
        logger.exception("Error during subprocess execution")
        return JSONResponse({"message": str(e)}, status_code=500)
    
    return JSONResponse(result)

if __name__ == '__main__':
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 3000
    uvicorn.run(app, port=port)
