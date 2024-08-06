from app.process import process_csv
from dotenv import load_dotenv
from loguru import logger
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

if __name__ == '__main__':
    if len(sys.argv) < 2:
        logger.error("Usage is python run_csv.py <csv>")
    else:
        logger.info("args are:: {}".format(sys.argv))
        process_csv(sys.argv[1])