import sys
import json
from app.process import process_upload
from loguru import logger

if __name__ == "__main__":
    filename = sys.argv[1]
    email = sys.argv[2]
    group_by = sys.argv[3]

    try:
        (
            success,
            msg,
            url,
            skipped,
            not_existing,
            invalid,
            invalid_s3_url,
            total
        ) = process_upload(filename, email, group_by)

        result = {
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
        print(json.dumps(result))
    except Exception as e:
        logger.exception("Error during processing")
        print(json.dumps({"message": str(e)}))
        sys.exit(1)
