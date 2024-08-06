# import os
# import shutil
# import bs4
# from tqdm import tqdm
# import boto3
# import botocore
# from datetime import datetime
# import pyzipper
# import random
# from app.zohomail import send_zoho_mail
# import unicodedata
# import re
# import numpy as np
# from concurrent.futures import ThreadPoolExecutor
# import math
# from loguru import logger
# from glob import glob
# from playwright.sync_api import sync_playwright
# from urllib.parse import urlparse


# AWS_ACCESS_KEY_ID = "AKIAWVKQBF2L4K74ODER"
# AWS_SECRET_ACCESS_KEY = "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"
# AWS_REGION = "ap-south-1"
# EXPIRY_TIME = 7 * 24 * 60 * 60


# def slugify(value, allow_unicode=False):
#     """
#     Taken from https://github.com/django/django/blob/master/django/utils/text.py
#     Convert to ASCII if 'allow_unicode' is False. Convert spaces or repeated
#     dashes to single dashes. Remove characters that aren't alphanumerics,
#     underscores, or hyphens. Convert to lowercase. Also strip leading and
#     trailing whitespace, dashes, and underscores.
#     """
#     value = str(value)
#     if allow_unicode:
#         value = unicodedata.normalize("NFKC", value)
#     else:
#         value = (
#             unicodedata.normalize("NFKD", value)
#             .encode("ascii", "ignore")
#             .decode("ascii")
#         )
#     value = re.sub(r"[^\w\s-]", "", value.lower())
#     return re.sub(r"[-\s]+", "-", value).strip("-_")


# def find_bucket_key(s3_path):
#     """
#     This is a helper function that given an s3 path such that the path is of
#     the form: bucket/key
#     It will return the bucket and the key represented by the s3 path
#     """
#     s3_components = s3_path.split("/")
#     bucket = s3_components[0].split(".")[0]
#     if bucket == "s3":
#         bucket = s3_components[0]
#     s3_key = ""
#     if len(s3_components) > 1:
#         s3_key = "/".join(s3_components[1:])
#     return bucket, s3_key


# def split_s3_bucket_key(s3_path: str):
#     """Split s3 path into bucket and key prefix.
#     This will also handle the s3:// prefix.
#     :return: Tuple of ('bucketname', 'keyname')
#     """
#     if s3_path.startswith("s3://"):
#         s3_path = s3_path[5:]
#     if s3_path.startswith("https://"):
#         s3_path = s3_path[8:]
#     return find_bucket_key(s3_path)


# def download_files(filename, group_by):
#     base = os.path.join(
#         os.getenv("LOCATION"), os.path.splitext(os.path.basename(filename))[0]
#     )
#     logger.info("Creating base folder as :: {}".format(base))
#     if os.path.exists(base):
#         shutil.rmtree(base)
#     os.makedirs(base)
#     logger.info("Reading file")
#     try:
#         with open(filename, "r") as reader:
#             lines = reader.readlines()
#             lines = lines[1:]
#     except UnicodeDecodeError:
#         with open(filename, "r", encoding="windows-1252") as reader:
#             lines = reader.readlines()
#             lines = lines[1:]
#     logger.info("Downloading s3 links")
#     s3 = boto3.resource(
#         "s3",
#         aws_access_key_id=AWS_ACCESS_KEY_ID,
#         aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
#         region_name=AWS_REGION,
#     )
#     max_workers = math.ceil(len(lines) / 200)
#     logger.info(
#         "Dividing into workers :: {} for total:: {}".format(max_workers, len(lines))
#     )
#     chunks = np.array_split(lines, max_workers)

#     def fetcher(chunk):
#         skipped = {"Error": {}}
#         not_existing = []
#         invalid = []
#         invalid_s3_url = []
#         for line in tqdm(chunk):
#             line = line.strip()
#             vals = line.split(",")
#             if len(vals) < 3:
#                 invalid.append(line)
#                 continue
#             try:
#                 parsed_url = urlparse(vals[0].strip())
#                 bucket = parsed_url.netloc.split('.')[0]
#                 path = parsed_url.path + ('?' + parsed_url.query if parsed_url.query else '') + ('#' + parsed_url.fragment if parsed_url.fragment else '')
#                 key = path.lstrip('/')
#                 group_value = vals[2].strip() if group_by == "airline" else vals[3].strip()
#                 target_dir = os.path.join(base, group_value)
#                 os.makedirs(target_dir, exist_ok=True)
#                 oname = vals[1].strip()
#                 tname = os.path.join(target_dir, oname)
#                 if not os.path.exists(tname):
#                     s3.Bucket(bucket).download_file(key, tname)
#             except botocore.exceptions.ClientError as e:
#                 if e.response["Error"]["Code"] == "404":
#                     not_existing.append(line)
#                 else:
#                     err_code = e.response["Error"]["Code"]
#                     if err_code not in skipped["Error"]:
#                         skipped["Error"][err_code] = []
#                     skipped["Error"][err_code].append(line)
#             except botocore.exceptions.ParamValidationError:
#                 invalid_s3_url.append(line)
#             except FileNotFoundError:
#                 if "FileNotFound" not in skipped["Error"]:
#                     skipped["Error"]["FileNotFound"] = []
#                 skipped["Error"]["FileNotFound"].append(line)
#             except ValueError:
#                 invalid_s3_url.append(line)
#         return (base, skipped, not_existing, invalid, invalid_s3_url, len(chunk))

#     results = []
#     with ThreadPoolExecutor(max_workers=max_workers) as executor:
#         for result in tqdm(executor.map(fetcher, chunks), total=len(chunks)):
#             results.append(result)
#     base = ""
#     skipped = {}
#     not_existing = []
#     invalid = []
#     invalid_s3_url = []
#     total = 0
#     for _ in range(0, max_workers + 5):
#         print("\n")
#     for result in results:
#         base = result[0]
#         skipped.update(result[1])
#         not_existing.extend(result[2])
#         invalid.extend(result[3])
#         invalid_s3_url.extend(result[4])
#         total += result[5]
#     return base, skipped, not_existing, invalid, invalid_s3_url, total

# def zip_folder(source: str, output_zip: str = ""):
#     print("Zipping folder")
#     password = None
#     if not output_zip:
#         output_zip = source + ".zip"
#     with pyzipper.AESZipFile(
#         output_zip, "w", compression=pyzipper.ZIP_STORED
#     ) as zip_file:
#         for root, _, files in os.walk(source):
#             for file in files:
#                 file_path = os.path.join(root, file)
#                 zip_file.write(file_path, arcname=os.path.relpath(file_path, source))
#     return output_zip, password



# def upload_to_s3(zip_file):
#     """this will upload the zip to the s3"""
#     logger.info("Uploading to s3")
#     upload_name = datetime.now().strftime("%Y%m%d%H%M%S%f") + "-invoices.zip"
#     bucket = "finkraft-invoices-outstation"
#     s3 = boto3.resource(
#         "s3",
#         aws_access_key_id=AWS_ACCESS_KEY_ID,
#         aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
#         region_name=AWS_REGION,
#     )
#     key = "downloadables/" + os.path.basename(upload_name)
#     s3.meta.client.upload_file(
#         zip_file,
#         bucket,
#         key,
#     )
#     return bucket, key


# def generate_presigned_url(bucket, key):
#     """this will generate a presigned url"""
#     logger.info("Generating pre-signed url")
#     s3 = boto3.client(
#         "s3",
#         aws_access_key_id=AWS_ACCESS_KEY_ID,
#         aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
#         region_name=AWS_REGION,
#     )
#     url = s3.generate_presigned_url(
#         ClientMethod="get_object",
#         Params={"Bucket": bucket, "Key": key},
#         ExpiresIn=EXPIRY_TIME,  # seconds
#     )
#     return url


# def get_skipped_count(skipped):
#     check = skipped["Error"]
#     val = 0
#     for key in check:
#         val += len(check[key])
#     return val


# def convert_html_to_pdf(folder):
#     logger.info("Converting html to pdf in folder:: {}".format(folder))
#     with sync_playwright() as p:
#         browser = p.chromium.launch(headless=True)
#         page = browser.new_page()
#         for ifile in tqdm(glob(folder + "/**/*.html", recursive=True)):
#             iname = os.path.abspath(ifile)
#             oname = os.path.splitext(iname)[0] + ".pdf"
#             page.emulate_media(media='print')
#             page.goto(f"file:///{iname}")
#             page.pdf(path=oname)
#             os.remove(iname)

# def send_mail(url,
#             password,
#             total,
#             unprocessed,
#             email,
#             skipped,
#             not_existing,
#             invalid,
#             invalid_s3_url,):
#     success = True
#     msg = "Successfully downloaded and sent"
#     try:
#         send_zoho_mail(
#             url,
#             password,
#             total,
#             unprocessed,
#             email,
#             skipped,
#             not_existing,
#             invalid,
#             invalid_s3_url,
#         )
#     except Exception as e:
#         print("send mail error ",e)
#         import traceback
#         success = False
#         msg = traceback.format_exc()
#     return success, msg





# def process_upload(filename, email, group_by):
#     """this will process the uploaded file"""
#     logger.info("Processing file")
#     base, skipped, not_existing, invalid, invalid_s3_url, total = download_files(
#         filename, group_by
#     )
#     logger.info("Downloaded to {}".format(base))
#     convert_html_to_pdf(base)
#     zip, password = zip_folder(base)
#     bucket, key = upload_to_s3(zip)
#     url = generate_presigned_url(bucket, key)
#     unprocessed = (
#         get_skipped_count(skipped)
#         + len(not_existing)
#         + len(invalid)
#         + len(invalid_s3_url)
#     )
#     success, msg = send_mail(url,
#             password,
#             total,
#             unprocessed,
#             email,
#             skipped,
#             not_existing,
#             invalid,
#             invalid_s3_url,)
#     success = True
#     msg = "sent"
#     if os.path.exists(filename):
#         os.remove(filename)
#     if os.path.exists(zip):
#         os.remove(zip)
#     if os.path.exists(base):
#         shutil.rmtree(base)
#     return success, msg, url, skipped, not_existing, invalid, invalid_s3_url, total


# def process_csv(filename):
#     logger.info("Processing file:: {}".format(filename))
#     base, skipped, not_existing, invalid, invalid_s3_url, total = download_files(
#         filename
#     )
#     convert_html_to_pdf(base)
#     results = {
#         'base' : base,
#         'skipped' : skipped,
#         'not_existing' : not_existing,
#         'invalid' : invalid,
#         'invalid_s3_url' : invalid_s3_url,
#         'total' : total,
#     }
#     import json
#     with open('results.json', 'w') as writer:
#         json.dump(results, writer, default=str, indent=4)       



# -------------------------------------------------------------------------


import csv
import os
import shutil
import bs4
from tqdm import tqdm
import boto3
import botocore
from datetime import datetime
import pyzipper
import random
from app.zohomail import send_zoho_mail
import unicodedata
import re
import numpy as np
from concurrent.futures import ThreadPoolExecutor
import math
from loguru import logger
from glob import glob
from playwright.sync_api import sync_playwright
from urllib.parse import urlparse


AWS_ACCESS_KEY_ID = "AKIAWVKQBF2L4K74ODER"
AWS_SECRET_ACCESS_KEY = "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"
AWS_REGION = "ap-south-1"
EXPIRY_TIME = 7 * 24 * 60 * 60


def slugify(value, allow_unicode=False):
    """
    Taken from https://github.com/django/django/blob/master/django/utils/text.py
    Convert to ASCII if 'allow_unicode' is False. Convert spaces or repeated
    dashes to single dashes. Remove characters that aren't alphanumerics,
    underscores, or hyphens. Convert to lowercase. Also strip leading and
    trailing whitespace, dashes, and underscores.
    """
    value = str(value)
    if allow_unicode:
        value = unicodedata.normalize("NFKC", value)
    else:
        value = (
            unicodedata.normalize("NFKD", value)
            .encode("ascii", "ignore")
            .decode("ascii")
        )
    value = re.sub(r"[^\w\s-]", "", value.lower())
    return re.sub(r"[-\s]+", "-", value).strip("-_")


def find_bucket_key(s3_path):
    """
    This is a helper function that given an s3 path such that the path is of
    the form: bucket/key
    It will return the bucket and the key represented by the s3 path
    """
    s3_components = s3_path.split("/")
    bucket = s3_components[0].split(".")[0]
    if bucket == "s3":
        bucket = s3_components[0]
    s3_key = ""
    if len(s3_components) > 1:
        s3_key = "/".join(s3_components[1:])
    return bucket, s3_key


def split_s3_bucket_key(s3_path: str):
    """Split s3 path into bucket and key prefix.
    This will also handle the s3:// prefix.
    :return: Tuple of ('bucketname', 'keyname')
    """
    if s3_path.startswith("s3://"):
        s3_path = s3_path[5:]
    if s3_path.startswith("https://"):
        s3_path = s3_path[8:]
    return find_bucket_key(s3_path)


def save_original_csv(filename):
    """
    Save a copy of the original CSV file before processing.
    """
    original_csv_path = os.path.splitext(filename)[0] + "_original.csv"
    shutil.copyfile(filename, original_csv_path)
    return original_csv_path


def download_files(filename, group_by):
    base = os.path.join(
        os.getenv("LOCATION"), os.path.splitext(os.path.basename(filename))[0]
    )
    logger.info("Creating base folder as :: {}".format(base))
    if os.path.exists(base):
        shutil.rmtree(base)
    os.makedirs(base)
    logger.info("Reading file")
    
    # Save original CSV file
    original_csv_path = save_original_csv(filename)
    
    # Read the CSV file
    try:
        with open(filename, "r") as reader:
            lines = reader.readlines()
            header = lines[0].strip().split(',')
            lines = lines[1:]  # Skip header

    except UnicodeDecodeError:
        with open(filename, "r", encoding="windows-1252") as reader:
            lines = reader.readlines()
            header = lines[0].strip().split(',')
            lines = lines[1:]  # Skip header

    logger.info("Downloading S3 links")
    s3 = boto3.resource(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )
    
    # Calculate number of worker threads
    max_workers = math.ceil(len(lines) / 200)
    logger.info("Dividing into workers :: {} for total :: {}".format(max_workers, len(lines)))
    chunks = np.array_split(lines, max_workers)

    def fetcher(chunk):
        skipped = {"Error": {}}
        not_existing = []
        invalid = []
        invalid_s3_url = []
        updated_lines = []  # To store updated lines for CSV update
        
        for line in chunk:
            line = line.strip()
            vals = line.split(",")
            
            if len(vals) < 3:
                invalid.append(line)
                continue
            
            try:
                s3_url = vals[0].strip()
                parsed_url = urlparse(s3_url)
                bucket = parsed_url.netloc.split('.')[0]
                path = parsed_url.path + ('?' + parsed_url.query if parsed_url.query else '') + ('#' + parsed_url.fragment if parsed_url.fragment else '')
                key = path.lstrip('/')
                group_value = vals[2].strip() if group_by == "airline" else vals[3].strip()
                target_dir = os.path.join(base, group_value)
                os.makedirs(target_dir, exist_ok=True)
                original_name = vals[1].strip()
                target_name = os.path.join(target_dir, original_name)
                
                # Download file from S3
                s3.Bucket(bucket).download_file(key, target_name)
                
                # Append status 'pass' to updated line
                vals.append('Success')
                updated_lines.append(vals)
                
            except botocore.exceptions.ClientError as e:
                if e.response["Error"]["Code"] == "404":
                    not_existing.append(line)
                else:
                    err_code = e.response["Error"]["Code"]
                    if err_code not in skipped["Error"]:
                        skipped["Error"][err_code] = []
                    skipped["Error"][err_code].append(line)
                
                # Append status 'fail' to updated line
                vals.append('fail')
                updated_lines.append(vals)
                
            except botocore.exceptions.ParamValidationError:
                invalid_s3_url.append(line)
                
                # Append status 'fail' to updated line
                vals.append('fail')
                updated_lines.append(vals)
                
            except FileNotFoundError:
                if "FileNotFound" not in skipped["Error"]:
                    skipped["Error"]["FileNotFound"] = []
                skipped["Error"]["FileNotFound"].append(line)
                
                # Append status 'fail' to updated line
                vals.append('fail')
                updated_lines.append(vals)
                
            except ValueError:
                invalid_s3_url.append(line)
                
                # Append status 'fail' to updated line
                vals.append('fail')
                updated_lines.append(vals)
        
        return base, skipped, not_existing, invalid, invalid_s3_url, updated_lines

    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for result in executor.map(fetcher, chunks):
            results.append(result)
    
    # Write updated lines back to the CSV file
    updated_csv_lines = []
    for result in results:
        updated_csv_lines.extend(result[-1])
    
    with open(original_csv_path, 'w', newline='') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(header + ['Status'])
        for line in updated_csv_lines:
            writer.writerow(line)

    # Aggregate results for return
    base = ""
    skipped = {}
    not_existing = []
    invalid = []
    invalid_s3_url = []
    total = 0
    
    for result in results:
        base = result[0]
        skipped.update(result[1])
        not_existing.extend(result[2])
        invalid.extend(result[3])
        invalid_s3_url.extend(result[4])
        total += len(result[-1])  # Count of processed lines

    return base, skipped, not_existing, invalid, invalid_s3_url, total



def zip_folder(source: str, output_zip: str = ""):
    print("Zipping folder")
    password = None
    if not output_zip:
        output_zip = source + ".zip"
    with pyzipper.AESZipFile(
        output_zip, "w", compression=pyzipper.ZIP_STORED
    ) as zip_file:
        for root, _, files in os.walk(source):
            for file in files:
                file_path = os.path.join(root, file)
                zip_file.write(file_path, arcname=os.path.relpath(file_path, source))
    return output_zip, password



def upload_to_s3(zip_file):
    """this will upload the zip to the s3"""
    logger.info("Uploading to s3")
    upload_name = datetime.now().strftime("%Y%m%d%H%M%S%f") + "-invoices.zip"
    bucket = "finkraft-invoices-outstation"
    s3 = boto3.resource(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )
    key = "downloadables/" + os.path.basename(upload_name)
    s3.meta.client.upload_file(
        zip_file,
        bucket,
        key,
    )
    return bucket, key


def generate_presigned_url(bucket, key):
    """this will generate a presigned url"""
    logger.info("Generating pre-signed url")
    s3 = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )
    url = s3.generate_presigned_url(
        ClientMethod="get_object",
        Params={"Bucket": bucket, "Key": key},
        ExpiresIn=EXPIRY_TIME,  # seconds
    )
    return url


def get_skipped_count(skipped):
    check = skipped["Error"]
    val = 0
    for key in check:
        val += len(check[key])
    return val


def convert_html_to_pdf(folder):
    logger.info("Converting html to pdf in folder:: {}".format(folder))
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        for ifile in tqdm(glob(folder + "/**/*.html", recursive=True)):
            iname = os.path.abspath(ifile)
            oname = os.path.splitext(iname)[0] + ".pdf"
            page.emulate_media(media='print')
            page.goto(f"file:///{iname}")
            page.pdf(path=oname)
            os.remove(iname)

def send_mail(url,
            password,
            total,
            unprocessed,
            email,
            skipped,
            not_existing,
            invalid,
            invalid_s3_url,):
    success = True
    msg = "Successfully downloaded and sent"
    try:
        send_zoho_mail(
            url,
            password,
            total,
            unprocessed,
            email,
            skipped,
            not_existing,
            invalid,
            invalid_s3_url,
        )
    except Exception as e:
        print("send mail error ",e)
        import traceback
        success = False
        msg = traceback.format_exc()
    return success, msg





def process_upload(filename, email, group_by):
    """this will process the uploaded file"""
    logger.info("Processing file")
    base, skipped, not_existing, invalid, invalid_s3_url, total = download_files(
        filename, group_by
    )
    logger.info("Downloaded to {}".format(base))
    convert_html_to_pdf(base)
    zip, password = zip_folder(base)
    bucket, key = upload_to_s3(zip)
    url = generate_presigned_url(bucket, key)
    unprocessed = (
        get_skipped_count(skipped)
        + len(not_existing)
        + len(invalid)
        + len(invalid_s3_url)
    )
    success, msg = send_mail(url,
            password,
            total,
            unprocessed,
            email,
            skipped,
            not_existing,
            invalid,
            invalid_s3_url,)
    success = True
    msg = "sent"
    if os.path.exists(filename):
        os.remove(filename)
    if os.path.exists(zip):
        os.remove(zip)
    if os.path.exists(base):
        shutil.rmtree(base)
    return success, msg, url, skipped, not_existing, invalid, invalid_s3_url, total


def process_csv(filename):
    logger.info("Processing file:: {}".format(filename))
    base, skipped, not_existing, invalid, invalid_s3_url, total = download_files(
        filename
    )
    convert_html_to_pdf(base)
    results = {
        'base' : base,
        'skipped' : skipped,
        'not_existing' : not_existing,
        'invalid' : invalid,
        'invalid_s3_url' : invalid_s3_url,
        'total' : total,
    }
    import json
    with open('results.json', 'w') as writer:
        json.dump(results, writer, default=str, indent=4)
