FROM ubuntu:22.04
RUN apt update && apt install -y software-properties-common \
&& add-apt-repository ppa:deadsnakes/ppa
RUN apt install -y python3.10-dev
RUN apt install -y python3-pip
WORKDIR /code

RUN python3 -m pip install --upgrade pip setuptools wheel
COPY ./requirements.txt /code/requirements.txt

RUN python3 -m pip install -r /code/requirements.txt
RUN playwright install --with-deps chromium


COPY ./app /code/app
COPY .env /code/.env



# uvicorn app.main:app --host 127.0.0.1 --port 5000 --timeout-keep-alive 3600
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "5000", "--timeout-keep-alive", "3600"] 