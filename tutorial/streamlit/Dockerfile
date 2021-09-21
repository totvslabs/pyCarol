FROM python:3.8

RUN mkdir /app
WORKDIR /app
ADD requirements.txt /app/
RUN pip install -r requirements.txt

RUN python -m spacy download en_core_web_sm

ADD . /app

EXPOSE 8501

CMD streamlit run --server.port 5500 app.py