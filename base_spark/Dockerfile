FROM bitnami/spark:latest

# Install Spacy and download the English language model
RUN pip3 install spacy && \
    python3 -m spacy download en_core_web_sm
