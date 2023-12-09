# sentimentanalysis

how to start project.

1. build and run docker image for kafka producer.
   -> cd tweets_producer.
   -> docker build -t tp .
   -> docker run tp
This will stream tweets the tweets to kafka topic

2. docker compose file is configured to run the spark application, backend server and web application
3. can be executed through
1. execution.bat or execution.sh
