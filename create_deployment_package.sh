#!/usr/bin/env bash
# API key vmXvcob4V33NpNVXKsDll8nAQAcmHGZZ87Fl4HF6

echo "==================================="
echo " Lambda package deployer v0.1"
rm -rf *zip
echo "1- Create the logging package "
zip -q9 filestats.zip main.py
zip -qr9 filestats.zip modules

#echo "  -> Delete current deployment"
#aws lambda delete-function \
#--region eu-west-1 \
#--function-name filestats
#sleep 5


echo "  -> Deploy"
#aws lambda create-function \
aws lambda update-function-code \
--region us-east-2 \
--function-name filestats \
--zip-file fileb:///Users/adamalloul/newsletter_stats/filestats.zip
#--role arn:aws:iam::145354838388:role/fstats_runner \
#--handler main.main \
#--runtime python3.6

#echo "  -> Wait 5s"
#sleep 5
# echo "  -> Test"
# ./test_api_gateway.py


# echo "              Done"
# echo "==================================="
