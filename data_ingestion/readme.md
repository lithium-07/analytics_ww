Pre req:
This will need a gcp-cred.json file to connect. This file can be obtained from:
console.cloud.google.com -> api and services -> credentials

You will have to create credentials if they are not present and then download them as json
(credentials should be generated for a service account)

Steps to run locally(use these commands in the terminal after cloning this repo):
1. pip install -r requirements.txt
2. uvicorn main:app --reload


