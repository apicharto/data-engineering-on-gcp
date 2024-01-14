#!/bin/bash

API_KEY='$2a$10$7wMPuPGt8k4DuZ9rF1oGV.oDVZqd6JbegECR3wzmA9r7jlUHt2Owi'
COLLECTION_ID='659a4d0b1f5677401f189fee'

curl -XPOST \
    -H "Content-type: application/json" \
    -H "X-Master-Key: $API_KEY" \
    -H "X-Collection-Id: $COLLECTION_ID" \
    -d @dogs.json \
    "https://api.jsonbin.io/v3/b"a  