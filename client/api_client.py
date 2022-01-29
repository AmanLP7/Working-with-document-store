import linecache
import json

import requests

start = 1
end = 50

i = start

while i<=end:
    line = linecache.getline(
        "/mnt/c/Users/91870/Projects/My-github/Working-with-document-store/output.txt", 
        i
        )
    data = json.loads(line)
    print(data)
    response = requests.post("http://localhost:80/invoiceitem", json=data)
    print(response.json())
    i += 1