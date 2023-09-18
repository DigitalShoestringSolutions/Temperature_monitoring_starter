curl -G 'http://172.18.0.2:8086/query' --data-urlencode "db=emon" --data-urlencode "epoch=s" --data-urlencode "q=select * from Process_1" -H "Accept: application/csv" > $1 

