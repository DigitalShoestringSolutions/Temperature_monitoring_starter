curl -G 'http://172.18.0.2:8086/query' --data-urlencode "db=emon" --data-urlencode "epoch=s" --data-urlencode "q=delete from Process_1 where time <= '"$1" "$2"'"

