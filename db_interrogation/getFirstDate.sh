echo $(date -d @$(head -2 $1 | tail -1 | cut -d ',' -f 3))
