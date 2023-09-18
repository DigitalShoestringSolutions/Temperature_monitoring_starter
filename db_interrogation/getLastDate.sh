echo $(date -d @$(tail -1 $1 | cut -d ',' -f 3))
