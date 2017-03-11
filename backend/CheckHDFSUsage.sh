    #!/usr/bin/env bash
    max_depth=5
     
    largest_root_dirs=$(hdfs dfs -du -s '/*' | sort -nr | perl -ane 'print "$F[1] "')
     
    printf "%15s  %s\n" "bytes" "directory"
    for ld in $largest_root_dirs; do
        printf "%15.0f  %s\n" $(hdfs dfs -du -s $ld| cut -d' ' -f1) $ld
        all_dirs=$(hdfs dfs -ls -R $ld | egrep '^dr........' | perl -ane "scalar(split('/',\$_)) <= $max_depth && print \"\$F[7]\n\"" )
     
        for d in $all_dirs; do
            line=$(hdfs dfs -du -s $d)
            size=$(echo $line | cut -d' ' -f1)
            parent_dir=${d%/*}
            child=${d##*/}
            if [ -n "$parent_dir" ]; then
                leading_dirs=$(echo $parent_dir | perl -pe 's/./-/g; s/^.(.+)$/\|$1/')
                d=${leading_dirs}/$child
            fi
            printf "%15.0f  %s\n" $size $d
        done
    done
