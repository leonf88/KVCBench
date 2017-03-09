#!/usr/bin/env bash

# prepare the data to process
# prepare.sh <root_path> <dir1> <dir2> <dir3> ...

target_path=$1
shift 1

if [ -z "$target_path" ]
then
    echo "Target path is empty"
    exit 1
fi

rm -r -I $target_path

while [ $# -gt 0 ]
do
    dir_path=$1
    shift 1
    for sub_path in `ls $dir_path`
    do
        # sub_path should be like 10G_ST_FLK
        data=`echo $sub_path | awk -F"_" '{print $1}'`
        expr=`echo $sub_path | awk -F"_" '{print $2}'`
        plat=`echo $sub_path | awk -F"_" '{print $3}'`

        data_path="$target_path/$expr"
        echo $data_path
        [ ! -e "$data_path" ] && mkdir -p $data_path

        tcost=`tail -n 1 $dir_path/$sub_path/app_report_*.log | grep -Po '(?<=cost ).*(?= sec)'`
        case $plat in
        "HAD")
            echo "$data $tcost" >> $data_path/had.data
            sort -n $data_path/had.data -o $data_path/had.data
        ;;
        "SPK")
            echo "$data $tcost" >> $data_path/spk.data
            sort -n $data_path/spk.data -o $data_path/spk.data
        ;;
        "FLK")
            echo "$data $tcost" >> $data_path/flk.data
            sort -n $data_path/flk.data -o $data_path/flk.data
        ;;
        "DM")
            echo "$data $tcost" >> $data_path/dm.data
            sort -n $data_path/dm.data -o $data_path/dm.data
        ;;
        "DMI")
            echo "$data $tcost" >> $data_path/dmi.data
            sort -n $data_path/dmi.data -o $data_path/dmi.data
        ;;
        esac
    done
done