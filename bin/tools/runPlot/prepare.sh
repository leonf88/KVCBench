#!/usr/bin/env bash

# prepare the data to process
# prepare.sh <root_path> <dir1> <dir2> <dir3> ...

target_path=$1
shift 1

while [ $# -gt 0 ]
do
    dir_path=$1
    shift 1
    for sub_path in `ls $dir_path`
    do
        # sub_path should be like 10M_PR_HAD or 30M_KM_SPK_NEW
        last_part=${sub_path##*_}
        if [ $last_part == "NEW" ]; then
            _sub_path=${sub_path%_*}
        else
            _sub_path=${sub_path}
        fi
        expr=${_sub_path%_*}
        plat=${_sub_path##*_}

        data_path="$target_path/$expr"
        echo $data_path
        [ ! -e "$data_path" ] && mkdir -p $data_path

        case $plat in
        "HAD")
            cp $dir_path/$sub_path/dstat.log $data_path/had.data
        ;;
        "HAD_NEW")
            cp $dir_path/$sub_path/dstat.log $data_path/had.data
        ;;
        "SPK")
            cp $dir_path/$sub_path/dstat.log $data_path/spk.data
        ;;
        "SPK_NEW")
            cp $dir_path/$sub_path/dstat.log $data_path/spk.data
        ;;
        "FLK")
            cp $dir_path/$sub_path/dstat.log $data_path/flk.data
        ;;
        "FLK_NEW")
            cp $dir_path/$sub_path/dstat.log $data_path/flk.data
        ;;
        "DM")
            cp $dir_path/$sub_path/dstat.log $data_path/dm.data
        ;;
        "DMI")
            cp $dir_path/$sub_path/dstat.log $data_path/dmi.data
        ;;
        esac
    done
done
