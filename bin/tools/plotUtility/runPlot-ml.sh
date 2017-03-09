#! /usr/bin/env bash
# Run gnuplot for 3 platforms' dstat.log.
# Run: ./runPlot-ml.sh [Results_DIR]

curDir=`pwd`
if [ "$#" -ne "1" ]; then
    echo "Usage: ./runPlot-ml.sh [Figures_DIR]"
    exit 1
fi

figDir=$(greadlink -f $1)
[ ! -d "$figDir" ] && echo "\"$figDir\" does not exist!" && exit -1

# Gnuplot each job.
for job in `ls $figDir`
do
  echo "Ploting job: $job"
  pushd $figDir/$job > /dev/null
  gnuplot $curDir/plotDstat-ml.dem
  popd > /dev/null
done

