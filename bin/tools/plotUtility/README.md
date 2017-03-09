prepare data With:

    bash prepare.sh data/micro ../../results-flk-micro ../../results-spk-micro ../../results-had-micro ../../results-dm-micro
    bash prepare.sh data/ml ../../results-had-ml ../../results-spk-ml ../../results-dm-ml

tarDstat.sh at the server With:

    ./tarDstat.sh [Results_DIR]

runPlot.sh at client With:

    ./runPlot-ml.sh data/ml
    ./runPlot-micro.sh data/micro

* Gnuplot Color: [LINK][gnu-color]

[gnu-color]: https://www2.uni-hamburg.de/Wiss/FB/15/Sustainability/schneider/gnuplot/colors.htm
