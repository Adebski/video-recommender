#!/bin/bash

DIR=$1

# PR

gnuplot << EOF
   set xlabel "Recall"
   set ylabel "Precision"
   set term png
   set output "${DIR}/pr.png"
   plot "${DIR}/pr.dat" using 1:2 title "precision/recall"
EOF

gnuplot << EOF
   set xlabel "False positive rate"
   set ylabel "True positive rate"
   set term png
   set output "${DIR}/roc.png"
   plot "${DIR}/roc.dat" using 1:2 title "ROC"
EOF

gnuplot << EOF
   set xlabel "Threshold"
   set term png
   set output "${DIR}/thresholds.png"
   plot "${DIR}/thresholds.dat" using 1:2 title "Precision", "" using 1:3 title "Recall", "" using 1:4 title "F1-measure"
EOF

	
