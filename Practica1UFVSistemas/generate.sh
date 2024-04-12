#!/bin/bash

randomNum(){
    local min=$1
    local max=$2
    echo $((RANDOM % (max - min + 1) + min))
}

while getopts ":l:s:t:u:f:" opt; do
  case ${opt} in
    l)
      lines="${OPTARG}"
      echo "lines read: $lines"
      ;;
    s)
      sucursales="${OPTARG}"
      echo "sucursales read: $sucursales"
      ;;
    t)
      tipo="${OPTARG}"
      echo "tipo read: $tipo"
      ;;
    u)
      numUsers="${OPTARG}"
      echo "users read: $numUsers"
      ;;
    f)
      numFiles="${OPTARG}"
      echo "files read: $numFiles"
      ;;
  esac
done

state=("Error" "Correcto" "Finalizado")

hour=$(date +"%H")
minute=$(date +"%M")


for (( i=1; i <= $sucursales; i++)) ; do
    if ((i >= 0)) && ((i < 10)); then PADDING="00"; 
    elif ((i >= 10)) && ((i < 100)); then PADDING="0"; 
    elif ((i >= 100)) && ((i < 1000)); then PADDING=""; 
    fi
    for (( k=1; k <= $numFiles; k++)) ; do
        for (( j=1; j <= $lines; j++)) ; do
            echo "OPE$j, $(date +"%d/%m/%Y") $(randomNum 0 $hour):$(randomNum 0 $minute), $(date +"%d/%m/%Y") $(randomNum 0 $hour):$(randomNum 0 $minute), USER0$(randomNum 1 $numUsers), $tipo, $(randomNum 1 3), $(randomNum -1000 1000), ${state[$(randomNum 0 2)]}" >> "Files/SU${PADDING}${i}_OPE0${i}_$(date +"%d%m%Y")$k.csv"
        done
    done
done

