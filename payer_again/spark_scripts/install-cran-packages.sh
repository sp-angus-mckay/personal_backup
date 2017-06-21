#!/bin/bash

PACKAGES=""

while [[ $# > 1 ]]; do
    key="$1"

    case $key in
        # The packages to install separated by semicolon
        # Eg: --packages magrittr;dplyr
        --packages)
            PACKAGES="$2"
            shift
            ;;
        *)
            echo "Unknown option: ${key}"
            exit 1;
    esac
    shift
done

echo "*****************************************"

PACKAGES_ARR=(${PACKAGES//;/ })
for i in "${PACKAGES_ARR[@]}"
do
    :
    echo "  Installing ${i}"
    echo "*****************************************"
    sudo R -e "install.packages('${i}', repos='https://cran.rstudio.com/')" 1>&2
done
