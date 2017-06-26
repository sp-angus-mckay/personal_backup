# aws s3 cp s3://sp-dataproduct/payer_again_dc/production/payer_again_dc_initialization_short.sh .
# aws s3 cp s3://sp-dataproduct/payer_again_dc/production/install_packages.R /tmp/
# aws s3 cp s3://sp-dataproduct/payer_again_dc/production/packages /tmp/packages --recursive
# sh ./payer_again_dc_initialization_short.sh "$@" &
# #sh ./install-cran-packages.sh --packages 'httr;devtools;RJDBC;ghit;Matrix;lubridate;rjson;bit64;data.table;FNN'
# 
# echo 'INITIALIZATION SHORT FINISHED'
# #sudo R --vanilla --slave -f /tmp/install_packages.R --args '/tmp/packages' 'httr;RJDBC;ghit;Matrix;magrittr;stringi;stringr;lubridate;rjson;bit64;data.table;FNN;devtools'