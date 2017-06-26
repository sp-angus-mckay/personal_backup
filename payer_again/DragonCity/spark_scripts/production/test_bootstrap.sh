aws s3 cp s3://sp-dataproduct/payer_again_dc/production/payer_again_initialization_short.sh .
aws s3 cp s3://sp-dataproduct/payer_again_dc/production/install-cran-packages.sh .
sh ./payer_again_initialization_short.sh "$@" &
sh ./install-cran-packages.sh --packages 'devtools;RJDBC;ghit;Matrix;lubridate;rjson;bit64;data.table;httr'