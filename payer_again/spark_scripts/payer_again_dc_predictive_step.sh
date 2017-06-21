sudo mkdir  /home/amckay/payer_again_dc
sudo aws s3 cp s3://sp-dataproduct/payer_again_dc/production /home/amckay/payer_again_dc --recursive
sudo chown amckay /home/amckay/payer_again_dc --recursive

#sudo R --vanilla --slave -f /home/amckay/payer_again_dc/production/src/payer_again_dc_predictive.R
sudo Rscript /home/amckay/payer_again_dc/production/src/payer_again_dc_predictive.R