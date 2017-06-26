sudo mkdir /home/hadoop/payer_again_dc
sudo aws s3 cp s3://sp-dataproduct/payer_again_dc/production /home/hadoop/payer_again_dc --exclude 'sp-dataproduct/payer_again_dc/production/output/*' --recursive
sudo mkdir /home/hadoop/payer_again_dc/output
sudo chown hadoop /home/hadoop/payer_again_dc/ --recursive

sudo mkdir -p /mnt/payer_again_dc/input_data
sudo chown hadoop /mnt/payer_again_dc --recursive

sudo yum install -y xorg-x11-xauth.x86_64 xorg-x11-server-utils.x86_64 xterm libXt libX11-devel libXt-devel libcurl-devel git
sudo yum install -y postgresql-devel

sudo R --vanilla --slave -f /home/hadoop/payer_again_dc/src/payer_again_dc_predictive.R
