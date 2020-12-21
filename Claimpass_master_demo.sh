#!/bin/bash
#step1 - sparkshell call

source  /mapr/datalake/optum/optuminsight/udw/dev/ped/dev/d_conf/Claimpass_master_demo_configuration.cfg
sh ${script_path}/Claimpass_spark_submit_demo.sh ${conf_path}/Claimpass_master_demo_configuration.cfg
