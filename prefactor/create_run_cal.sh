#!/bin/bash



generate_run_script(){

	local shfile=$1
	local pipeline_cfg_file=$2
	local parset_file=$3
	local prmon_file=$4
	local prmon_json=$5
	local data_outdir=$6
	local data_destdir=$7

	echo "INFO: Creating sh file $shfile ..."
	( 
			echo "#!/bin/bash"
			echo ""
			echo "source /usr/lofarinit.sh"
			echo ""

			##echo "prmon -p $$ -f $prmon_file -j $prmon_json -i 30 &"
			echo "prmon -f $prmon_file -j $prmon_json -i 30 -- genericpipeline.py -v -d -c $pipeline_cfg_file $parset_file"
			echo ""

			echo 'echo "Moving output data from dir $data_outdir to $data_destdir ..."'
			echo 'mv $data_outdir/*.ndppp_prep_cal $data_destdir' 

 	) > $shfile

	chmod +x $shfile
}



######################################
##            MAIN 
######################################
RUN_SCRIPT=$1
PARSET_FILE=$2
PIPELINE_CFG_FILE=$3
DATA_OUTDIR=$4
DATA_DESTDIR=$5

## Set prmon files
filename_base=$(basename "$RUN_SCRIPT")
filename_base_noext="${filename_base%.*}"
prmon_file="prmon_$filename_base_noext.txt"
prmon_json="prmon_$filename_base_noext.json"
echo "prmon_file=$prmon_file"

CURRENT_DIR=$PWD

## Generate run script
echo "INFO: Creating run script $RUN_SCRIPT ..."
generate_run_script $RUN_SCRIPT $PIPELINE_CFG_FILE $PARSET_FILE $prmon_file $prmon_json $DATA_OUTDIR $DATA_DESTDIR


