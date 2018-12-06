#!/bin/bash

#######################################
##         CHECK ARGS
#######################################
NARGS="$#"
echo "INFO: NARGS= $NARGS"

if [ "$NARGS" -lt 2 ]; then
	echo "ERROR: Invalid number of arguments...see script usage!"
  echo ""
	echo "**************************"
  echo "***     USAGE          ***"
	echo "**************************"
 	echo "$0 [ARGS]"
	echo ""
	echo "=========================="
	echo "==    ARGUMENT LIST     =="
	echo "=========================="
	echo "*** MANDATORY ARGS ***"
	echo "--filelist-cal=[FILELIST] - Ascii file with list of visibility files for calibrator (CASA format) to be processed in step 1" 
	echo "--containerimg=[CONTAINER_IMG] - Singularity container image file (.simg) with LOFAR installed software"
	echo ""

	echo "*** OPTIONAL ARGS ***"
	echo "=== RUN OPTIONS ==="	
	echo "--no-cal-step1 - Do not perform calibrator cal step 1 (default=true)"
	echo "--no-cal-step2 - Do not perform calibrator cal step 2 (default=true)"
	echo "--no-target-step1 - Do not perform target cal step 1 (default=true)"
	echo "--no-target-step2 - Do not perform target cal step 2 (default=true)"
	echo "--envfile=[ENV_FILE] - File (.sh) with list of environment variables to be loaded by each processing node"
	echo "--outdir=[OUTPUT_DIR] - Output directory where to write run output file (default=pwd)"
	echo "--storagedir=[STORAGE_DIR] - Output directory where to store run output files (default=pwd)"
	echo "--maxfiles=[NMAX_PROCESSED_FILES] - Maximum number of input files processed in filelist (default=-1=all files)"
	echo "--addrunindex - Append a run index to submission script (in case of list execution) (default=no)"	
	echo "--nproc=[NPROC] - Number of  processors per node used  (default=1)"
	echo "--nthreads=[NTHREADS] - Number of threads to be used in multithreaded pipeline stage (default=1)"
	echo "--nproc-step2=[NPROC_STEP2] - Number of  processors per node used in stage 2 (default=1)"
	echo "--nthreads-step2=[NTHREADS_STEP2] - Number of threads to be used in multithreaded pipeline stage 2 (default=1)"
	echo "--containeroptions=[CONTAINER_OPTIONS] - Options to be passed to container run (e.g. -B /home/user:/home/user) (default=none)"
	echo ""

	echo "=== SUBMISSION OPTIONS ==="
	echo "--submit - Submit the script to the batch system using queue specified"
	echo "--run - Run the script locally"
	echo "--batchsystem - Name of batch system. Valid choices are {PBS,SLURM} (default=PBS)"
	echo "--queue=[BATCH_QUEUE] - Name of queue in batch system" 
	echo "--jobwalltime=[JOB_WALLTIME] - Job wall time in batch system (default=96:00:00)"
	echo "--jobcpus=[JOB_NCPUS] - Number of cpu per node requested for the job (default=1)"
	echo "--jobnodes=[JOB_NNODES] - Number of nodes requested for the job (default=1)"
	echo "--jobmemory=[JOB_MEMORY] - Memory in GB required for the job (default=4)"
	echo "--jobusergroup=[JOB_USER_GROUP] - Name of job user group batch system (default=empty)" 	
	echo "=========================="
  exit 1
fi

#######################################
##         PARSE ARGS
#######################################
ENV_FILE=""
ENV_FILE_GIVEN=false
SUBMIT=false
DO_RUN=false
BATCH_SYSTEM="PBS"
CONTAINER_IMG=""
CONTAINER_OPTIONS=""
APPEND_RUN_INDEX=false
DO_CALIBRATOR_CAL_STEP1=true
DO_CALIBRATOR_CAL_STEP2=true
DO_TARGET_CAL_STEP1=true
DO_TARGET_CAL_STEP2=true
FILELIST_CAL=""
FILELIST_CAL_GIVEN=false
NMAX_PROCESSED_FILES=-1
BATCH_QUEUE=""
JOB_WALLTIME="96:00:00"
JOB_MEMORY="4"
JOB_USER_GROUP=""
JOB_USER_GROUP_OPTION=""
JOB_NNODES="1"
JOB_NCPUS="1"
OUTPUT_DIR=$PWD
STORAGE_DIR=$PWD
STORAGE_DIR_GIVEN=false
NPROC=1
NPROC_STEP2=1
NTHREADS=1
NTHREADS_STEP2=1


for item in "$@"
do
	case $item in 
		## MANDATORY ##	
		--filelist-cal=*)
    	FILELIST_CAL=`echo $item | sed 's/[-a-zA-Z0-9]*=//'`
			if [ "$FILELIST_CAL" != "" ]; then
				FILELIST_CAL_GIVEN=true
			fi
    ;;
		--containerimg=*)
    	CONTAINER_IMG=`echo $item | sed 's/[-a-zA-Z0-9]*=//'`
    ;;
	
		## OPTIONAL ##	
		## - RUN OPTIONS	
		--no-cal-step1*)
			DO_CALIBRATOR_CAL_STEP1=false
		;;
		--no-cal-step2*)
			DO_CALIBRATOR_CAL_STEP2=false
		;;
		--no-target-step1*)
			DO_TARGET_CAL_STEP1=false
		;;
		--no-target-step2*)
			DO_TARGET_CAL_STEP2=false
		;;

		--envfile=*)
    	ENV_FILE=`echo $item | sed 's/[-a-zA-Z0-9]*=//'`
			if [ "$ENV_FILE" != "" ]; then
				ENV_FILE_GIVEN=true
			fi
    ;;
		--outdir=*)
    	OUTPUT_DIR=`echo $item | sed 's/[-a-zA-Z0-9]*=//'`
    ;;
		--storagedir=*)
    	STORAGE_DIR=`echo $item | sed 's/[-a-zA-Z0-9]*=//'`
			if [ "$STORAGE_DIR" != "" ]; then
				STORAGE_DIR_GIVEN=true
			fi
    ;;
		--containeroptions=*)
    	CONTAINER_OPTIONS=`echo $item | sed 's/[-a-zA-Z0-9]*=//'`
    ;;
		--maxfiles=*)
    	NMAX_PROCESSED_FILES=`echo $item | sed 's/[-a-zA-Z0-9]*=//'`
    ;;
		--addrunindex*)
			APPEND_RUN_INDEX=true
		;;
		--nproc=*)
      NPROC=`echo $item | sed 's/[-a-zA-Z0-9]*=//'`
    ;;
		--nproc-step2=*)
      NPROC_STEP2=`echo $item | sed 's/[-a-zA-Z0-9]*=//'`
    ;;
		--nthreads=*)
    	NTHREADS=`echo $item | sed 's/[-a-zA-Z0-9]*=//'`
    ;;
		--nthreads-step2=*)
    	NTHREADS_STEP2=`echo $item | sed 's/[-a-zA-Z0-9]*=//'`
    ;;
			
		## - SUBMISSION OPTIONS
		--submit*)
    	SUBMIT=true
    ;;
		--run*)
    	DO_RUN=true
    ;;
		--batchsystem=*)
    	BATCH_SYSTEM=`echo $item | sed 's/[-a-zA-Z0-9]*=//'`
    ;;
		--queue=*)
    	BATCH_QUEUE=`echo $item | sed 's/[-a-zA-Z0-9]*=//'`
    ;;
		--jobwalltime=*)
			JOB_WALLTIME=`echo $item | sed 's/[-a-zA-Z0-9]*=//'`	
		;;
		--jobcpus=*)
      JOB_NCPUS=`echo $item | sed 's/[-a-zA-Z0-9]*=//'`
    ;;
		--jobnodes=*)
      JOB_NNODES=`echo $item | sed 's/[-a-zA-Z0-9]*=//'`
    ;;
		--jobmemory=*)
			JOB_MEMORY=`echo $item | sed 's/[-a-zA-Z0-9]*=//'`	
		;;
		--jobusergroup=*)
			JOB_USER_GROUP=`echo $item | sed 's/[-a-zA-Z0-9]*=//'`	
			JOB_USER_GROUP_OPTION="#PBS -A $JOB_USER_GROUP"
		;;


    *)
    # Unknown option
    echo "ERROR: Unknown option ($item)...exit!"
    exit 1
    ;;
	esac
done


## Check arguments parsed
if [ "$FILELIST_CAL_GIVEN" = false ] && [ "$DO_CALIBRATOR_CAL_STEP1" = true ]; then
  echo "ERROR: Missing or empty FILELIST_CAL args (hint: you should specify list of cal files to be processed if DO_CAL_STEP1 is enabled)!"
  exit 1
fi

if [ "$CONTAINER_IMG" = "" ] ; then
  echo "ERROR: Empty CONTAINER_IMG argument (hint: you must specify a container image)!"
  exit 1
fi

if [ "$BATCH_QUEUE" = "" ] && [ "$SUBMIT" = true ]; then
  echo "ERROR: Empty BATCH_QUEUE argument (hint: you must specify a queue if submit option is activated)!"
  exit 1
fi

if [ "$BATCH_SYSTEM" = "" ] && [ "$SUBMIT" = true ]; then
  echo "ERROR: Empty BATCH_SYSTEM argument (hint: you must specify a batch systen if submit option is activated)!"
  exit 1
fi

if [ "$BATCH_SYSTEM" != "PBS" ] && [ "$BATCH_SYSTEM" != "SLURM" ]; then
  echo "ERROR: Unknown/not supported BATCH_SYSTEM argument $BATCH_SYSTEM (hint: PBS/SLURM are supported)!"
  exit 1
fi


#######################################
##     DEFINE & LOAD ENV VARS
#######################################
export BASEDIR="$PWD"
export OUTPUT_DATADIR="$PWD"
export DATADIR=""

## Load env file
if [ "$ENV_FILE_GIVEN" = true ]; then
	echo "INFO: Loading environment variables defined in file $ENV_FILE ..."
	source $ENV_FILE
fi

## Define batch run options
if [ "$BATCH_SYSTEM" = "PBS" ]; then
  BATCH_SUB_CMD="qsub"
	BATCH_QUEUE_NAME_OPTION="-q"
	BATCH_JOB_NAME_DIRECTIVE="#PBS -N"
	BATCH_JOB_DEPENDENCY_OPTION="-W depend=afterok"
	BATCH_JOB_OUTFILE_DIRECTIVE="#PBS -o $BASEDIR"
	BATCH_JOB_ERRFILE_DIRECTIVE="#PBS -e $BASEDIR"
	BATCH_JOB_JOINOUTERR_DIRECTIVE="#PBS -j oe"
	BATCH_JOB_WALLTIME_DIRECTIVE="#PBS -l walltime=$JOB_WALLTIME"
	BATCH_JOB_SHELL_DIRECTIVE="#PBS -S /bin/bash"
	BATCH_JOB_USERGRP_DIRECTIVE="#PBS -A $JOB_USER_GROUP"
	BATCH_JOB_PRIORITY="#PBS -p 1"
	BATCH_JOB_NOREQUEUE_DIRECTIVE="#PBS -r n"
	BATCH_JOB_SCATTER_DIRECTIVE="#PBS -l place=scatter"
	BATCH_JOB_NNODES_DIRECTIVE="#PBS -l select=$JOB_NNODES"':'"ncpus=$JOB_NCPUS"':'"mpiprocs=$NPROC"':'"mem=$JOB_MEMORY"'gb'
	#BATCH_JOB_NPROC_DIRECTIVE="#PBS -l mpiprocs="
	#BATCH_JOB_MEM_DIRECTIVE="#PBS -l mem="
	#BATCH_JOB_NCORE_DIRECTIVE="#PBS -l ncpus="
	BATCH_JOB_NPROC_DIRECTIVE=""
	BATCH_JOB_MEM_DIRECTIVE=""
	BATCH_JOB_NCORE_DIRECTIVE=""

elif [ "$BATCH_SYSTEM" = "SLURM" ]; then
  BATCH_SUB_CMD="sbatch"
	BATCH_QUEUE_NAME_OPTION="-p"
	BATCH_JOB_DEPENDENCY_OPTION="--dependency=afterok"
	BATCH_JOB_NAME_DIRECTIVE="#SBATCH -J"
	BATCH_JOB_OUTFILE_DIRECTIVE="#SBATCH -o $BASEDIR"
	BATCH_JOB_ERRFILE_DIRECTIVE="#SBATCH -e $BASEDIR"
	BATCH_JOB_JOINOUTERR_DIRECTIVE="" # There is no such option in SLURM
	BATCH_JOB_WALLTIME_DIRECTIVE="#SBATCH --time=$JOB_WALLTIME"
	BATCH_JOB_SHELL_DIRECTIVE="" # Equivalent SLURM directive not found
	BATCH_JOB_USERGRP_DIRECTIVE="#SBATCH -A $JOB_USER_GROUP"
	BATCH_JOB_PRIORITY="" # Equivalent SLURM directive not found
	BATCH_JOB_NOREQUEUE_DIRECTIVE="#SBATCH --no-requeue"
	BATCH_JOB_SCATTER_DIRECTIVE="#SBATCH --spread-job"
	BATCH_JOB_NNODES_DIRECTIVE="#SBATCH --nodes=$JOB_NNODES"
	BATCH_JOB_NPROC_DIRECTIVE="#SBATCH --ntasks-per-node=$NPROC"
	BATCH_JOB_MEM_DIRECTIVE="#SBATCH --mem=$JOB_MEMORY"'gb'
	BATCH_JOB_NCORE_DIRECTIVE="#SBATCH --ntasks-per-node=$JOB_NCPUS"
else 
	echo "ERROR: Unknown/not supported BATCH_SYSTEM argument $BATCH_SYSTEM (hint: PBS/SLURM are supported)!"
  exit 1
fi


#######################################
##   DEFINE GENERATE EXE SCRIPT FCN
#######################################
create_submit_script(){

	local jobindex=$1
	local shfile=$2
	local exe=$3
	local logfile=$4
	local prmonfile=$5
	
	
	echo "INFO: Creating sh file $shfile (jobindex=$jobindex, exe=$exe)..."
	( 
			echo "#!/bin/bash"
			
			echo "$BATCH_JOB_NAME_DIRECTIVE CalibJob$jobindex"
			echo "$BATCH_JOB_OUTFILE_DIRECTIVE"
			echo "$BATCH_JOB_ERRFILE_DIRECTIVE"
			echo "$BATCH_JOB_JOINOUTERR_DIRECTIVE"
			echo "$BATCH_JOB_WALLTIME_DIRECTIVE"
			echo "$BATCH_JOB_SHELL_DIRECTIVE"
			##echo "$BATCH_JOB_USERGRP_DIRECTIVE"
			echo "$BATCH_JOB_PRIORITY"
			echo "$BATCH_JOB_NOREQUEUE_DIRECTIVE"
			echo "$BATCH_JOB_SCATTER_DIRECTIVE"
			echo "$BATCH_JOB_NNODES_DIRECTIVE"
			echo "$BATCH_JOB_NPROC_DIRECTIVE"
			echo "$BATCH_JOB_MEM_DIRECTIVE"
			echo "$BATCH_JOB_NCORE_DIRECTIVE"

      echo " "
			echo "JOBDIR=$BASEDIR"
			echo 'cd $JOBDIR'

			echo ""

			echo ""
      echo "singularity exec $CONTAINER_OPTIONS $CONTAINER_IMG $exe >& $logfile"

      echo '  echo ""'

      echo " "
      echo " "
      
      echo 'echo "*** END RUN ***"'

 	) > $shfile

	chmod +x $shfile
}
## close function create_submit_script()

#######################################
##     PREPARE JOB
#######################################
## Prepare output dirs
echo "INFO: Preparing output dirs ..."
mkdir -p $OUTPUT_DIR/cal_inspection
mkdir -p $OUTPUT_DIR/cal_values  
mkdir -p $OUTPUT_DIR/log  
mkdir -p $OUTPUT_DIR/runtime
mkdir -p $OUTPUT_DIR/working

## Set storage dir
if [ "$STORAGE_DIR_GIVEN" = false ]; then
	STORAGE_DIR="$OUTPUT_DIR/storage"
	echo "INFO: Setting STORAGE_DIR to $STORAGE_DIR ..."
	mkdir -p $STORAGE_DIR
fi

cd $BASEDIR

#######################################
##     CALIBRATOR CAL - STEP 1
#######################################

file_counter=0
index=1
JOBID_CHAIN=""

if [ "$DO_CALIBRATOR_CAL_STEP1" = true ]; then

	echo "INFO: Prepare calibrator calibration run (looping over files in list $FILELIST_CAL) ..."

	while read filename 
	do
		## Define input/output filenames
		datadir=$(dirname $filename)
		filename_base=$(basename "$filename")
		file_extension="${filename_base##*.}"
		filename_base_noext="${filename_base%.*}"
		inputfile=$filename_base
		echo "filename=$filename, inputfile=$inputfile, datadir=$datadir"

		## Define log file
		logfile="out_calib-RUN$index.log"

		## Define parset file
		parset_file="calib-RUN$index.parset"
		pipeline_file="calib_pipeline-RUN$index.cfg"

		## Define script files
		run_script="run_calib-RUN$index.sh"
		submit_script="submit_calib-RUN$index.sh" 

		## Define job dir
		dataoutdir="$OUTPUT_DIR/working/calib-RUN$index"
		##datadestdir="$OUTPUT_DIR/working"
    datadestdir="$STORAGE_DIR"

		## Generate config files
		echo "INFO: Generating config files $parset_file $pipeline_file ..."
		create_cfg_cal.sh $parset_file $pipeline_file $datadir $inputfile $OUTPUT_DIR $NPROC $NTHREADS

		## Generate run script
		##echo "INFO: Generating run script $run_script and submit script $submit_script ..."
		##create_shfile_step1.sh $run_script $submit_script $parset_file $pipeline_file $CONTAINER_IMG $CONTAINER_OPTIONS	
		echo "INFO: Generating run script $run_script ..."	
		create_run_cal.sh $run_script $parset_file $pipeline_file $dataoutdir $datadestdir
	
		## Generate cal submit script
		echo "INFO: Generating cal submit script $submit_script"
		create_submit_script $index $submit_script $BASEDIR/$run_script $logfile

		# Submits the job to batch system
		export CURRENTJOBDIR=$BASEDIR
		if [ "$SUBMIT" = true ] ; then
			echo "INFO: Submitting script $submit_script to QUEUE $BATCH_QUEUE using $BATCH_SYSTEM batch system (full submit cmd=$BATCH_SUB_CMD $BATCH_QUEUE_NAME_OPTION $BATCH_QUEUE $CURRENTJOBDIR/$submit_script) ..."
			JOB_ID=`$BATCH_SUB_CMD $BATCH_QUEUE_NAME_OPTION $BATCH_QUEUE $CURRENTJOBDIR/$submit_script`
			JOBID_CHAIN="$JOBID_CHAIN:$JOBID"
			echo "INFO: Submitted script $submit_script to queue with job id $JOB_ID ..."
		fi
		if [ "$DO_RUN" = true ] ; then
			echo "INFO: Running script $submit_script locally ..."
			$CURRENTJOBDIR/$submit_script &
		fi

		(( file_counter= $file_counter + 1 ))
		(( index= $index + 1 ))

		## If total number of jobs exceeds the maximum stop everything!
		if [ "$NMAX_PROCESSED_FILES" != "-1" ] && [ $file_counter -ge $NMAX_PROCESSED_FILES ]; then
  		echo "INFO: Maximum number of processed files ($NMAX_PROCESSED_FILES) reached, exit loop..."
  		break;
		fi

	done < "$FILELIST_CAL"
fi

#######################################
##     CALIBRATOR CAL - STEP 2
#######################################

if [ "$DO_CALIBRATOR_CAL_STEP2" = true ]; then

	## Define input output files
	inputfile="*dppp_prep_cal"
	#datadir="$OUTPUT_DIR/working"
	datadir="$STORAGE_DIR"
	
	datadir_output="$OUTPUT_DIR"
	datadir_storage="$STORAGE_DIR"

	## Define config files
	parset_file="calibmerge.parset"
	pipeline_file="calibmerge_pipeline.cfg"	

	## Define log file
	logfile="out_calibmerge.log"

	## Define script files
	run_script="run_calibmerge.sh"
	submit_script="submit_calibmerge.sh"

	## Generate config files
	echo "INFO: Generating cal merge config files $parset_file $pipeline_file ..."
	create_cfg_calmerge.sh $parset_file $pipeline_file $datadir $inputfile $OUTPUT_DIR $NPROC_STEP2 $NTHREADS_STEP2

	## Generate run script
	echo "INFO: Generating cal merge run script $run_script ..."	
	create_run_calmerge.sh $run_script $parset_file $pipeline_file $datadir_output $datadir_storage

	## Generate submit script
	echo "INFO: Generating cal submit script $submit_script"
	create_submit_script $index $submit_script $BASEDIR/$run_script $logfile

	# Submits the job to batch system
	if [ "$SUBMIT" = true ] ; then
		echo "INFO: Submitting script $submit_script to QUEUE $BATCH_QUEUE using $BATCH_SYSTEM batch system ..."
		JOB_ID=`$BATCH_SUB_CMD $BATCH_JOB_DEPENDENCY_OPTION$JOBID_CHAIN $BATCH_QUEUE_NAME_OPTION $BATCH_QUEUE $BASEDIR/$submit_script`
		echo "INFO: Submitted script $submit_script to queue with job id $JOB_ID (dependency list=$BATCH_JOB_DEPENDENCY_OPTION$JOBID_CHAIN) ..."
	fi

fi


#######################################
##     TARGET CAL - STEP 1
#######################################
## TO BE ADDED

#######################################
##     TARGET CAL - STEP 2
#######################################
## TO BE ADDED

echo "*** END SUBMISSION ***"


