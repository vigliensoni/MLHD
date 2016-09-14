#! /opt/sharcnet/python/2.7.5/intel/bin/python

# sqsub -r 1h -f xeon -q mpi --mpp 4GB -o .reduce_test -n 2 python 4_SCRIPTS/MPI/13_filtering_close_scrobbles_python.py 0

# for ((i=0; i<=293;i++)); do echo `sqsub -r 1h -f xeon -q mpi --mpp 4GB -o .ranking_track_saw_1h_2n_4g -n 2 python 4_SCRIPTS/MPI/13_filtering_close_scrobbles_python.py $i`; done


# ##################################################
# This scripts uses MPI instead of sequential UNIX
# to filtering log files with duplicates or close
# scrobbles
# ##################################################


from mpi4py import MPI 
import sys, os
import tarfile, gzip
from optparse import OptionParser
import tempfile
import GVM_classes, Features


comm = MPI.COMM_WORLD 
size = comm.Get_size() 
rank = comm.Get_rank() 

TEMP_FOLDER = tempfile.mkdtemp(dir='/scratch/vigliens/scratch')
# PBS_O_WORKDIR = os.environ["PBS_O_WORKDIR"]
try:
	SQ_JOBID = os.environ["SQ_JOBID"]
except:
	SQ_JOBID = '_STARR'

if __name__ == "__main__":
	usage = "usage: %prog [options] factor"
	opts = OptionParser(usage = usage)
	# opts.add_option('-f', '--hdf5', dest='h5')
	options, args = opts.parse_args()

	factor = args[0]

	# input_dir = '/Users/gabriel/9_TEST/IN_SMALL' # LOCAL GVM
	# input_dir = '/Users/gabriel/9_TEST/IN_MEDIUM' # LOCAL GVM
	# input_dir = '/Users/gabriel/Dropbox/9_TEST/1_TARS/' # LOCAL STARR
	input_dir = '/scratch/vigliens/GV/1_LASTFM_DATA/6_TEST/1_TARS' # simplest case
	# input_dir = '/scratch/vigliens/GV/1_LASTFM_DATA/6_TEST/2_1_TAR/IN' #8 TAR files
	# input_dir = '/scratch/vigliens/GV/1_LASTFM_DATA/2_ALL_607_GZIP_TAR_2GB' #general case
	
	# input_dir = '/work/vigliens/GV/1_LASTFM_DATA/2_ALL_607_GZIP_TAR_2GB' 
	print 'input dir:', input_dir

	# ##########################################################
	# Extracts all files within a TAR to TEMP_FOLDER
	# TODO: Extract only in one node? (which one) and bcast to 
	# the other ones
	# ##########################################################


	
	file_list = [] # List of all files in input_dir
	for root, subFolders, files in os.walk(input_dir):
		for f in files:
			if f == '.DS_Store':
				continue
			file_list.append('/'.join([root,f]))
	filename = file_list[size * int(factor) + rank]
	tar_object = tarfile.open('/'.join([filename]]))
	tar_object.extractall(TEMP_FOLDER)

	print size * int(factor) + rank, filename	



	# ################################
	# Actual filtering and saving
	# ################################

	# Iterate over all files in a TAR
	file_list = GVM_classes.folder_iterator(TEMP_FOLDER)
	print 'file_list', file_list
	for file_in_tar in file_list:
		if file_in_tar.split('/')[-1].startswith('.'):
			continue
		# lf = Features.ListeningFeatures(file_in_tar) 
		userdata = Features.LogFiltering(file_in_tar)
		userdata = userdata.scrobble_filtering()

		# Dumping filtered data to GZIP
		with gzip.open(file_in_tar, 'wb') as f:
			for line in userdata:
				f.write(line)


	# ##########################################################
	# TAR tempfolder and move back to final position 
	# ##########################################################
	# out_filepath = '/scratch/vigliens/'
	out_filepath = '/scratch/vigliens/GV/1_LASTFM_DATA/6_TEST/1_TARS'
	out_tarfile_name = 'all_files_test_' + rank + '.tar'
	for file_in_tar in file_list:
		if file_in_tar.split('/')[-1].startswith('.'):
			continue
		with tarfile.open(out_tarfile_name, 'w') as tar:
			for name in file_list:
				tar.add(name)

	print out_filepath + out_tarfile_name
	os.rename(out_tarfile_name, out_filepath + out_tarfile_name)











