#! /opt/sharcnet/python/2.7.3/bin/python

# #######################################
# SUBMISSION
# #######################################

# sqsub -r 3h -f xeon -q mpi --mpp 2GB -o extracting_temporal_features -n 2 python ./4_SCRIPTS/MPI/7_extracting_temporal_features.py 0 

# sqsub -r 20m -f xeon -q mpi --mpp 2GB -o extracting_temporal_features -n 2 python ./4_SCRIPTS/MPI/7_extracting_temporal_features.py 0 
# sqsub -r 40m -f xeon -q mpi --mpp 2GB -o billboard_to_h5 -n 2 python ./4_SCRIPTS/MPI/6_checking_double_usernames.py 0 

# ################################################################################
# This script should extract temporal features and store these in HDF5 files
# per user
# ################################################################################

from mpi4py import MPI 
import sys, os
import tarfile, gzip
import GVM_classes, Features
from H5 import *
import tempfile
import h5py
from optparse import OptionParser
import subprocess

rank = MPI.COMM_WORLD.Get_rank() 
size = MPI.COMM_WORLD.Get_size() 
name = MPI.Get_processor_name()

TEMP_FOLDER = tempfile.mkdtemp(dir='/scratch/vigliens/scratch')
PBS_O_WORKDIR = os.environ["PBS_O_WORKDIR"]
SQ_JOBID = os.environ["SQ_JOBID"]

# H5FILES_ROOT = "/scratch/vigliens/GV/1_LASTFM_DATA/7_HDF5/USERS"

def retrieving_aggregated_features(scrobbling_file):
	"""
	Retrieves scrobble data from UNIX script for computing aggregated features
	String filepath -> Dictionary
	"""

	script_filepath = "/work/vigliens/GV/2_CODE/4_SCRIPTS/14_aggregated_features_4_FEATURES.sh" #SHARCNET
	# script_filepath = "/Users/gabriel/Dropbox/1_PHD_VC/2_PROJECTS/3_LISTENING_BEHAVIOUR/4_SCRIPTS/14_aggregated_features_4_FEATURES.sh" #LOCAL
	features = subprocess.check_output([script_filepath, scrobbling_file])
	split_features = features.strip().split('\n')

	
	features_dict = { 'lfid' : int(split_features[1]),
					'username' : split_features[0],
					'age' : split_features[2],
					'country' : split_features[3],
					'gender' : split_features[4],
					'subscriber' : split_features[5], 
					'playcount' : int(split_features[6]),
					'registered_UNIX' : int(split_features[7]),
					'registered_HUMAN' : split_features[8], 
					'age_scrobbles' : int(split_features[9]),
					'user_type' : split_features[10],
					'mean_per_day_scrobbles' : int(split_features[11]),

					'freq_per_hour_daily' : [int(x) for x in split_features[15].split(' ')],
					'freq_per_hour_weekly' : [int(x) for x in split_features[16].split(' ')],
					'freq_per_day_of_the_week' : [int(x) for x in split_features[17].split(' ')],
					'freq_per_month' : [int(x) for x in split_features[18].split(' ')],
					'freq_per_yearday' : [int(x) for x in split_features[19].split(' ')],
					'freq_per_hour_weekdays' : [float(x) for x in split_features[20].split(' ')],
					'freq_per_hour_saturday' : [int(x) for x in split_features[21].split(' ')],
					'freq_per_hour_sunday' : [int(x) for x in split_features[22].split(' ')] 
					}

	return features_dict



def aggregated_features_to_hdf5(dict_aggregated_features):
	"""
	Writes the aggregated features to an HDF5 file
	Dict -> HDF5 File
	"""

	# print 'data:{0}'.format(dict_aggregated_features)
	# Check if dataset exists, if not, create it
	lfid = str(dict_aggregated_features['lfid'])
	h5c = H5_check(H5FILES_ROOT, lfid)
	root_filepath = h5c.hierarchical_folder_creator()
	h5_filepath = '/'.join([root_filepath, lfid + '.h5'])
	h5 = H5(h5_filepath, 'a', driver='core')
	
	# dset = h5['/features/aggregated_listening/aggregated_features']
	# h5.populate_dataset(dset, dict_aggregated_features)
	
	h5.populate_group_of_datasets('/features/aggregated_listening', dict_aggregated_features)
	h5.populate_compound_dataset('/info/lastfm_metadata', dict_aggregated_features)
	h5.close()





if __name__ == "__main__":
	usage = "usage: %prog [options] factor H5_FILES_ROOT"
	opts = OptionParser(usage = usage)
	# opts.add_option('-f', '--hdf5', dest='h5')
	options, args = opts.parse_args()

	# Factor to overcome the 256 CPUs limitation.
	# Each run must have a different factor
	# E.g., to run the 583 TAR files, it would be 
	# possible to do 256, 256, and 71. However
	# it is more balanced to do 192, 192, and 199; as in 
	# sqsub -r 12m -q mpi -o test_mpi -n 192 python ./4_SCRIPTS/MPI/helloworld.py [0-2]

	factor = args[0]
	H5FILES_ROOT = args[1]

	log_file = open('.extracting_temporal_features.log', 'a')


	# Init parameteres for where userfiles are
	# Creates TAR object and extracts its members to TMP folder
	input_dir = '/scratch/vigliens/GV/1_LASTFM_DATA/2_ALL_607_GZIP_TAR_2GB'
	file_list = [] # List of all files in input_dir
	for root, subFolders, files in os.walk(input_dir):
		for f in files:
			file_list.append('/'.join([root,f]))
	tar_object = tarfile.open('/'.join([file_list[size * int(factor) + rank]]))
	tar_object.extractall(TEMP_FOLDER)

	print rank, file_list[size * int(factor) + rank]

	# ##########################################################
	# Iterate over all files in a TAR, searching for all songs
	# ##########################################################

	for file_in_tar in GVM_classes.folder_iterator(TEMP_FOLDER):
	# try:			
		# Data extraction
		user_features = Features.ListeningFeatures(file_in_tar)	# Initializes object for feature extraction 
		user_scrobbling_data = user_features.userdata
		
		# Store the data in a temp file
		user_scrobbling_data_file = tempfile.NamedTemporaryFile(delete=False)
		for line in user_scrobbling_data:
			user_scrobbling_data_file.write(line)	
		user_scrobbling_data_file.close()

		# If HDF5 file already exists, continue with the next file
		h5c = H5_check(H5FILES_ROOT, str(user_features.lfid))
		if h5c.file_exists() is True:
			continue

		# Retrieve the features from the temp file
		dict_aggregated_features = retrieving_aggregated_features(user_scrobbling_data_file.name)
		aggregated_features_to_hdf5(dict_aggregated_features)

		# Delete the temp file
		os.unlink(user_scrobbling_data_file.name)

	# except Exception, e:
	# 	# log_file.write(str(e) + '\n')
	# 	log_file.write(str(user_features.lfid) + '\t' + user_features.username + '\t' + str(file_list[size * int(factor) + rank]) + '\t' + str(e) + '\n')



	log_file.close()
	MPI.Finalize()


