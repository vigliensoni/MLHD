#! /opt/sharcnet/python/2.7.3/bin/python

# #######################################
# SUBMISSION
# #######################################

# sqsub -r 1h -f xeon -q mpi --mpp 2GB -o .gather_testing -n 8 python 4_SCRIPTS/MPI/8_extracting_temporal_features_into_one_hdf5.py 0

# for ((i=0; i<=5;i++)); do echo `sqsub -r 1h -f xeon -q mpi --mpp 3GB -o .gather_testing -n 96 python 4_SCRIPTS/MPI/8_extracting_temporal_features_into_one_hdf5.py $i`; done


# ################################################################################
# This script should extract temporal features and store these in one HDF5 
# dataset file
# Working version !
# ################################################################################

from mpi4py import MPI 
import sys, os
import tarfile, gzip
import GVM_classes, Features
from H5 import *
import tempfile
import h5py
from optparse import OptionParser
import copy
import shutil
# import subprocess


comm = MPI.COMM_WORLD 
size = comm.Get_size() 
rank = comm.Get_rank() 

TEMP_FOLDER = tempfile.mkdtemp(dir='/scratch/vigliens/scratch')
PBS_O_WORKDIR = os.environ["PBS_O_WORKDIR"]
SQ_JOBID = os.environ["SQ_JOBID"]

# H5FILES_ROOT = "/scratch/vigliens/GV/1_LASTFM_DATA/7_HDF5/USERS"

def features_to_hdf5(feature_list, dataset_name):
	"""
	Writes the aggregated music listening features 
	into a single hdf5 dataset
	"""
	
	h5 = h5py.File(H5_FILEPATH, 'a')
	h5.require_group('features/aggregated_listening')	
	feature_vector_length = len(feature_list[0])

	try:
		h5['features/aggregated_listening'].require_dataset(dataset_name, (0, feature_vector_length), 'i', maxshape = (None, feature_vector_length))
	except:
		pass
	
	dset = h5['features/aggregated_listening'][dataset_name]
	# Resizing the dataset with the new list of features
	dset.resize((dset.len() + len(feature_list), feature_vector_length))
	dset[dset.len() - len(feature_list):, ] = feature_list #filling the dataset from the first new, empty row

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
	# H5FILES_ROOT = args[1]

	# log_file = open('.extracting_temporal_features.log', 'a')

	# ##########################################################
	# Init parameteres for where userfiles are
	# Creates TAR object and extracts its members to TMP folder
	# ##########################################################	

	# input_dir = '/scratch/vigliens/GV/1_LASTFM_DATA/6_TEST/2_1_TAR/IN' #test case
	input_dir = '/scratch/vigliens/GV/1_LASTFM_DATA/2_ALL_607_GZIP_TAR_2GB' #general case
	H5_FILEPATH = "/scratch/vigliens/GV/1_LASTFM_DATA/7_HDF5/aggregated_listening.h5"

	file_list = [] # List of all files in input_dir
	for root, subFolders, files in os.walk(input_dir):
		for f in files:
			file_list.append('/'.join([root,f]))
	tar_object = tarfile.open('/'.join([file_list[size * int(factor) + rank]]))
	tar_object.extractall(TEMP_FOLDER)

	print size * int(factor) + rank, file_list[size * int(factor) + rank]

	# ##########################################################
	# Iterate over all files in a TAR, searching for all songs
	# ##########################################################

	list_of_dict_agg_feat = [] #list with dictionaries fo aggregated features
	for file_in_tar in GVM_classes.folder_iterator(TEMP_FOLDER)[:]:
	# try:			
		# Data extraction
		user_features = Features.ListeningFeatures(file_in_tar)	# Initializes object for feature extraction 
		dict_aggregated_features = user_features.all_aggregated_frequencies()
		list_of_dict_agg_feat.append(dict_aggregated_features)
		del user_features, dict_aggregated_features

	shutil.rmtree(TEMP_FOLDER)
	comm.Barrier() 

	# ##########################################################
	# Gather on root
	# ##########################################################
	
	# dict_agg_feat_list = copy.deepcopy(comm.gather(list_of_dict_agg_feat, root=0))
	dict_agg_feat_list = comm.gather(list_of_dict_agg_feat, root=0)
	del list_of_dict_agg_feat

	if rank == 0:
		freq_per_hour_daily = []
		freq_per_hour_weekly = []
		freq_per_day_of_the_week = []
		freq_per_month = []
		freq_per_yearday = []
		for di in dict_agg_feat_list:
			for d in di:
				freq_per_hour_daily.append([d['lfid']] + d['freq_per_hour_daily'])
				freq_per_hour_weekly.append([d['lfid']] + d['freq_per_hour_weekly'])
				freq_per_day_of_the_week.append([d['lfid']] + d['freq_per_day_of_the_week'])
				freq_per_month.append([d['lfid']] + d['freq_per_month'])
				freq_per_yearday.append([d['lfid']] + d['freq_per_yearday'])

		features_to_hdf5(freq_per_hour_daily, 'freq_per_hour_daily')
		features_to_hdf5(freq_per_hour_weekly, 'freq_per_hour_weekly')
		features_to_hdf5(freq_per_day_of_the_week, 'freq_per_day_of_the_week')
		features_to_hdf5(freq_per_month, 'freq_per_month')
		features_to_hdf5(freq_per_yearday, 'freq_per_yearday')

		del dict_agg_feat_list, freq_per_hour_daily, freq_per_hour_weekly, freq_per_day_of_the_week, freq_per_month, freq_per_yearday

	# except Exception, e:
	# 	# log_file.write(str(e) + '\n')
	# 	log_file.write(str(user_features.lfid) + '\t' + user_features.username + '\t' + str(file_list[size * int(factor) + rank]) + '\t' + str(e) + '\n')



	# log_file.close()


