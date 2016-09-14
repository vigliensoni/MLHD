#! /opt/sharcnet/python/2.7.3/bin/python

# #######################################
# SUBMISSION
# #######################################

# sqsub -r 10m -f xeon -q mpi --mpp 3GB -o .9_aggregated_listening -n 8 python 4_SCRIPTS/MPI/9_aggregated_listening_and_metadata_2_hdf5.py 0

# for ((i=0; i<=5;i++)); do echo `sqsub -r 2h -f xeon -q mpi --mpp 3GB -o .9_aggregated_listening -n 96 python 4_SCRIPTS/MPI/9_aggregated_listening_and_metadata_2_hdf5.py $i`; done


# ################################################################################
# This script extracts listeners metadata and stores it into one single dataset 
# of a HDF5 file
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


def metadata_to_hdf5(metadata_list):
	"""
	Writes a list of listener metadata into a dataset of an hdf5 file
	List of Dict -> File
	"""

	# print 'metadata_list:{0}'.format(metadata_list)

	h5 = h5py.File(H5_FILEPATH, 'a')
	h5.require_group('info')
	try:
		h5['info'].require_dataset('metadata', (0,), 
			dtype=[('username', '|S64'), ('lfid', '<i4'), ('age', '<i4'), 
			('country', '|S2'), ('gender', '|S1'), ('subscriber', '<i4'), 
			('playcount', '<i4'), ('age_scrobbles', '<i4'), ('user_type', '|S8'), 
			('registered', '<i4'), ('firstscrobble', '<i4'), ('lastscrobble', '<i4')], 
			maxshape = (None, ))
	except:
		pass

	dset = h5['info/metadata']
	dset.resize((dset.len() + len(metadata_list), ))
	dset[dset.len() - len(metadata_list) : ] = metadata_list

	h5.close()






if __name__ == "__main__":
	usage = "usage: %prog [options] factor"
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
	# H5_FILEPATH = "/scratch/vigliens/GV/1_LASTFM_DATA/7_HDF5/test.h5" # test case
	H5_FILEPATH = "/scratch/vigliens/GV/1_LASTFM_DATA/7_HDF5/metadata_registration_fixed_600K.h5"

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

	# list_of_dict_agg_feat = [] #list with dictionaries fo aggregated features

	metadata_list = []

	for file_in_tar in GVM_classes.folder_iterator(TEMP_FOLDER)[:]:
		try:			

			metadata_features = Features.MetadataFeatures(file_in_tar)	# Initializes object for feature extraction 
			metadata_dict = metadata_features.metadata_dict()
			metadata_list.append((metadata_dict['username'],
								metadata_dict['lfid'],
								metadata_dict['age'],
								metadata_dict['country'],
								metadata_dict['gender'],
								metadata_dict['subscriber'],
								metadata_dict['playcount'],
								metadata_dict['age_scrobbles'],
								metadata_dict['user_type'],
								metadata_dict['registered'],
								metadata_dict['firstscrobble'],
								metadata_dict['lastscrobble']))
			
			del metadata_features, metadata_dict
		except:
			print file_in_tar

	shutil.rmtree(TEMP_FOLDER)
	comm.Barrier() 

	# ##########################################################
	# Gather on root
	# ##########################################################
	

	metadata_list = comm.gather(metadata_list, root=0)

	# print 'metadata_all:{0}'.format(metadata_all)

	if rank == 0:
		for metadata in metadata_list:
			metadata_to_hdf5(metadata)



