#! /opt/sharcnet/python/2.7.3/bin/python

# #######################################
# SUBMISSION
# #######################################

# sqsub -r 20m -q mpi -f xeon --mpp 2GB -o ./work/vigliens/GV/8_RESULTS/fringe_and_expl.log -n 8 python 4_SCRIPTS/MPI/10_extracting_fringeness_to_hdf5.py 0 /work/vigliens/GV/1_LASTFM_DATA/2_ALL_607_GZIP_TAR_2GB/ /scratch/vigliens/GV/8_RESULTS/fringe_and_exploratory.h5

# for ((i=0; i<=5;i++)); do echo `sqsub -r 2h -q mpi --mpp 2GB -o /work/vigliens/GV/8_RESULTS/fringe_and_expl_2h_2g_96n_saw.log -n 96 python 4_SCRIPTS/MPI/10_extracting_fringeness_to_hdf5.py $i /work/vigliens/GV/1_LASTFM_DATA/2_ALL_607_GZIP_TAR_2GB/ /scratch/vigliens/GV/8_RESULTS/fringe_and_exploratory.h5`; done

# ################################################################################
# This script should extract listening behaviour features and store these in a 
# single HDF5  dataset file
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
import time


comm = MPI.COMM_WORLD 
size = comm.Get_size() 
rank = comm.Get_rank() 

try:
	TEMP_FOLDER = tempfile.mkdtemp(dir='/scratch/vigliens/scratch')
except:
	print 'Creating TEMP folder in /var/'
	TEMP_FOLDER = tempfile.mkdtemp()

try:
	SQ_JOBID = os.environ["SQ_JOBID"]
except:
	SQ_JOBID = '_STARR'

# PBS_O_WORKDIR = os.environ["PBS_O_WORKDIR"]



if __name__ == "__main__":
	usage = "usage: %prog [options] factor input_folder output_folder H5_FILEPATH"
	opts = OptionParser(usage = usage)
	# opts.add_option('-f', '--hdf5', dest='h5')
	options, args = opts.parse_args()

	# ###########################################
	#  ARGS
	# ###########################################

	if len(args) != 3:
		print 'You should provide a:\n\n1) factor\n2) input_folder\n3) H5 file outpath'
		sys.exit("\nEnter the proper values\n")

	factor = args[0]
	input_folder = args[1]
	H5_FILEPATH = args[2]

	# ##########################################################
	# Creates TAR object and extracts its members to TMP folder
	# ##########################################################	

	file_list = [] # List of all files in input_folder
	for root, subFolders, files in os.walk(input_folder):
		for f in files:
			file_list.append('/'.join([root,f]))
	tar_object = tarfile.open('/'.join([file_list[size * int(factor) + rank]]))
	tar_object.extractall(TEMP_FOLDER)

	print size * int(factor) + rank, file_list[size * int(factor) + rank]

	# ##########################################################
	# Iterate over all files in a TAR, searching for all songs
	# ##########################################################

	t0 = time.time()
	list_of_dict_agg_feat = [] #list with dictionaries of aggregated features
	for file_in_tar in GVM_classes.folder_iterator(TEMP_FOLDER)[:]:
		collected_features = dict()
		# Data extraction
		listening_features = Features.ListeningFeatures(file_in_tar)

		collected_features['metatada'] = listening_features.metadata_dict()
		collected_features['fringeness'] = listening_features.fringeness()
		collected_features['exploratoryness'] = listening_features.exploratoryness()

		list_of_dict_agg_feat.append(collected_features)

	print 'Rank ', rank, ' features in ', str(int(time.time() - t0)), ' secs'

	comm.Barrier() 

	# ##########################################################
	# Gather on root
	# ##########################################################
	
	
	dict_agg_feat_list = comm.gather(list_of_dict_agg_feat, root=0)
	del list_of_dict_agg_feat

	if rank == 0:
		# Setting up the HDF5 file
		h = H5(H5_FILEPATH)
		h._groups_and_datasets_checker()

		# Writing data
		for di in dict_agg_feat_list:
			for d in di:
				# Populating the compound dataset using H5 library
				h.populate_compound_dataset('features/listening_behaviour/fringeness', d['fringeness'])
				h.populate_compound_dataset('features/listening_behaviour/exploratoryness', d['exploratoryness'])


		h.close()


	# shutil.rmtree(TEMP_FOLDER)
