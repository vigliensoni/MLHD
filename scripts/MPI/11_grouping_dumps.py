#! /opt/sharcnet/python/2.7.5/intel/bin/python

# sqsub -r 10h -q serial --mpp 4GB -o /work/vigliens/GV/8_RESULTS/grouping_artists_10h_4g_1n_saw.log -n 1 python 4_SCRIPTS/MPI/11_grouping_dumps.py /scratch/vigliens/GV/8_RESULTS/ranking_artists/ /scratch/vigliens/GV/8_RESULTS/grouped_ranking_artists.dump

# sqsub -r 10h -q serial -f xeon --mpp 4GB -o /work/vigliens/GV/8_RESULTS/grouping_albums -n 1 python 4_SCRIPTS/MPI/11_grouping_dumps.py

import numpy as np
import GVM_classes
import copy
import time
from optparse import OptionParser


if __name__ == "__main__":
	usage = "usage: %prog [options] input_folder output_filepath"
	opts = OptionParser(usage = usage)
	# opts.add_option('-f', '--hdf5', dest='h5')
	options, args = opts.parse_args()

	# ###########################################
	#  ARGS
	# ###########################################

	if len(args) != 2:
		print 'You should provide an:\n\n1) input_folder\n2) output filepath'
		sys.exit("\nEnter the proper values\n")

	input_folder = args[0]
	output_filepath = args[1]
	
	# input_folder = '/work/vigliens/GV/8_RESULTS/ranking_albums/'
	input_files_list = GVM_classes.folder_iterator(input_folder)


	# A better approach is to create smaller functions so each variable 
	# has a shorter lifetime between creation and being dereferenced when 
	# the namespace is removed at function exit


	x_grouped = np.load(input_files_list[0])
	for i, filepath in enumerate(input_files_list[1:]):
		t0 = time.time()

		x = np.load(filepath)
		dummy_array = np.array(GVM_classes.group_by_key(np.append(x, x_grouped)), dtype = x.dtype)
		del x_grouped

		x_grouped = copy.copy(dummy_array)
		del x, dummy_array	

		print i, filepath, int(time.time()-t0)


	npydt = np.dtype({'mbid': ('S40', 0), 'freq': (np.int64, 40)})
	x_grouped_np = np.array(x_grouped, dtype = npydt)
	x_grouped_np.dump(output_filepath)















