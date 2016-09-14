#! /opt/sharcnet/python/2.7.3/bin/python

# #######################################
# SUBMISSION
# #######################################

# sqsub -r 4m -f xeon -q mpi -o test_mpi -n 2 python ./4_SCRIPTS/MPI/1_song_scrobbled_in.py 0


from mpi4py import MPI 
import sys, os
import tarfile, gzip
import GVM_classes
import Features
import tempfile
import h5py
from optparse import OptionParser



rank = MPI.COMM_WORLD.Get_rank() 
size = MPI.COMM_WORLD.Get_size() 
name = MPI.Get_processor_name()
# print "\nHelloworld! I am process {0} of {1} on {2}".format(rank, size, name)

rank_lead_zero = "%02d" % (rank + 1,)
TEMP_FOLDER = tempfile.mkdtemp(dir='/scratch/vigliens/scratch')
PBS_O_WORKDIR = os.environ["PBS_O_WORKDIR"]
SQ_JOBID = os.environ["SQ_JOBID"]






if __name__ == "__main__":
	usage = "usage: %prog [options] factor"
	opts = OptionParser(usage = usage)
	opts.add_option('-f', '--hdf5', dest='h5')
	options, args = opts.parse_args()

	# Factor to overcome the 256 CPUs limitation.
	# Each run must have a different factor
	# E.g., to run the 583 TAR files, it would be 
	# possible to do 256, 256, and 71. However
	# it is more balanced to do 192, 192, and 199; as in 
	# sqsub -r 12m -q mpi -o test_mpi -n 192 python ./4_SCRIPTS/MPI/helloworld.py [0-2]

	factor = args[0]

	# # ############
	# # OPTIONS
	# # ############
	# if options.h5 == 'user':
	# 	# output_filepath = os.path.join(args[1], 'consolidated_daily_freqs.txt')
	# 	pass
	# elif options.h5 == 'song':
	# 	# output_filepath = os.path.join(args[1], 'consolidated_weekly_freqs.txt')
	# 	pass
	# elif options.h5 == 'billboard':
	# 	# h5 = h5py.File('/scratch/vigliens/GV/1_LASTFM_DATA/7_HDF5/BILLBOARD/billboard.h5')
	# 	h5 = h5py.File('/tmp/billboard.h5')
	# 	h5.require_group('songs')
	# 	song_mbid = '28fcedcc-1f1f-439a-8743-100730c748b1' #maroon 5 song
	# 	h5['songs'].require_group(song_mbid)
	# 	h5['songs'][song_mbid].require_dataset('metadata', (1, ), dtype='i8')
	# 	# h5['songs'][song_mbid].require_dataset('users', (1, ), dtype='i8')
	# 	h5['songs'][song_mbid].require_dataset('utc_per_user', (10, ), dtype='i8', maxshape=(None, None))
	# 	pass
	# else:
	# 	'You must provide a destination for the hdf5 file'
	# 	sys.exit()


		
	# file_out = open('/'.join([PBS_O_WORKDIR, str(SQ_JOBID), 'file_out.dat']), 'w')

	

	# Init parameters
	input_dir = '/scratch/vigliens/GV/1_LASTFM_DATA/2_ALL_607_GZIP_TAR_2GB'
	file_list = [] # List of all files in input_dir
	for root, subFolders, files in os.walk(input_dir):
		for f in files:
			file_list.append('/'.join([root,f]))

	# prints what is being processed		
	# print file_list[size * int(factor) + rank], TEMP_FOLDER

	# Creates TAR object and extracts its members to TMP folder
	tar_object = tarfile.open('/'.join([file_list[size * int(factor) + rank]]))
	tar_object.extractall(TEMP_FOLDER)

	# Iterates over all members and prints first line

	# song_mbid = '28fcedcc-1f1f-439a-8743-100730c748b1' #maroon 5 song
	song_mbid = '0a6ada57-8fb4-42a7-baf5-b71c73e41303' #sayitright

	# If billboard option, create a dataset for each song
	# if options.h5 == 'billboard':
	# 	h5['songs'].require_group(song_mbid)
	# 	h5['songs'][song_mbid].require_dataset('metadata', (1, ), dtype='i8')
	# 	h5['songs'][song_mbid].require_dataset('utc_per_user', (1, ), dtype='i8', maxshape=(None, None))


	out_file = open(PBS_O_WORKDIR+'/'+SQ_JOBID+'_out.dat', 'a')
	print_data = []
	for member in GVM_classes.folder_iterator(TEMP_FOLDER):
		# Initializes object for feature extraction 

		try:
			user_features = Features.ListeningFeatures(member)
			utc_times_per_song = user_features.utc_times_per_song(song_mbid)
		except Exception, e:
			print file_list[size * int(factor) + rank], member, e, '\n'

		


		# Print only if listener listened to the song
		if len(utc_times_per_song) > 0:
			# print user_features.lfid, '\t', len(utc_times_per_song), '\t', utc_times_per_song
			out_file.write('\t'.join([str(user_features.lfid), user_features.username, str(len(utc_times_per_song)), str(utc_times_per_song), '\n']))
			# file_out.write('\t'.join([str(user_features.lfid), str(len(utc_times_per_song)), str(utc_times_per_song), '\n']))

	out_file.close()

	MPI.Finalize()



