#!/home/vigliens/python/bin/python

# sqsub -r 1h -f xeon -q mpi --mpp 8GB -o 23_graphlab -n 8 /home/vigliens/python/bin/python /home/vigliens/Documents/2_CODE/4_SCRIPTS/MPI/23_graphlab_mini_example.py


# for ((i=0; i<=47;i++)); do echo `sqsub -r 4h -f xeon -q mpi --mpp 4GB -o 23_graphlab_0_01 -n 8 /home/vigliens/python/bin/python /home/vigliens/Documents/2_CODE/4_SCRIPTS/MPI/23_graphlab_0_01.py $i`; done


# for ((i=192; i<=287;i++)); do echo `sqsub -r 4h -f xeon -q threaded --mpp 8GB -o 23_graphlab_0_1_orca_threaded_$i -n 8 /home/vigliens/python/bin/python /home/vigliens/Documents/2_CODE/4_SCRIPTS/MPI/23_graphlab_0_1_orca.py $i`; done


# THREADED 2h 16 cores
# for ((i=96; i<=191;i++)); do echo `sqsub -r 2h -f xeon -q threaded --mpp 8GB -o 23_graphlab_0_1_orca_threaded_$i -n 16 /home/vigliens/python/bin/python /home/vigliens/Documents/2_CODE/4_SCRIPTS/MPI/23_graphlab_0_1_orca.py $i`; done



# ##################################################
# This scripts should
# 1. Run full demographics features experiment in 1% of the data
# with a .9/.1 split for train/testing. This is the one running now 
# (384 jobs), though the filename says different
# ##################################################


from mpi4py import MPI
import os
from optparse import OptionParser
# import tempfile
import graphlab as gl
import GraphlabHelpers as GH
import itertools
import GVM_classes


comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()
print "SIZE:{0}, RANK:{1}".format(size, rank)

# TEMP_FOLDER = tempfile.mkdtemp(dir='/scratch/vigliens')  # ComputeCanada
OUT_FOLDER = '/scratch/vigliens/out_0_1/'  # ComputeCanada
# print 'TEMP_FOLDER: ', TEMP_FOLDER
# TEMP_FOLDER = tempfile.mkdtemp(dir='/Users/gabriel/scratch/') # Local

# PBS_O_WORKDIR = os.environ["PBS_O_WORKDIR"]
try:
    SQ_JOBID = os.environ["SQ_JOBID"]
except:
    SQ_JOBID = '_STARR'

print 'SQ_JOBID:{0}'.format(SQ_JOBID)


def init_parameters():
    """
    Returns a list with all combinations of init parameters in the form

    [regularization_val, num_factor, side_data_combination]

    [1e-05, 50, ['usid', 'age']]

    """
    # 6 features generate 32 combinations
    side_data_combinations = GVM_classes.permutations(['age',
                                                       'gender',
                                                       'country',
                                                       'mainstreamness_artist',
                                                       'exploratoryness_artist'])
    # adding the user ID only
    side_data_combinations = GVM_classes.listprepender(side_data_combinations, field='usid')
    side_data_combinations.insert(0, ['usid'])

    num_factors = [50, 100, 200]
    # num_factors = [200]
    regularization_vals = [1e-05, 1e-06, 1e-07, 1e-08]
    # regularization_vals = [1e-08]

    parameters = [regularization_vals, num_factors, side_data_combinations]
    parameter_combination = list(itertools.product(*parameters))  #combinatory of all parameters
    return parameter_combination



def observed_data_loader(path='/scratch/vigliens/9_GL_MODELS/0_1/full'):
    """
    """
    observed_data = gl.SFrame(path)
    full_train, full_test = GH.datasetsplit(observed_data, split=0.9)
    return full_train, full_test


def model_training(observation_data,
                   num_factors,
                   regularization_vals,
                   side_data_factorization,
                   user_data,
                   solver):
    """
    Creates a factorization recommender

    Returns a model
    """
    model = gl.factorization_recommender.create(
        observation_data=observation_data,
        user_id='usid',
        item_id='mbid',
        target='rating',
        user_data=user_data,
        side_data_factorization=side_data_factorization,
        max_iterations=50,
        num_factors=num_factors,
        regularization=regularization_vals,
        solver=solver)
    return model


def storing_results(out_folder, file_no, regularization_vals, num_factors, user_side_data_par, train, test, time):
    """
    """
    with open(out_folder + str(file_no), 'w') as out_file:
        out_file.write('\t'.join([str(train),
                                 str(test),
                                 str(regularization_vals),
                                 str(num_factors),
                                 str(int(time)),
                                 str('.'.join(user_side_data_par))
                                 ]))
        out_file.write('\n')

if __name__ == "__main__":
    usage = "usage: %prog [options] factor"
    opts = OptionParser(usage=usage)
    # # opts.add_option('-f', '--hdf5', dest='h5')
    options, args = opts.parse_args()
    i = int(args[0])
    # data_local = "/Users/gabriel/Documents/5_DATA/FREQUENCY_LIKERT/RATINGS_BY_TIME_BRACKETS/0_01/full_filtered"




    # 1. INITIALIZATION PARAMETERS
    par_combination = init_parameters()

    file_no = i * size + rank
    regularization_vals = par_combination[file_no][0]
    num_factors = par_combination[file_no][1]
    user_side_data_par = par_combination[file_no][2]

    print ",".join([str(file_no), str(regularization_vals), str(num_factors), str(user_side_data_par)])

    # 2. LOADING DATA. TRAINING AND TESTING SETS SHOULDNT BE FIXED????
    # observation data
    # full_train, full_test = observed_data_loader('/scratch/vigliens/9_GL_MODELS/0_1/full')  # CC
    full_train = gl.SFrame('/scratch/vigliens/9_GL_MODELS/0_1/full_train')
    full_test = gl.SFrame('/scratch/vigliens/9_GL_MODELS/0_1/full_test')
    # full_train, full_test = observed_data_loader('/Users/gabriel/Documents/5_DATA/FREQUENCY_LIKERT/RATINGS_BY_TIME_BRACKETS/0_01/full_filtered')  #LOCAL

    # userside data
    usd = gl.SFrame('/scratch/vigliens/9_GL_MODELS/METADATA/metadata_and_three_features_preprocessed/')  #CC
    # usd = gl.SFrame('/Users/gabriel/Documents/5_DATA/METADATA/metadata_and_three_features_preprocessed')  #LOCAL
    usdf = usd.select_columns(user_side_data_par)

    model = model_training(observation_data=full_train,
                           num_factors=num_factors,
                           regularization_vals=regularization_vals,
                           side_data_factorization=True,
                           user_data=usdf,
                           solver='adagrad')

    # compare model with test test
    fact, reg, train, test = GH.model_comparison_pandas(model, full_test)

# gl.set_runtime_config('GRAPHLAB_DEFAULT_NUM_PYLAMBDA_WORKERS', 2)


    storing_results(OUT_FOLDER, format(file_no, '03'), regularization_vals, num_factors, user_side_data_par, train, test, model.training_time)












