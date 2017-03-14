import gzip
import GVM_classes


class MetadataFeatureExtractor:
    """Extract metadata information from first line of files."""

    def metadata(self, user_filepath):
        """Constructor given a filepath of a GZIP TXT file."""
        userdata = gzip.GzipFile(user_filepath).readline()
        return userdata

folderpath = "/Volumes/ListenBUP1/9_ALL_BUP"
files = GVM_classes.folder_iterator(folderpath)


outputfile = open('/Users/gabriel/Downloads/metadata_with_creation_date.tsv', 'ab')


for file in files:
    # extracting creation date
    f = GVM_classes.FileMetadata()
    creationdate = f.birthdate(file)

    # extracting first line of metatada
    u = MetadataFeatureExtractor()
    metadata = u.metadata(file)
    # removes newline at the end
    metadata = metadata.strip()
    # append file creation date to metadata
    metadata = '\t'.join([metadata, str(creationdate)])
    print metadata
    outputfile.write(metadata)
    outputfile.write('\n')
