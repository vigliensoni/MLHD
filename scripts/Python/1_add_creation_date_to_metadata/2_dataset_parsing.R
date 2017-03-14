# This script takes data from 1_ and converts it into an R dataframe to be processed for creating a nice dataset of user metadata

# load dataset with creation date data
library(readr)
metadata_with_creation_date <- read_delim("/Users/gabriel/Documents/2_CODE/MLHD/scripts/Python/1_add_creation_date_to_metadata/output_data/metadata_with_creation_date.tsv", 
    "\t", escape_double = FALSE, col_names = c("username", "lfid", "age", "country", "gender", "type_int", "playcount_OR", "registered_UTC", "registered_human", "age_days_scrobbles", "user_type", "mean_scrobbles_per_day", "data_collection_UTC"), 
    trim_ws = TRUE)

# remove malformed rows (645 AND 573897)
metadata_with_creation_date <- metadata_with_creation_date[-c(645, 573897), ]

# drop non-interesting columns
metadata_with_creation_date <- subset(metadata_with_creation_date, select = -c(type_int, registered_human, age_days_scrobbles, mean_scrobbles_per_day))


# creating UUID for each row (listener)
uuid_list <- replicate(nrow(metadata_with_creation_date), UUIDgenerate())

# appending these uuids to the dataframe
metadata_with_creation_date['uuid'] <- uuid_list
# order the columns
metadata_with_creation_date <- metadata_with_creation_date[, c(10, 1, 2, 3, 4, 5, 6, 8, 7, 9)]

# save the dataframe
write.table(metadata_with_creation_date, file = "/Users/gabriel/Documents/2_CODE/MLHD/scripts/Python/1_add_creation_date_to_metadata/output_data/2_metadata_with_creation_date_and_uuid.tsv", row.names=FALSE, sep="\t", quote = FALSE)

