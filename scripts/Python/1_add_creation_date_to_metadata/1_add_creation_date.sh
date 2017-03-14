# iterate thorugh all files in folder

for file in *

do	
	# retrieving username
	# username=`echo "$file" | cut -d'.' -f1`
	# echo "$username"
	
	# convert formatted date to UNIX timestamp
	birthtime=`stat -s $file | cut -f 12 -d " " | cut -f 2 -d "="`
	echo "$birthtime"


	# retrieving all metadata and adding the file creation date
	data=`zless $file | head -1`
	echo "$data"


done



