#!/bin/bash

# ----------------------------------------------------------------------------------------------
# outputs text to the console and wraps it in a ## block to make it stand out
#
# @param $1 - the text to output
# 
# ------------------------------------------------------------------------------------------------
consoleOut(){
    echo -e "\n##################################################"
    echo -e "$1"
    echo -e "####################################################\n"
}

# -----------------------------------------------------------------------------------------------
# pipes the file list into awk and uses awk to count the number of rows in the file list
#
# returns an int thats equal to the number of files in the s3 bucket
# ------------------------------------------------------------------------------------------------
getS3FileCount(){
    fileCount=$(aws s3 --profile $AWS_PROFILE ls $BUCKET --recursive | awk '{ print $4}' | awk 'END{print NR}')
    # fileCount string to int convertion
    fileCount=$(($fileCount + 0))
    return fileCount
}

# -------------------------------------------------------------------------------------------------
# dumps the s3 object header data (including user defined metadata) into files on the local system
# dependency an existence of the s3 bucket dir and file structure under a ./metadata dir
# will dump the header data into a filename and path of the object to which is the header data
#  
# xargs has a buffer limit so this will page'ify the object list in s3 to buffer it to xargs
# -------------------------------------------------------------------------------------------------
storeMetaData(){
    fileCount=getS3FileCount
    if [[ $fileCount -lt 500 ]];
    then
        pageSize=$fileCount
    else
        pageSize=500
    fi
    start=1
    end=$pageSize
    for ((i=1; end<=$fileCount; i++))
    do
    #aws s3 --profile $AWS_PROFILE ls $BUCKET --recursive | awk '{ print $4}' | awk "NR==$start,NR==$end{print}" | xargs -I {} -d '\n' /bin/bash -c "aws --profile $AWS_PROFILE s3api head-object --bucket $BUCKET --key \"$1\" | tee \"metadata/$1\"" -- {}
    aws s3 --profile $AWS_PROFILE ls $BUCKET --recursive | awk '{ print $4}' | awk "NR==$start,NR==$end{print}" | xargs -I {} -d '\n' /bin/bash -c 'aws --profile lscs s3api head-object --bucket idg-cbso-test-bucket --key "$1" | tee "metadata/$1"' -- {}
    start=$(($i+$pageSize))
    end=$(($i+$pageSize+$pageSize))
    i=$(($i+$pageSize))
    done
    if [[ $start -le $fileCount ]];
    then
        #aws s3 --profile $AWS_PROFILE ls $BUCKET --recursive | awk '{ print $4}' | awk "NR==$start,NR==$fileCount{print}" | xargs -I {} -d '\n' /bin/bash -c 'aws --profile $AWS_PROFILE s3api head-object --bucket lenovo-dcg-lscs-cbso-pcg-mvp-dev --key "$1" | tee "metadata/$1"' -- {}
        echo something here
    fi
}

##############################
# MAIN
##############################

# Step 1: Reads the input arguments
while [ "$1" != "" ]; do
    case $1 in
        --bucket ) shift
            BUCKET=$1
            ;;
        --aws-profile ) shift
            AWS_PROFILE=$1
            ;;
        --ceph-profile ) shift
            CEPH_PROFILE=$1
            ;;
        * )
            echo "$1 is not a recognized parameter"
            exit 1
            ;;
    esac
    shift
done

# Step 2: Validate required arguments
if [ -z "${BUCKET}" ] && [ -z "${AWS_PROFILE}" ]; then
  Write-Output """you must specify an S3 Bucket with --bucket [bucket-name]; 
    the profile name in ~/.aws/credentials for connecting to the bucket in AWS with --aws-profile [profile name]
    the profile
    """
  exit 1
fi


# sync all s3 (objects only) locally
mkdir -p objects
aws s3 --profile $AWS_PROFILE sync s3://$BUCKET objects

# rsync all to metadata dir 
# we need to prime the local file system with the files we need to store metadata
mkdir -p metadata
rsync -a ./objects/* metadata/

# truncate all files in metadata to 0 bytes
# we don't want them to be the actual objects these files are empty shells for the metadata
find ./metadata -type f -exec truncate -s 0 {} \;

storeMetaData

consoleOut "starting to cat files"

OIFS="$IFS"
IFS=$'\n'
for file in $(find metadata -type f ) ; do
        cat "$file" | jq .Metadata | tee "$file"
done
IFS="$OIFS"
