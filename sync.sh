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
    fileCount=$(aws s3 --profile $AWS_PROFILE ls $AWS_BUCKET --recursive | awk '{ print $4}' | awk 'END{print NR}')
    # fileCount string to int convertion
    fileCount=$(($fileCount + 0))
    return fileCount
}

# -------------------------------------------------------------------------------------------------
# grab metadata from aws s3 and store it in local files under ./metadata/[key] 
# where [key] is the key path in s3 of the object
# --------------------------------------------------------------------------------------------------
getMetaData(){
    OIFS="$IFS"
    IFS=$'\n'
    for file in $(find metadata -type f ) ; do
        file=$(echo $file | cut -d '/' -f2-)
        echo "$file"
        aws --profile $AWS_PROFILE s3api head-object --bucket $AWS_BUCKET --key "$file" | tee "metadata/$file"
    done
    IFS="$OIFS"
}

# -------------------------------------------------------------------------------------------------
# loops through all files in ./metadata allowing for spaces in file names
# edit the contents of the ./metadata files so that they only contain the value of the metadata json object
# of the HEADER json information that was stored there before
# use -c option of jq so that it is minified json
# --------------------------------------------------------------------------------------------------
truncateMetadata() {
    OIFS="$IFS"
    IFS=$'\n'
    for file in $(find metadata -type f ) ; do
        cat "$file" | jq -c .Metadata | tee "$file"
    done
    IFS="$OIFS"
}

# -------------------------------------------------------------------------------------------------
# loop through all files in ./objects allowing for spaces in filenames
# uploads all files in ./objects to the ceph target bucket
# includes upload into object header, the user defined metadata contained in the contents of the 
# corresponding ./metadata/[key] file on local
# --------------------------------------------------------------------------------------------------
uploadToCeph() {
    OIFS="$IFS"
    IFS=$'\n'
    for file in $(find objects -type f ) ; do
            file=$(echo $file | cut -d '/' -f2-)
            aws s3 --profile $CEPH_PROFILE --endpoint=$CEPH_ENDPOINT cp objects/$file s3://$CEPH_BUCKET/$file --metadata "$(cat metadata/$file)"
    done
    IFS="$OIFS"
}

#---------------------------------------------------------------------------------------------------------
# loops through all files in ./object to display the object header results found in the ceph target bucket
# use this to manually "eyeball" validate that the metadata made it into the target object header in ceph
#---------------------------------------------------------------------------------------------------------
showResultsInCephTarget(){
    OIFS="$IFS"
    IFS=$'\n'
    for file in $(find objects -type f ) ; do
            file=$(echo $file | cut -d '/' -f2-)
            aws --profile $CEPH_PROFILE --endpoint=$CEPH_ENDPOINT s3api head-object --bucket $CEPH_BUCKET --key $file
    done
    IFS="$OIFS"
}

##############################
# MAIN
##############################

# Step 1: Reads the input arguments
while [ "$1" != "" ]; do
    case $1 in
        --aws-bucket ) shift
            AWS_BUCKET=$1
            ;;
        --aws-profile ) shift
            AWS_PROFILE=$1
            ;;
        --ceph-profile ) shift
            CEPH_PROFILE=$1
            ;;
        --ceph-bucket ) shift
            CEPH_BUCKET=$1
            ;;
        --ceph-endpoint ) shift
            CEPH_ENDPOINT=$1
            ;;
        * )
            echo "$1 is not a recognized parameter"
            exit 1
            ;;
    esac
    shift
done

# Step 2: Validate required arguments
if [ -z "${AWS_BUCKET}" ] && [ -z "${AWS_PROFILE}" ] && [ -z "${CEPH_PROFILE}" ] && [ -z "${CEPH_BUCKET}" ] && [ -z "${CEPH_ENDPOINT}" ]; then
  Write-Output """you must specify an S3 Bucket with --bucket [bucket-name]; 
    the profile name in ~/.aws/credentials for connecting to the bucket in AWS with --aws-profile [profile name]
    the profile
    """
  exit 1
fi


consoleOut "sync all s3 (objects only) into ./objects on local disk"
mkdir -p objects
aws s3 --profile $AWS_PROFILE sync s3://$AWS_BUCKET objects

consoleOut "rsync all objects to metadata dir" 
# we need to prime the local file system with the files we need to store metadata
mkdir -p metadata
rsync -a ./objects/* metadata/

consoleOut "truncate all files in metadata to 0 bytes"
# we don't want them to be the actual objects these files are empty shells for the metadata
find ./metadata -type f -exec truncate -s 0 {} \;

consoleOut "dump metadata from aws s3 into corresponding files ./metadata/[key]"
getMetaData

consoleOut "truncating metadata files to only user defined metadata"
truncateMetadata

consoleOut "uploading files to ceph bucket"
uploadToCeph

consoleOut "displaying metadata results in target ceph bucket"
showResultsInCephTarget