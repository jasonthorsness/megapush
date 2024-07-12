#!/bin/bash

BUCKET_NAME="megapush"
ROLE_NAME="megapush-role"
POLICY_NAME="megapush-policy"
USER_NAME="megapush-user"

get_region() {
    if [ -n "$AWS_REGION" ]; then
        REGION=$AWS_REGION
    else
        read -p "Enter the AWS region (e.g., us-west-2): " REGION
    fi
}

get_region
aws s3 rm s3://$BUCKET_NAME --recursive
aws s3api delete-bucket --bucket $BUCKET_NAME --region $REGION
aws iam delete-role-policy --role-name $ROLE_NAME --policy-name $POLICY_NAME
aws iam delete-role --role-name $ROLE_NAME
aws iam delete-user-policy --user-name $USER_NAME --policy-name $POLICY_NAME
ACCESS_KEYS=$(aws iam list-access-keys --user-name $USER_NAME)
for key in $(echo $ACCESS_KEYS | jq -r '.AccessKeyMetadata[].AccessKeyId'); do
    aws iam delete-access-key --user-name $USER_NAME --access-key-id $key
done
aws iam delete-user --user-name $USER_NAME
