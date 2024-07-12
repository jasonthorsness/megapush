#!/bin/bash

set -e

BUCKET_NAME="megapush"
ROLE_NAME="megapush-role"
POLICY_NAME="megapush-policy"
USER_NAME="megapush-user"
SESSION_NAME="megapush-session"

get_region() {
    if [ -n "$AWS_REGION" ]; then
        REGION=$AWS_REGION
    else
        read -p "Enter the AWS region (e.g., us-west-2): " REGION
    fi
}

get_region

aws s3api create-bucket --bucket $BUCKET_NAME --region $REGION --create-bucket-configuration LocationConstraint=$REGION

LIFECYCLE_POLICY=$(cat <<EOF
{
    "Rules": [
        {
            "Status": "Enabled",
            "Prefix": "",
            "Expiration": {
                "Days": 1
            },
            "ID": "AutoDeletionRule"
        }
    ]
}
EOF
)

aws s3api put-bucket-lifecycle-configuration --bucket $BUCKET_NAME --lifecycle-configuration "$LIFECYCLE_POLICY"

aws iam create-user --user-name $USER_NAME
until aws iam get-user --user-name "$USER_NAME" > /dev/null 2>&1; do echo "Waiting for IAM user $USER_NAME to be created..."; sleep 2; done

POLICY_DOCUMENT=$(cat <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket",
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject"
            ],
            "Resource": [
                "arn:aws:s3:::$BUCKET_NAME",
                "arn:aws:s3:::$BUCKET_NAME/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": "sts:AssumeRole",
            "Resource": "arn:aws:iam::$(aws sts get-caller-identity --query Account --output text):role/$ROLE_NAME"
        }
    ]
}
EOF
)

aws iam put-user-policy --user-name $USER_NAME --policy-name $POLICY_NAME --policy-document "$POLICY_DOCUMENT"

ACCESS_KEYS=$(aws iam create-access-key --user-name $USER_NAME)
ACCESS_KEY_ID=$(echo $ACCESS_KEYS | jq -r .AccessKey.AccessKeyId)
SECRET_ACCESS_KEY=$(echo $ACCESS_KEYS | jq -r .AccessKey.SecretAccessKey)

TRUST_POLICY=$(cat <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::$(aws sts get-caller-identity --query Account --output text):user/$USER_NAME"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}
EOF
)

aws iam create-role --role-name $ROLE_NAME --assume-role-policy-document "$TRUST_POLICY"

aws iam put-role-policy --role-name $ROLE_NAME --policy-name $POLICY_NAME --policy-document "$POLICY_DOCUMENT"

until AWS_ACCESS_KEY_ID=$ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY=$SECRET_ACCESS_KEY aws sts assume-role --role-arn arn:aws:iam::$(aws sts get-caller-identity --query Account --output text):role/$ROLE_NAME --role-session-name $SESSION_NAME --duration-seconds 3600 > /dev/null 2>&1; do echo "Waiting for IAM role $ROLE_NAME to be assumable..."; sleep 2; done
ASSUMED_ROLE=$(AWS_ACCESS_KEY_ID=$ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY=$SECRET_ACCESS_KEY aws sts assume-role --role-arn arn:aws:iam::$(aws sts get-caller-identity --query Account --output text):role/$ROLE_NAME --role-session-name $SESSION_NAME --duration-seconds 3600)

SESSION_ACCESS_KEY_ID=$(echo $ASSUMED_ROLE | jq -r .Credentials.AccessKeyId)
SESSION_SECRET_ACCESS_KEY=$(echo $ASSUMED_ROLE | jq -r .Credentials.SecretAccessKey)
SESSION_TOKEN=$(echo $ASSUMED_ROLE | jq -r .Credentials.SessionToken)

export AWS_ACCESS_KEY_ID=$SESSION_ACCESS_KEY_ID
export AWS_SECRET_ACCESS_KEY=$SESSION_SECRET_ACCESS_KEY
export AWS_SESSION_TOKEN=$SESSION_TOKEN

echo "Temporary credentials have been set as environment variables."
echo "AWS_ACCESS_KEY_ID: $SESSION_ACCESS_KEY_ID"
echo "AWS_SECRET_ACCESS_KEY: $SESSION_SECRET_ACCESS_KEY"
echo "AWS_SESSION_TOKEN: $SESSION_TOKEN"
echo "AWS_REGION: $REGION"