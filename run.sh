#!/bin/bash

USAGE="docker run -e GCS_BUCKET=<YOUR_GCS_BUCKET_NAME> \
[-e STORAGE_PATH='<ROOT_FOLDER>' ] \
[-e GCS_KEYFILE='<JSON_PRIVATE_KEY>' ] \
[-e GOOGLE_APPLICATION_CREDENTIALS='<GOOGLE_APPLICATION_CREDENTIALS>' ] \
[-e GCP_OAUTH2_REFRESH_TOKEN='<OAUTH_REFRESH_TOKEN>' ] \
-p 5000:5000 \
registry2-gcs"

if [[ -z "${GCS_BUCKET}" ]]; then
  echo "GCS_BUCKET not defined"
  echo
  echo "Usage: $USAGE"
  exit 1
fi

if [[ -n "${GOOGLE_APPLICATION_CREDENTIALS}" ]]; then
  echo "Using Google application credentials in ${GOOGLE_APPLICATION_CREDENTIALS}"
elif [[ -n "${GCS_KEYFILE}" ]]; then
  echo "Using private-key file in ${GCS_KEYFILE}"
elif [[ -n "${GCP_OAUTH2_REFRESH_TOKEN}" ]]; then
  echo "Using refresh token from GCP_OAUTH2_REFRESH_TOKEN"
  cat >key.json <<EOF
{
  "client_id": "32555940559.apps.googleusercontent.com",
  "client_secret": "ZmssLNjJy2998hD4CTg2ejr2",
  "refresh_token": "${GCP_OAUTH2_REFRESH_TOKEN}",
  "type": "authorized_user"
}
EOF
export GOOGLE_APPLICATION_CREDENTIALS=key.json
else

  # fallback on GCE auth
  curl --silent -H 'Metadata-Flavor: Google' \
    http://metadata.google.internal./computeMetadata/v1/instance/service-accounts/default/scopes \
    | grep devstorage > /dev/null
  GCE_SA=$?

  if [[ $GCE_SA -eq 0 ]]; then
    echo "Using credentials from GCE Service Account"
  else
    echo "No credentials found.  Either:"
    echo " - Set GOOGLE_APPLICATION_CREDENTIALS"
    echo " - Set GCS_KEYFILE"
    echo " - Set GCP_OAUTH2_REFRESH_TOKEN"
    echo " - Run in GCE with a service account configured for devstorage access"
    echo
    echo "$USAGE"
    exit 1
  fi
fi
export REGISTRY_LOG_LEVEL=warn
export REGISTRY_STORAGE=gcs
export REGISTRY_STORAGE_GCS_BUCKET="${GCS_BUCKET}"
if [[ -n "${GCS_KEYFILE}" ]]; then
    export REGISTRY_STORAGE_GCS_KEYFILE="${GCS_KEYFILE}"
fi
if [[ -n "${STORAGE_PATH}" ]]; then
    export REGISTRY_STORAGE_GCS_ROOTDIRECTORY="${STORAGE_PATH}"
fi

exec registry $@
