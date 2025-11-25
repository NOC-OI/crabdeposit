#!/bin/bash
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd $SCRIPT_DIR
cd ..
mkdir -p testdata
cd testdata
SHAOUT=($(cat *.adc *.hdr *.roi *.tsv | sha256sum -b))
SHAOUT=${SHAOUT[0]}
if [ "$SHAOUT" = "1bfc3f0c25e97627781d20eb476e7fcd9c11ad1fb23b2bb00919de2972875195" ]; then
    echo "Test data OK, skipping download"
else
    echo "Test data hash ($SHAOUT) did not match expected output, redownloading data"
    rm *.adc *.hdr *.roi *.tsv
    wget https://static.indentationerror.com/test_data/crabdeposit.zip
    unzip crabdeposit.zip
    rm crabdeposit.zip
fi
