#!/bin/bash

##
# CREATES OVERVIEW OF LICENSE FILES
##

LICENSEFILE="LICENSE-DEPENDENCIES.md"

echo -e "---" > $LICENSEFILE
echo -e "publishedOnWebsite: false" >> $LICENSEFILE
echo -e "title: Contextionary" >> $LICENSEFILE
echo -e "subject: OSS" >> $LICENSEFILE
echo -e "---\n" >> $LICENSEFILE
echo -e "# Licenses of Dependencies\n" >> $LICENSEFILE
echo -e "More information about this file can be found [here](https://github.com/creativesoftwarefdn/weaviate/tree/develop/docs/en/contribute/licenses.md).\n" >> $LICENSEFILE

##
# Install license software
##
go get -u github.com/pmezard/licenses

##
# Add licenses as a table, assumes GOPATH is set correctly!
##
LICENSES=$(licenses -a -w github.com/creativesoftwarefdn/weaviate/cmd/weaviate-server)

echo "## PACKAGE and LICENCE\n" >> $LICENSEFILE

echo '```markdown' >> $LICENSEFILE

while IFS= read -r LICENSE
do
    echo "$LICENSE" >> $LICENSEFILE
done < <(printf '%s\n' "$LICENSES")

echo '```' >> $LICENSEFILE
