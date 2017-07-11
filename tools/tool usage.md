Remove all Go-swagger folders

Create server command: `swagger generate server --name=weaviate --spec=https://raw.githubusercontent.com/weaviate/weaviate-swagger/develop/weaviate.yaml --default-scheme=https`

copy header.txt and add_header.sh to root folder of project
run `./add_header.sh`

remove header.txt and add_header.sh