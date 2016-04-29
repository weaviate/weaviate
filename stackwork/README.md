## Stackwork
Stackwork is used to run full stack integration tests.

### Prerequisites
- Docker 1.10.0 or higher (Stackwork uses the Docker CLI)
- Docker Compose 1.6.0 or higher (Stackwork uses the Docker Compose CLI)
- JVM (to run Gradle Wrapper - Gradle itself is 'installed' by Wrapper)

### Have Stackwork build images and start / test/ tear down automatically

    ./gradlew stackworkCheck

### Build the images and run manually
From the main directory, run `./gradlew buildImage -i`.
In the output, look for `:stackwork:docker-server:buildImage` and then the created images id (e.g. `Docker image id: IMAGE_ID`).
Run the server with `docker run --rm -it IMAGE_ID`.
