
# Kafka Stream Playground

```
# compile jar file
mvn compile assembly:single

# build local docker image
docker build . -t xx

# run application
docker run --rm -d --network host -v ${PWD}/target/streams.examples-0.1-jar-with-dependencies.jar:/tmp/target/streams.examples-0.1-jar-with-dependencies.jar --name xx0 xx
```

## Experiments

Check threading model, tasks distribution.
```
# Create containers
sh start_10_containers.sh

# Check task distribution from log
docker logs xx9 | grep Thread | grep active

# Cleanup
docker stop xx0 xx1 xx2 xx3 xx4 xx5 xx6 xx7 xx8 xx9
```