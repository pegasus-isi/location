# location

Mapping jobs to the resource they ran at

## service

Service for Pegasus jobs to call home to.

    cd location
    docker build -t pegasus/location .
    docker push pegasus/location

## location-summary

Aggregates/transforms records to a new index.

    cd location-summary
    docker build -t pegasus/location-summary .
    docker push pegasus/location-summary

