## Magnum ZIO Example

This is a simple example of how to use ZIO with Magnum.

### Prerequisites

- Docker
- SBT

### Setup

```sh
cd magnum-zio-example     # Go to the example project folder
docker compose up -d      # Start the PostgreSQL database container and apply the sql/migrations in the background
```

### Running the Application

```sh
cd ..                     # Go back to the root folder of the project
sbt magnumZioExample/run  # Run the application
```

### Shutting Down

```sh
cd magnum-zio-example     # Go to the example project folder
docker compose down -v    # Stop the PostgreSQL database container and remove the volumes
```
