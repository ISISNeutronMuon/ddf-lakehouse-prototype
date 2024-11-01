# Certificates

The subdirectories contain the certificate request configurations for the
project. Each subdirectory is named by the `YYYY-MM` that the request was made.

The keys and certificates are stored in a shared folder in Keeper.

## Generate a new certificate request

- Make a new directory named using the current year/month in the format above.
- Copy the previous certificate request configuration file (*.cnf) to the new directory

The next step depends on whether you already have a key. If you have an existing key, run:

```sh
openssl req -nodes -new -key [SERVER_NAME].key -out [SERVER_NAME].csr -config [SERVER_NAME].cnf
```

If you need to generate a new key, run:

```sh
openssl req -nodes -new -newkey rsa:4096 -keyout [SERVER_NAME].key -out [SERVER_NAME].csr -config [SERVER_NAME].cnf
```

where `[SERVER_NAME]` should be replaced by the domain name of the server.
