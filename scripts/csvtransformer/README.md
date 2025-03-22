# Exporting from database to CSV

You can export data of either **sqlite3** or **neo4j** database to **CSV** files, they can also be used later on to import back from **CSV** to **sqlite3** or **neo4j** database.

The CSV files that are produced while exporting from either of the database are -

- ```nodes.Relay.csv``` : It contains integer mapped with fingerprint of the Relays.
- ```relationships.CIRCUIT_SUCCESS.csv``` :  Two of it's columns represent the integer from ```nodes.Relay.csv```, which denotes the first hop and second hop of the two hop circuit creation attempt i.e relay combination that were successful, with timestamp and  message.
- ```relationships.CIRCUIT_FAILURE.csv``` : It contains relay combination that failed in creating two hop circuit, with timestamp and message

## From ```sqlite3``` to ```CSV```:

Let's consider that you have **erpc.db**, a sqlite3 database flat file that has
stored all the contents after running the erpc tool in the same directory as
**index.mjs**. Also, you copy **.env.sample** file to **.env** and it contains the following key value pair:

```sh
LOCAL_CSV_FOLDER_SQLITE3 = ./export_sqlite3 # The relative path where you would want all the exported CSV to be saved
```

After you've created the **.env** file, you can convert the contents of
the database within the **erpc.db** file to **CSV** using the following command

```bash
node index.mjs export --sqlite3 ./erpc.db
```
Now, the **CSV** files are created in the path ```LOCAL_CSV_FOLDER_SQLITE3```, as mentioned in the **.env** file.
They can be used for importing data back to **neo4j** and **sqlite3** later on.



## From ```neo4j``` to ```CSV```:

Let's consider that you have all the data after running the erpc tool in a **neo4j** database(in a docker container). In order to export those data
to **CSV** format, as the very first step you would need to provide an **.env** file in the same root folder as **index.mjs** file.
The key value pairs in the **.env** file should be in following format:
```sh
NEO4J_HOSTNAME = localhost # The hostname of neo4j db, behind the scenes the bolt protocol is used to access the data
NEO4J_USERNAME = neo4j  # The username of the neo4j db
NEO4J_PASSWORD = password # The password of neo4j db
NEO4j_DOCKER_CONTAINER_NAME = neo4j-neo4j-1 # The docker container name where neo4j is running
LOCAL_CSV_FOLDER_NEO4J = ./export_neo4j # The relative path where you would want all the exported CSV to saved
```
Once you are sure that your neo4j docker container is running, run the following command -
```bash
node index.mjs export --neo4j
```
Now, the **CSV** files are created in the path ```LOCAL_CSV_FOLDER_NEO4J```, as mentioned in the **.env** file.
They can be used for importing data back to **neo4j** and **sqlite3** later on.

# Importing ```CSV``` to database

Now that you have all 3 **CSV** files built from either **sqlite3** or **neo4j**,  you can simply create a fresh **sqlite3** db out of it or import the data
into **neo4j** db.

## From ```CSV``` to ```sqlite3```

You will have to provide all the different **CSV** files using the following command.

```bash
node index.mjs import --relays export_sqlite3/nodes.Relay.csv --success_circuits_relationship \
export_sqlite3/relationships.CIRCUIT_SUCCESS.csv --failed_circuits_relationship \
export_sqlite3/relationships.CIRCUIT_FAILURE.csv --sqlite3 erpc.imported.db
```
The above command creates a database flat file called **erpc.imported.db**.

## From ```CSV``` to ```neo4j```

To import from existing **CSV** files, you would need to have following variables in the **.env** file -

```sh
NEO4J_DATABASE = neo4j # The database name inside of neo4j db where we want to import the data, in community edition only one database is given to us i.e neo4j
NEO4J_USERNAME = neo4j  # The username of the neo4j db
NEO4J_PASSWORD = password # The password of neo4j db
NEO4j_DOCKER_CONTAINER_NAME = neo4j-neo4j-1 # The docker container name where neo4j is running
CONTAINER_CSV_FOLDER = /output # Folder where the locally specified CSV files will be copied to inside of the running container instance
```

after that you can simply run the command -

```sh
node index.mjs import --relays export_sqlite3/nodes.Relay.csv --success_circuits_relationship \
export_sqlite3/relationships.CIRCUIT_SUCCESS.csv --failed_circuits_relationship \
export_sqlite3/relationships.CIRCUIT_FAILURE.csv --neo4j
```

After you are done importing, you won't see any change if you were to query in the cypher-shell. You would need to restart your docker container. See [this stackoverflow issue](https://stackoverflow.com/questions/52119414/after-neo4j-admin-import-going-ok-i-cant-see-any-nodes)
