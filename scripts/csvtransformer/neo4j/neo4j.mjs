import path from "path";
import {execSync} from "child_process"; import neo4j_driver from "neo4j-driver";
import inquirer from 'inquirer';

/// Export from neo4j to CSV
const neo4jToCSVExport = () => {
    // Access the variables
    const hostname = process.env.NEO4J_HOSTNAME;
    const username = process.env.NEO4J_USERNAME;
    const password = process.env.NEO4J_PASSWORD;
    const local_csv_folder = process.env.LOCAL_CSV_FOLDER_NEO4J
    const container_name = process.env.NEO4j_DOCKER_CONTAINER_NAME;

    const csv_files_directory_inside_container = "/var/lib/neo4j/import"

    // Create a Neo4j driver instance
    const driver = neo4j_driver.driver(`bolt://${hostname}`, neo4j_driver.auth.basic(username, password));

    const cypherQuery = 'CALL apoc.export.csv.all("neo4j.csv", {bulkImport:true})';

    const session = driver.session();

    // Run the Cypher query session
    session.run(cypherQuery)
        .then((_) => {

            execSync(`rm -rf ${local_csv_folder}`);
            console.log(`Deleted contents of ${local_csv_folder} folder if it existed`)
            // Additional code for downloading the exported CSV file
            execSync(`docker cp "${container_name}:${csv_files_directory_inside_container}" ./${local_csv_folder}`)

            console.log('CSV export query executed successfully');

            session.close();
            driver.close();
        })
        .catch((error) => {
            console.error('Error executing the query', error);
            session.close();
            driver.close();
        });
}

/// Import from CSV to neo4j
const CSVToNeo4jImport = (csv_relays, csv_success_circuits_relationship, csv_failed_circuits_relationship) => {
    const container_name = process.env.NEO4j_DOCKER_CONTAINER_NAME;
    const container_csv_folder = process.env.CONTAINER_CSV_FOLDER;
    const username = process.env.NEO4J_USERNAME;
    const password = process.env.NEO4J_PASSWORD;
    const database_name = process.env.NEO4J_DATABASE;

    execSync(`docker exec ${container_name} rm -rf ${container_csv_folder}`)
    execSync(`docker exec ${container_name} mkdir ${container_csv_folder}`)

    let csv_relays_path_in_container = path.join(container_csv_folder, getFileNameFromPath(csv_relays));
    execSync(`docker cp ${csv_relays} ${container_name}:${csv_relays_path_in_container}`);
    console.log(`Copied ${csv_relays} to ${csv_relays_path_in_container} in the container ${container_name}`);

    let csv_success_circuits_relationship_path_in_container = path.join(container_csv_folder, getFileNameFromPath(csv_success_circuits_relationship));
    execSync(`docker cp ${csv_success_circuits_relationship} ${container_name}:${csv_success_circuits_relationship_path_in_container}`);
    console.log(`Copied ${csv_success_circuits_relationship} to ${csv_success_circuits_relationship_path_in_container} in the container ${container_name}`);

    let csv_failed_circuits_relationship_path_in_container = path.join(container_csv_folder, getFileNameFromPath(csv_failed_circuits_relationship));
    execSync(`docker cp ${csv_failed_circuits_relationship} ${container_name}:${csv_failed_circuits_relationship_path_in_container}`);
    console.log(`Copied ${csv_failed_circuits_relationship} to ${csv_failed_circuits_relationship_path_in_container} in the container ${container_name}`);


    // We'll show the current no of nodes and relationships present in the database
    // if it's not 0 and 0  then we'll ask the user whether to continue or not because we'll have to either 
    // make the user to select "delete" or "overwrite"
    let [totalNodes, totalRelationships] = execSync(`docker exec ${container_name} cypher-shell --non-interactive -u ${username} -p ${password} -d ${database_name} "MATCH (n) RETURN count(n) as totalNodes; MATCH ()-[r]->() RETURN count(r) as totalRelationships;" | awk '/^[0-9]+$/'`).toString().match(/\d+/g).map(value => parseInt(value));
    console.log(`The database ${database_name} currently has ${totalNodes} Nodes and ${totalRelationships} Relationships`);

    const addDataOnDatabase = () => {
        console.log(`Adding nodes and relationships into the database "${database_name}", please wait...`);
        let output = execSync(`docker exec ${container_name} neo4j-admin database import full --overwrite-destination --nodes ${csv_relays_path_in_container} --relationships ${csv_success_circuits_relationship_path_in_container} --relationships ${csv_failed_circuits_relationship_path_in_container}`);
        console.log(output.toString());
        console.log("Imported data into neo4j!!!");
    }
    try {
        if (totalNodes != 0 || totalRelationships != 0) {
            inquirer.prompt([
                {
                    type: 'list',
                    name: 'action',
                    message: 'Overwrite (W) or Delete and Add (D)?',
                    choices: ['W', 'D'],
                },
            ]).then(({action}) => {
                if (action === "W") {
                    addDataOnDatabase();
                } else if (action === "D") {
                    execSync(`docker exec ${container_name} cypher-shell -u ${username} -p ${password} -d ${database_name} "MATCH (n) DETACH DELETE n;"`);
                    console.log(`Deleted ${totalNodes} Nodes and ${totalRelationships} Relationships...`);
                    addDataOnDatabase();
                }

            });

        } else {
            addDataOnDatabase();
        }


    } catch (e) {
        throw e;
    }
}

const getFileNameFromPath = (path) => {
    let path_parts = path.split('/');
    return path_parts[path_parts.length - 1];
}

export {
    neo4jToCSVExport,
    CSVToNeo4jImport
};

