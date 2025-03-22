import sqlite3 from "sqlite3";
import {execSync} from "child_process";
import fs from "fs";
import fastcsv from 'fast-csv';

const RELAY_LABEL_FOR_NEO4J = "Relay";
const CIRCUIT_FAILURE_LABEL_FOR_NEO4J = "CIRCUIT_FAILURE";
const CIRCUIT_SUCCESS_LABEL_FOR_NEO4J = "CIRCUIT_SUCCESS";

const TABLE_NAME_FOR_STORING_SUCCESS_CIRCUITS = "success_circuits";
const TABLE_NAME_FOR_STORING_FAILED_CIRCUITS = "failed_circuits";
const TABLE_NAME_FOR_STORING_RELAYS = "relays";

class Relay {
    constructor(index, fingerprint) {
        this.index = index;
        this.fingerprint = fingerprint;
    }

    toCSV() {
        return `${this.index},${this.fingerprint},${RELAY_LABEL_FOR_NEO4J}`;
    }
}

class Circuit {
    constructor(firsthop, secondhop, message, timestamp) {
        this.firsthop = firsthop;
        this.secondhop = secondhop;
        this.message = message;
        this.timestamp = timestamp;
    }

    toCSV(is_failure) {
        return `${this.firsthop},${this.secondhop},${is_failure ? CIRCUIT_FAILURE_LABEL_FOR_NEO4J : CIRCUIT_SUCCESS_LABEL_FOR_NEO4J},${this.message},${this.timestamp}`;
    }

}

const createCSVFromCircuits = (circuits, is_failure) => {
    let heading = ":START_ID,:END_ID,:TYPE,message,timestamp";
    circuits.forEach((circuit) => {
        heading += '\n';
        heading += circuit.toCSV(is_failure);

    });

    return heading;
}

const createCSVFromRelays = (relays) => {
    let heading = ":ID,fingerprint,:LABEL";
    relays.forEach((relay) => {
        heading += '\n';
        heading += relay.toCSV();
    });
    return heading;
}


// Export sqlite3 data to CSV
const sqlite3ToCSVExport = (sqlite3DbPath) => {

    const local_csv_folder = process.env.LOCAL_CSV_FOLDER_SQLITE3;
    const db = new sqlite3.Database(sqlite3DbPath, sqlite3.OPEN_READONLY);

    const idToRelaysMap = {};
    const allRelays = [];
    const allSuccessCircuits = [];
    const allFailedCircuits = [];

    // Get all Relays
    const get_relays_query = "SELECT fingerprint FROM relays";
    const get_success_circuits_query = `SELECT firsthop, secondhop, message, timestamp FROM ${TABLE_NAME_FOR_STORING_SUCCESS_CIRCUITS}`;
    const get_failed_circuits_query = `SELECT firsthop,secondhop,message,timestamp FROM ${TABLE_NAME_FOR_STORING_FAILED_CIRCUITS}`;


    // Read data from the database
    db.all(get_relays_query, [], (err, rows) => {
        if (err) {
            console.error(err.message);
            return;
        }

        rows.forEach((row, i) => {
            let relay = new Relay(i, row.fingerprint);
            idToRelaysMap[row.fingerprint] = relay;
            allRelays.push(relay);
        })

        let csv = createCSVFromRelays(allRelays);

        execSync(`rm -rf ${local_csv_folder}`);
        console.log(`Deleted contents of ${local_csv_folder} folder if it existed`)
        execSync(`mkdir ${local_csv_folder}`);

        fs.writeFile(`${local_csv_folder}/nodes.Relay.csv`, csv, (err) => {
            if (err) {
                console.error('Error writing to the file:', err);
            } else {
                console.log(`${local_csv_folder}/nodes.Relay.csv was created successfully`);
            }
        });

        db.all(get_success_circuits_query, [], (err, circuits) => {
            if (err) {
                console.error(err.message);
                return;
            }

            circuits.forEach((circuit) => {
                let start_id = idToRelaysMap[circuit.firsthop].index;
                let end_id = idToRelaysMap[circuit.secondhop].index;
                let _circuit = new Circuit(start_id, end_id, `"${circuit.message}"`, circuit.timestamp);

                allSuccessCircuits.push(_circuit);
            });

            let csv = createCSVFromCircuits(allSuccessCircuits, false);

            // Write the data to the file
            fs.writeFile(`${local_csv_folder}/relationships.CIRCUIT_SUCCESS.csv`, csv, (err) => {
                if (err) {
                    console.error('Error writing to the file:', err);
                } else {
                    console.log(`${local_csv_folder}/relationships.CIRCUIT_SUCCESS.csv was created successfully`);
                }
            });
        });


        db.all(get_failed_circuits_query, [], (err, circuits) => {
            if (err) {
                console.error(err.message);
                return;
            }

            circuits.forEach((circuit) => {
                let start_id = idToRelaysMap[circuit.firsthop].index;
                let end_id = idToRelaysMap[circuit.secondhop].index;
                let _circuit = new Circuit(start_id, end_id, `"${circuit.message}"`, circuit.timestamp);

                allFailedCircuits.push(_circuit);
            });


            let csv = createCSVFromCircuits(allFailedCircuits, true);

            // Write the data to the file
            fs.writeFile(`${local_csv_folder}/relationships.CIRCUIT_FAILURE.csv`, csv, (err) => {
                if (err) {
                    console.error('Error writing to the file:', err);
                } else {
                    console.log(`${local_csv_folder}/relationships.CIRCUIT_FAILURE.csv was created successfully`);
                }
            });

        });
    });
}

const BATCH_SIZE = 1000;

const addRelaysToDb = async (sqlite3DbPath, csv_relays) => {
    const idToRelaysMap = {};
    const allRelays = [];


    return new Promise((resolve, reject) => {
        const db = new sqlite3.Database(sqlite3DbPath, (err) => {
            if (err) {
                console.log(err);
                reject(err);
            } else {
                db.run(`CREATE TABLE IF NOT EXISTS ${TABLE_NAME_FOR_STORING_RELAYS}(fingerprint TEXT PRIMARY KEY)`, (e) => {
                    if (!e) {
                        fs.createReadStream(csv_relays)
                            .pipe(fastcsv.parse({headers: true}))
                            .on("error", (_) => {
                                return;
                            })
                            .on('data', (row) => {
                                let relay = new Relay(row[':ID'], row.fingerprint);
                                idToRelaysMap[row[':ID']] = relay;
                                allRelays.push(relay);
                            }).on('end', () => {
                                db.serialize(() => {
                                    const insertBatch = (start, end) => {
                                        db.run('BEGIN TRANSACTION');
                                        const stmt = db.prepare(`INSERT INTO ${TABLE_NAME_FOR_STORING_RELAYS} (fingerprint) VALUES (?)`);
                                        end = Math.min(end, allRelays.length - 1);
                                        for (let i = start; i <= end; i++) {
                                            stmt.run(allRelays[i].fingerprint, (_) => {
                                            });
                                        }

                                        stmt.finalize((_) => {
                                            db.run('COMMIT', (err) => {
                                                if (!err) {
                                                    if ((end + 1) <= allRelays.length - 1) {
                                                        insertBatch(end + 1, end + BATCH_SIZE);
                                                    } else {
                                                        db.close(() => {
                                                            console.log(`Added ${allRelays.length} relays from ${csv_relays}`);
                                                            resolve({
                                                                allRelays: allRelays,
                                                                idToRelaysMap: idToRelaysMap
                                                            });
                                                        });

                                                    }
                                                }
                                            });
                                        });

                                    };
                                    insertBatch(0, BATCH_SIZE);
                                })
                            });
                    } else {
                        reject(e);
                    }
                });
            }
        });
    });
}

const addSuccessCircuitsToDb = async (sqlite3DbPath, csv_success_circuits_relationship, idToRelaysMap) => {
    return new Promise((resolve, reject) => {
        const allSuccessCircuits = [];
        const db = new sqlite3.Database(sqlite3DbPath, (err) => {
            if (err) {
                reject(err);
            } else {
                db.run(`CREATE TABLE IF NOT EXISTS ${TABLE_NAME_FOR_STORING_SUCCESS_CIRCUITS} (firsthop TEXT, secondhop TEXT, message TEXT, timestamp INTEGER, PRIMARY KEY (firsthop, secondhop, timestamp))`, (e) => {
                    if (!e) {
                        fs.createReadStream(csv_success_circuits_relationship)
                            .pipe(fastcsv.parse({headers: true}))
                            .on("error", (e) => {
                                reject(e);
                            })
                            .on('data', (row) => {
                                if (row) {
                                    let firsthop = idToRelaysMap[row[':START_ID']];
                                    let secondhop = idToRelaysMap[row[':END_ID']];

                                    if (firsthop && secondhop) {
                                        let circuit = new Circuit(idToRelaysMap[row[':START_ID']], idToRelaysMap[row[':END_ID']], row.message, parseInt(row.timestamp));
                                        allSuccessCircuits.push(circuit);
                                    }
                                }
                            }).on('end', () => {
                                db.serialize(() => {
                                    const insertBatch = (start, end) => {
                                        db.run('BEGIN TRANSACTION');

                                        const stmt = db.prepare(`INSERT INTO ${TABLE_NAME_FOR_STORING_SUCCESS_CIRCUITS} (firsthop, secondhop, message, timestamp) VALUES (?, ?, ?, ?)`);
                                        end = Math.min(end, allSuccessCircuits.length - 1);
                                        for (let i = start; i <= end; i++) {
                                            let circuit = allSuccessCircuits[i];
                                            stmt.run(circuit.firsthop.fingerprint, circuit.secondhop.fingerprint, circuit.message, circuit.timestamp, (_) => {});
                                        }

                                        stmt.finalize((_) => {
                                            db.run('COMMIT', (err) => {
                                                if (!err) {
                                                    if ((end + 1) <= allSuccessCircuits.length - 1) {
                                                        insertBatch(end + 1, end + BATCH_SIZE);
                                                    } else {

                                                        console.log(`Added ${allSuccessCircuits.length} success circuits from ${csv_success_circuits_relationship}`);
                                                        db.close(() => {
                                                            resolve();
                                                        })
                                                    }
                                                }
                                            });
                                        });
                                    };
                                    insertBatch(0, BATCH_SIZE);
                                })
                            });
                    } else {
                        console.log("Note: You cannot overwrite the same database");
                        reject(e);
                    }
                });
            }
        });
    });
}
const addFailedCircuitsToDb = async (sqlite3DbPath, csv_failed_circuits_relationship, idToRelaysMap) => {

    const allFailedCircuits = [];
    return new Promise((resolve, reject) => {
        const db = new sqlite3.Database(sqlite3DbPath, (err) => {
            if (err) {
                reject(err);
            } else {
                db.run(`CREATE TABLE IF NOT EXISTS ${TABLE_NAME_FOR_STORING_FAILED_CIRCUITS} (firsthop TEXT, secondhop TEXT, message TEXT, timestamp INTEGER, PRIMARY KEY (firsthop, secondhop, timestamp))`, (e) => {
                    if (!e) {
                        fs.createReadStream(csv_failed_circuits_relationship)
                            .pipe(fastcsv.parse({headers: true}))
                            .on("error", (e) => {
                                reject(e);
                            })
                            .on('data', (row) => {
                                if (row) {
                                    let firsthop = idToRelaysMap[row[':START_ID']];
                                    let secondhop = idToRelaysMap[row[':END_ID']];

                                    if (firsthop && secondhop) {
                                        let circuit = new Circuit(idToRelaysMap[row[':START_ID']], idToRelaysMap[row[':END_ID']], row.message, parseInt(row.timestamp));
                                        allFailedCircuits.push(circuit);
                                    }
                                }
                            }).on('end', () => {
                                db.serialize(() => {
                                    const insertBatch = (start, end) => {
                                        db.run('BEGIN TRANSACTION');
                                        const stmt = db.prepare(`INSERT INTO ${TABLE_NAME_FOR_STORING_FAILED_CIRCUITS} (firsthop, secondhop, message, timestamp) VALUES (?, ?, ?, ?)`); end = Math.min(end, allFailedCircuits.length - 1);
                                        for (let i = start; i <= end; i++) {
                                            let circuit = allFailedCircuits[i];
                                            stmt.run(circuit.firsthop.fingerprint, circuit.secondhop.fingerprint, circuit.message, circuit.timestamp, (_) => {});
                                        }

                                        stmt.finalize((_) => {
                                            db.run('COMMIT', (err) => {
                                                if (err) {
                                                } else {
                                                    if ((end + 1) <= allFailedCircuits.length - 1) {
                                                        insertBatch(end + 1, end + BATCH_SIZE);
                                                    } else {
                                                        console.log(`Added ${allFailedCircuits.length} failed circuits from ${csv_failed_circuits_relationship}`);
                                                        db.close(() => {
                                                            resolve();
                                                        })
                                                    }
                                                }
                                            });
                                        });
                                    };
                                    insertBatch(0, BATCH_SIZE);
                                })
                            });
                    } else {
                        console.log("Note: You cannot overwrite the same database");
                        reject(e);
                    }
                });
            }
        });
    });
}

const CSVToSqlite3Import = async (csv_relays, csv_success_circuits_relationship, csv_failed_circuits_relationship, sqlite3DbPath) => {
    let {allRelays, idToRelaysMap} = await addRelaysToDb(sqlite3DbPath, csv_relays);
    await addSuccessCircuitsToDb(sqlite3DbPath, csv_success_circuits_relationship, idToRelaysMap, allRelays);
    await addFailedCircuitsToDb(sqlite3DbPath, csv_failed_circuits_relationship, idToRelaysMap, allRelays);
}
export {
    sqlite3ToCSVExport,
    CSVToSqlite3Import
}
