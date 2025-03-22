//const {Command} = require('commander');
import {Command} from 'commander';
import dotenv from 'dotenv';
import {neo4jToCSVExport, CSVToNeo4jImport} from './neo4j/neo4j.mjs';
import {sqlite3ToCSVExport, CSVToSqlite3Import} from './sqlite/sqlite3.mjs';
//const dotenv = require("dotenv");
//const neo4j = require('./neo4j/neo4j.mjs');
//const sqlite3 = require('./sqlite/sqlite3');

dotenv.config();

const program = new Command();

program
    .command('export')
    .description('Export to CSV')
    .option("--neo4j", "Export from neo4j to CSV")
    .option("--sqlite3 <value>", "Export from sqlite3 to CSV")
    .action((cmd) => {
        if (cmd.neo4j && cmd.sqlite3) {
            console.log("You can't export from both neo4j and sqlite3 at the same time")
        } else if (cmd.neo4j) {
            neo4jToCSVExport();
        } else if (cmd.sqlite3) {
            sqlite3ToCSVExport(cmd.sqlite3)
        } else {
            console.log("Please choose an option, either --neo4j or --sqlite3, for exporting to CSV");
        }
    });

program
    .command('import')
    .description('Import CSV files into sqlite3 or neo4j database')
    .option('--sqlite3 <value>')
    .option('--neo4j')
    .option('--relays <value>', 'Process nodes')
    .option('--success_circuits_relationship <value>', 'Success Circuits relationship CSV file')
    .option('--failed_circuits_relationship <value>', 'Failed Circuits relationship CSV file')
    .action((cmd) => {
        if (cmd.relays && cmd.success_circuits_relationship && cmd.failed_circuits_relationship) {
            if (cmd.sqlite3) {
                (async () => {
                    await CSVToSqlite3Import(cmd.relays, cmd.success_circuits_relationship, cmd.failed_circuits_relationship, cmd.sqlite3);
                })();

            } else if (cmd.neo4j) {
                CSVToNeo4jImport(cmd.relays, cmd.success_circuits_relationship, cmd.failed_circuits_relationship);
            } else {
                console.log("Please provide what you want to import this CSV files to : --sqlite3 or --neo4j");
            }
        } else {
            console.log("Please provide following args: --relays xyz.csv --success_circuits_relationship abc.csv --failed_circuits_relationship mno.csv");
        }
    });

program.parse(process.argv);


