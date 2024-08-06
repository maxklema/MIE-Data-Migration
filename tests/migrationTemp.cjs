const mie = require('@maxklema/mie-api-tools');
const figlet = require('figlet');
const path = require('path');
const { error } = require('console');
const uploadDocs = require('../src/documentUpload.cjs');

const migrationTemp = async (inputJSON) => {

    //constants
    let configJSON;

    const getFilePaths = (configJSON) => {

        let filePaths = {};

        for (let i = 0; i < configJSON["input_data"].length; i++){
            filePaths[i] = {};
            filePaths[i]["dirname"] = path.dirname(configJSON["input_data"][i]);
            filePaths[i]["basename"] = path.basename(configJSON["input_data"][i]);

            //ensure each file is a CSV
            if (!(filePaths[i]["basename"].toLowerCase()).endsWith(".csv")){
                throw error(`ERROR: All input files must be of type \'.csv\'. Received \'${filePaths[i]["basename"]}\' instead`);
            }

        }
        return filePaths;
    }

    ( async () => {
        let logo = figlet.textSync("MIE ", {
            horizontalLayout: "default",
            verticalLayout: "default",
            width: 80,
            whitespaceBreak: true,
        })
        console.log(logo);
        
        //Read JSON Input File
        configJSON = inputJSON;
        
        //Generate Session Token
        mie.username.value = configJSON["username"];
        mie.password.value = configJSON["password"];
        mie.practice.value = configJSON["handle"];
        mie.URL.value = configJSON["url"];

        process.stdout.write(`${'\x1b[33m➜\x1b[0m'} Getting Session ID... \r`)
        await mie.createRecord("patients", { "first_name": "test"});
        console.log(`${'\x1b[1;32m✓\x1b[0m'} Received Session ID: ${mie.Cookie.value}`);

        //parsing file paths for input_data
        let filePaths = getFilePaths(configJSON);
        console.log(`${'\x1b[1;32m✓\x1b[0m'} parsed all .csv input file paths`);

        // pass in the CSV
        await uploadDocs.uploadDocs(filePaths, configJSON);

    })();

}

module.exports = migrationTemp;


