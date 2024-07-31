const mie = require('@maxklema/mie-api-tools');
const figlet = require('figlet');
const fs = require('fs');
const uploadDocs = require('./src/documentUpload.cjs');
const path = require('path');
const { error } = require('console');
const yargs = require('yargs');

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

const getInputSpecs = () => {

    //command line arg to get configuration file path
    const argv = yargs
        .option('config', {
            alias: 'c',
            description: 'Config file path with migration details',
            type: 'string'
        })
        .help()
        .argv;
    
    if (!argv.config)
        throw error(`ERROR: No configuration file found. Try \'node migrate-data.cjs -c yourConfigFile.json\' instead.`);

    const configPath = argv.config;
    
    if (fs.existsSync(configPath)){
        return JSON.parse(fs.readFileSync(configPath));
    } else {
        throw error(`ERROR: Configuration file \'${configPath}\' coud not be found.`);
    }
}

( async () => {
    let logo = figlet.textSync("MIE", {
        horizontalLayout: "default",
        verticalLayout: "default",
        width: 80,
        whitespaceBreak: true,
    })
    console.log(logo);
    
    //Read JSON Input File
    configJSON = getInputSpecs();
    
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
    uploadDocs.uploadDocs(filePaths, configJSON);

})();


