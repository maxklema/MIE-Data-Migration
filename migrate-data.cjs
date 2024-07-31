const mie = require('@maxklema/mie-api-tools');
const figlet = require('figlet');
const fs = require('fs');
const uploadDocs = require('./src/documentUpload.cjs');
const path = require('path');
const { error } = require('console');

//constants
let inputJSON;

( async () => {
    let logo = figlet.textSync("MIE", {
        horizontalLayout: "default",
        verticalLayout: "default",
        width: 80,
        whitespaceBreak: true,
    })
    console.log(logo);
    
    //Read JSON Input File
    inputJSON = JSON.parse(fs.readFileSync("./input.json"));
    
    //Generate Session Token
    mie.username.value = inputJSON["username"];
    mie.password.value = inputJSON["password"];
    mie.practice.value = inputJSON["handle"];
    mie.URL.value = inputJSON["url"];

    await mie.createRecord("patients", { "first_name": "test"});
    console.log(`${'\x1b[1;32m✓\x1b[0m'} Received Session ID: ${mie.Cookie.value}`);

    //parsing file paths for input_data
    let filePaths = getFilePaths(inputJSON);
    console.log(`${'\x1b[1;32m✓\x1b[0m'} parsed all .CSV input file paths`);

    // pass in the CSV
    // uploadDocs.uploadDocs(filePaths);
})();

const getFilePaths = (inputJSON) => {

    let filePaths = {};

    for (let i = 0; i < inputJSON["input_data"].length; i++){
        filePaths[i] = {};
        filePaths[i]["dirname"] = path.dirname(inputJSON["input_data"][i]);
        filePaths[i]["basename"] = path.basename(inputJSON["input_data"][i]);

        //ensure each file is a CSV
        if (!(filePaths[i]["basename"].toLowerCase()).endsWith(".csv")){
            throw error(`ERROR: All input files must be of type \'.csv\'. Received \'${filePaths[i]["basename"]}\' instead`);
        }

    }
    return filePaths;
}

