const fs = require('fs');
const csv = require('csv-parser');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;
const { Worker, parentPort } = require('worker_threads');
const path = require('path');
const os = require("os");
const { pipeline } = require('stream/promises');
const stream = require('stream');
const { error } = require('console');
const { mapOne, mapTwo } = require('./docMappings.cjs')
const mie = require('@maxklema/mie-api-tools');

let MAX_WORKERS;
const processedFiles = new Set();
let configJSON;
let outputDir;

let csvBasename;
let successCsvPath;
let errorCsvPath;
let delimiter;

let file;
let mrnumber;
let pat_id;
let map;

//gather already-uploaded files
async function loadFiles(){
    return new Promise((resolve, reject) => {
        fs.createReadStream(successCsvPath)
            .pipe(csv())
            .on('data', (row) => {
                if (row){
                    //adds files already uploaded to a set (come back to)
                    processedFiles.add(getKey({
                        dataInput: csvBasename.substring(0, csvBasename.indexOf(".")), 
                        file: row[file], 
                        pat_id: row[pat_id] ? row[pat_id] : "null", 
                        mrnumber: row[mrnumber] ? row[mrnumber] : "null"
                    }));
                }
            })
            .on('end', resolve)
            .on('error', reject);
    })
}

function getKey(obj){
    return JSON.stringify(obj);
}

const setMapping = (Mapping) => {

    map = Mapping == "one" ? mapOne : mapTwo;

    for (const [key, value] of map.entries()){
        switch(value) {
            case "file":
                file = key;
                break;
            case "pat_id":
                pat_id = key;
                break;
            case "mrnumber":
                mrnumber = key;
                break;
        }
    }
}

async function* readInputRows(filename) {
    const headers = [];
    const csvParser = csv({
        mapHeaders: ({ header }) => {
            headers.push(header);
            return header;
        },
        separator: delimiter
    });

    csvParser.on('error', (err) => {
        throw error(`ERROR: there was an issue reading the headers for \'${path.join(csvFiles[j]["dirname"], csvBasename)}\'. Make sure they are fomratted correctly. ${err}`)
    })
    const stream = fs.createReadStream(filename);
    stream.pipe(csvParser)

    for await (const row of csvParser){
        yield row;
    }
}

//import multiple documents through a CSV file
async function uploadDocs(csvFiles, config){
    
    configJSON = config;

    //set number of worker threads
    MAX_WORKERS = configJSON["threads"] ? configJSON["threads"] : os.cpus().length;

    //set output directory
    outputDir = configJSON["output_dir"];
    outputDir = `${path.dirname(outputDir)}/${path.basename(outputDir)}`;
    if (!fs.existsSync(outputDir)){
        try {
            fs.mkdirSync(outputDir, { recursive: true} )
            console.log(`${'\x1b[1;32m✓\x1b[0m'} Created output directory: \'${outputDir}\'`);
        } catch (err) {
            throw error(`ERROR: Invalid output directory: \'${outputDir}\'. ${err}`); 
        }   
    }

    configJSON["csv_delimiter"] ?  delimiter = configJSON["csv_delimiter"] : delimiter = ",";
    
    //loop over each input data file
    for (let j = 0; j < Object.keys(csvFiles).length; j++){

        csvBasename = csvFiles[j]["basename"];
        let csvDirname = csvFiles[j]["dirname"];
        const docQueue = [];
        let workerPromises = [];
        const success = [];
        const errors = [];
        let totalFiles = 0;
        let skippedFiles = 0;
        
        //set the mapping
        setMapping(config["mapping"]);

        //create success and errors.csv file if one does not already exist
        const resultCsvDir = `${outputDir}/${csvBasename.substring(0, csvBasename.indexOf("."))}`        
        if (!fs.existsSync(resultCsvDir)){
            fs.mkdirSync(resultCsvDir, { recursive: true} );
            fs.writeFileSync(path.join(resultCsvDir, "errors.csv"), `${file},${pat_id},${mrnumber},status\n`, 'utf8');
            fs.writeFileSync(path.join(resultCsvDir, "success.csv"), `${file},${pat_id},${mrnumber},status\n`, 'utf8');
        }

        errorCsvPath = path.join(resultCsvDir, "errors.csv");
        successCsvPath = path.join(resultCsvDir, "success.csv");

        await loadFiles();

        for (i = 0; i < MAX_WORKERS; i++){

            const worker = new Worker(path.join(__dirname, "uploadDoc.cjs"));
            workerPromises.push({
                instance: worker,
                busy: false
            });
                
            worker.on('message', (message) => {
                if (message.success == true){
                    success.push({ file: message["row"][file], pat_id:  message["row"][pat_id] ?  message["row"][pat_id] : "null", mrnumber:  message["row"][mrnumber] ?  message["row"][mrnumber] : "null", status: 'Success'});
                } else if (message.success == false) {
                    errors.push({ file: message["row"][file], pat_id:  message["row"][pat_id] ?  message["row"][pat_id] : "null", mrnumber:  message["row"][mrnumber] ?  message["row"][mrnumber] : "null", status: 'Success'});
                } else {
                    errors.push({ file: message["row"][file], pat_id:  message["row"][pat_id] ?  message["row"][pat_id] : "null", mrnumber:  message["row"][mrnumber] ?  message["row"][mrnumber] : "null", status: 'Success'})
                }

                // Mark worker as idle
                const workerState = workerPromises.find(w => w.instance === worker);
                workerState.busy = false;

                // Assign rows if available
                if (docQueue.length > 0) {
                    const row = docQueue.shift();

                    const data = {
                        Mapping: config["mapping"],
                        Directory: csvDirname,
                        total: totalFiles,
                        uploaded: success.length + errors.length,
                        cookie: mie.Cookie.value,
                        URL: mie.URL.value,
                        practice: mie.practice.value
                    }

                    worker.postMessage({ type: 'job', row: row, data: data });
                    workerState.busy = true; // Mark worker as busy
                } else if (docQueue.length < MAX_WORKERS && skippedFiles + success.length + errors.length == totalFiles){
                    // console.log("here?");
                    workerPromises.forEach(worker => {
                        worker.instance.postMessage({type: "exit"})
                    })
                }
            });

            worker.on("exit", () => {});
        }

        for await (const row of readInputRows(path.join(csvFiles[j]["dirname"], csvBasename))){
            const key = {
                dataInput: csvBasename.substring(0, csvBasename.indexOf(".")), 
                file: row[file], 
                pat_id: row[pat_id] ? row[pat_id] : "null", 
                mrnumber: row[mrnumber] ? row[mrnumber] : "null"
            }
        
            //add files to queue that have not already been migrated
            if (!processedFiles.has(getKey(key))){
                processedFiles.add(getKey(key));
                docQueue.push(row); //push to queue for workers
            } else {
                skippedFiles += 1;
            }
            
            totalFiles += 1;
            
            const data = {
                Mapping: config["mapping"],
                Directory: csvDirname,
                total: totalFiles,
                uploaded: success.length + errors.length,
                cookie: mie.Cookie.value,
                URL: mie.URL.value,
                practice: mie.practice.value
            }

            const availableWorker = workerPromises.find(w => !w.busy);
            if (availableWorker){
                const newRow = docQueue.shift();
                if (newRow){
                    availableWorker.instance.postMessage({ type: 'job', row: newRow, data: data });
                    availableWorker.busy = true;
                }
            }
        }
        
        if (skippedFiles == totalFiles){
            workerPromises.forEach(worker => {
                worker.instance.postMessage({type: "exit"})
            })
        }

        await Promise.all(workerPromises.map(worker => new Promise(resolve => {
            worker.instance.once('exit', () => resolve());
        })))

        process.stdout.write('\x1b[2K'); //clear current line
        console.log(`\n${'\x1b[1;32m✓\x1b[0m'} ${`\x1b[1m\x1b[1;32m${`Migration job completed for ${csvBasename}`}\x1b[0m`}`);
        console.log(`${'\x1b[34m➜\x1b[0m'} ${`\x1b[1m\x1b[34mJob Details\x1b[0m`}`)
        console.log(`${'\x1b[34m➜\x1b[0m'} Files Uploaded: ${`\x1b[34m${success.length}\x1b[0m`}`)
        console.log(`${'\x1b[34m➜\x1b[0m'} Files not Uploaded (errors): ${`\x1b[34m${errors.length}\x1b[0m`}`)
        console.log(`${'\x1b[34m➜\x1b[0m'} Files Skipped (duplicates): ${`\x1b[34m${skippedFiles}\x1b[0m`}`)

        // success file CSV writer
        const successCSVWriter = createCsvWriter({
            path: successCsvPath,
            header: [
                {id: 'file', title: "filePath"},
                {id: 'pat_id', title: "patID"},
                {id: 'mrnumber', title: mrnumber},
                {id: 'status', title: 'status'}
            ],
            append: true
        });

        // Error file CSV Writer
        const errorCSVWriter = createCsvWriter({
            path: errorCsvPath,
            header: [
                {id: 'file', title: file},
                {id: 'pat_id', title: pat_id},
                {id: 'mrnumber', title: mrnumber},
                {id: 'status', title: 'status'}
            ],
            append: true
        });

        //write results to appropriate CSV file
        if (success.length != 0)
            successCSVWriter.writeRecords(success);
        
        if (errors.length != 0)
            errorCSVWriter.writeRecords(errors);  
        
        i = 0;
    }
}

module.exports = { uploadDocs };