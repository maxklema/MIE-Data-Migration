const mie = require('@maxklema/mie-api-tools');
const fs = require('fs');
const csv = require('csv-parser');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;
const { Worker } = require('worker_threads');
const path = require('path');
const os = require("os");
const { pipeline } = require('stream/promises');
const stream = require('stream');
const { error } = require('console');
const { mapOne, mapTwo } = require('./docMappings.cjs')

let MAX_WORKERS;
const processedFiles = new Set();
let configJSON;
let outputDir;

let csvBasename;
let successCsvPath;
let errorCsvPath;

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
    
    //loop over each input data file
    for (let j = 0; j < Object.keys(csvFiles).length; j++){

        csvBasename = csvFiles[j]["basename"];
        let csvDirname = csvFiles[j]["dirname"];
        const headers = [];
        const docQueue = [];
        let workerPromises = [];
        const success = [];
        const errors = [];
        let totalFiles = 0;
        
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
        const csvParser = csv({
            mapHeaders: ({ header }) => {
                headers.push(header);
                return header;
            }
        });

        //grabs all of the headers from the CSV file.
        csvParser.on('error', (err) => {
            throw error(`ERROR: there was an issue reading the headers for \'${path.join(csvFiles[j]["dirname"], csvBasename)}\'. Make sure they are fomratted correctly. ${err}`)
        })

        //creates a unique key for each row and pushes rows to queue.
        await pipeline(
            fs.createReadStream(path.join(csvFiles[j]["dirname"], csvBasename)),
            csvParser,
            new stream.Writable({
                objectMode: true,
                write(row, encoding, callback) {
                    const key = {
                        dataInput: csvBasename.substring(0, csvBasename.indexOf(".")), 
                        file: row[file], 
                        pat_id: row[pat_id] ? row[pat_id] : "null", 
                        mrnumber: row[mrnumber] ? row[mrnumber] : "null"
                    }

                    //add files to queue that have not already been migrated
                    // if (!processedFiles.has(getKey(key))){
                    processedFiles.add(getKey(key));
                    docQueue.push(row); //push to queue for workers
                    // }
                    
                    totalFiles += 1;

                    callback(); //repeat for each row of data
                }
            })
        );
            // console.log(totalFiles);

        //create x new workers 
        for (i = 0; i < MAX_WORKERS; i++){
            const addWorker = new Promise((resolve) => {
    
                function newWorker(){
                    const row = docQueue.shift();
                    console.log(docQueue.length);
                    if (!row){
                        resolve();
                        return;
                    }
    
                    const uploadStatusData = {
                        "total": totalFiles,
                        "uploaded": success.length + errors.length
                    }
                    
                    const workerData = {
                        workerData: {
                            row: row, 
                            URL: mie.URL.value, 
                            Cookie: mie.Cookie.value, 
                            Practice: mie.practice.value, 
                            Mapping: config["mapping"], 
                            Directory: csvDirname,
                            uploadStatus: uploadStatusData
                        }
                    }
                    const worker = new Worker(path.join(__dirname, "uploadDoc.cjs"), workerData);
        
                    worker.on('message', (message) => {
                        if (message.success == true){
                            success.push({ file: row[file], pat_id: row[pat_id] ? row[pat_id] : "null", mrnumber: row[mrnumber] ? row[mrnumber] : "null", status: 'Success'});
                            newWorker();
                        } else if (message.success == false) {
                            errors.push({ file: row[file], pat_id: row[pat_id] ? row[pat_id] : "null", mrnumber: row[mrnumber] ? row[mrnumber] : "null", status: 'Failed Upload'})
                            newWorker();
                        } else {
                            errors.push({ file: row[file], pat_id: row[pat_id] ? row[pat_id] : "null", mrnumber: row[mrnumber] ? row[mrnumber] : "null", status: 'Failed Upload'})
                            newWorker();
                        }
                    });
                }
                newWorker();
            })
            workerPromises.push(addWorker);
        }

        //wait for all the workers to finish migrating
        await Promise.all(workerPromises)

        process.stdout.write('\x1b[2K'); //clear current line
        console.log(`${'\x1b[1;32m✓\x1b[0m'} Migration job completed for ${csvBasename}`);

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