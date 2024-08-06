const fs = require('fs');
const csv = require('csv-parser');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;
const { Worker  } = require('worker_threads');
const path = require('path');
const os = require("os");
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
let inputCSVHeaders;

//gather already-uploaded files
async function loadFiles(){
    return new Promise((resolve, reject) => {
        fs.createReadStream(successCsvPath)
            .pipe(csv())
            .on('data', (row) => {
                if (row){

                    //adds files already uploaded to a set (come back to)
                    let customKey = {dataInput: csvBasename.substring(0, csvBasename.indexOf("."))};

                    for (const header of inputCSVHeaders){
                        customKey[header] = row[header] ? row[header] : "null";
                    }
                    processedFiles.add(getKey(customKey));
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

    if (typeof Mapping == 'object' && Mapping != null){
        map = new Map(Object.entries(Mapping)); //convert object to mapping
    } else {
        map = Mapping == "one" ? mapOne : mapTwo;
    }
}

function getCSVHeaders(filePath) {
    return new Promise((resolve, reject) => {
        const headers = [];

        const stream = fs.createReadStream(filePath)
            .pipe(csv({
                separator: delimiter
            }));

        stream.once('headers', (headerList) => {
            headers.push(...headerList);
            stream.destroy();
        });

        stream.on('close', () => resolve(headers))
        stream.on('error', reject)
    })
}

async function* readInputRows(filename) {
    const csvParser = csv({
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
    MAX_WORKERS = configJSON["threads"] ? configJSON["threads"] : os.cpus().length / 2;

    //set output directory
    configJSON["output_dir"] ? outputDir = configJSON["output_dir"] : "Output/";
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
        let workers = [];
        let success = 0;
        let errors = 0;
        let totalFiles = 0;
        let skippedFiles = 0;
        processedFiles.clear(); 
        
        //get headers
        await getCSVHeaders(path.join(csvFiles[j]["dirname"], csvBasename))
        .then(headers => inputCSVHeaders = headers)
        .catch(err => console.error(err));

        //set the mapping
        setMapping(config["mapping"]);

        //create output CSV headers
        let outputHeaders = ``;
        let headers = [];

        for (const header of inputCSVHeaders){
            outputHeaders += `${header},`;
            headers.push({id: `${header}`, title: `${header}`}); 
        }

        outputHeaders += 'status\n'
        headers.push({id: 'status', title: 'status'});

        //create success and errors.csv file if one does not already exist
        const resultCsvDir = `${outputDir}/${csvBasename.substring(0, csvBasename.indexOf("."))}`        
        if (!fs.existsSync(resultCsvDir)){
            fs.mkdirSync(resultCsvDir, { recursive: true} );
            fs.writeFileSync(path.join(resultCsvDir, "success.csv"), outputHeaders, 'utf8');
        } else if (!fs.existsSync(path.join(resultCsvDir, "success.csv"))){
            fs.writeFileSync(path.join(resultCsvDir, "success.csv"), outputHeaders, 'utf8');
        }

        fs.writeFileSync(path.join(resultCsvDir, "errors.csv"), outputHeaders, 'utf8');

        errorCsvPath = path.join(resultCsvDir, "errors.csv");
        successCsvPath = path.join(resultCsvDir, "success.csv");

        await loadFiles(); //parse successful rows to avoid duplicate file uploads;

        // success file CSV writer
        const successCSVWriter = createCsvWriter({
            path: successCsvPath,
            header: headers,
            append: true
        });

        // Error file CSV Writer
        const errorCSVWriter = createCsvWriter({
            path: errorCsvPath,
            header: headers,
            append: true
        });

        for (i = 0; i < MAX_WORKERS; i++){
            const worker = new Worker(path.join(__dirname, "uploadDoc.cjs"));
            
            workers.push({
                instance: worker,
                busy: false
            });
                
            worker.on('message', (message) => {

                let dataToWrite = { status: message["result"] };

                for (const header of inputCSVHeaders){
                    dataToWrite[header] = message["row"][header] ? message["row"][header] : "null"
                }

                if (message.success == true){
                    success += 1;
                    successCSVWriter.writeRecords([dataToWrite]);
                } else if (message.success == false) {
                    errors += 1;
                    errorCSVWriter.writeRecords([dataToWrite]);
                } else {
                    errors += 1;
                    errorCSVWriter.writeRecords([dataToWrite]);
                }

                // Mark worker as idle
                worker.busy = false;

                // Assign rows if available
                if (docQueue.length > 0) {
                    const row = docQueue.shift();

                    const data = {
                        Mapping: config["mapping"],
                        Directory: csvDirname,
                        total: totalFiles,
                        uploaded: success + errors,
                        cookie: mie.Cookie.value,
                        URL: mie.URL.value,
                        practice: mie.practice.value
                    }

                    worker.postMessage({ type: 'job', row: row, data: data });
                    worker.busy = true; // Mark worker as busy
                } else if (skippedFiles + success + errors == totalFiles){
                    workers.forEach(worker => {
                        worker.instance.postMessage({type: "exit"})
                    })
                }
            });

            worker.on("exit", () => {});
        }

        for await (const row of readInputRows(path.join(csvFiles[j]["dirname"], csvBasename))){
            
            //adds files already uploaded to a set (come back to)
            let customKey = {dataInput: csvBasename.substring(0, csvBasename.indexOf("."))};

            for (const header of inputCSVHeaders){
                customKey[header] = row[header] ? row[header] : "null";
            }
        
            //add files to queue that have not already been migrated
            if (!processedFiles.has(getKey(customKey))){
                processedFiles.add(getKey(customKey));
                docQueue.push(row); //push to queue for workers
            } else {
                skippedFiles += 1;
            }
            
            totalFiles += 1;

            const availableWorker = workers.find(w => !w.busy);
            if (availableWorker){
                const newRow = docQueue.shift();
                if (newRow){
                    
                    const data = {
                        Mapping: config["mapping"],
                        Directory: csvDirname,
                        total: totalFiles,
                        uploaded: success + errors,
                        cookie: mie.Cookie.value,
                        URL: mie.URL.value,
                        practice: mie.practice.value
                    }

                    availableWorker.instance.postMessage({ type: 'job', row: newRow, data: data });
                    availableWorker.busy = true;
                }
            }
        }
    
        //terminate worker threads if all files are duplicates
        if (skippedFiles + success + errors == totalFiles){
            workers.forEach(worker => {
                worker.instance.postMessage({type: "exit"})
            })
        }

        const workerPromises = workers.map(worker => new Promise(resolve => {
            worker.instance.once('exit', resolve);
            worker.instance.once('error', err => {
                console.error("Worker error: " + err);
                resolve();
            });
        }));

        await Promise.all(workerPromises);

        process.stdout.write('\x1b[2K'); //clear current line
        console.log(`\n${'\x1b[1;32m✓\x1b[0m'} ${`\x1b[1m\x1b[1;32m${`Migration job completed for ${csvBasename}`}\x1b[0m`}`);
        console.log(`${'\x1b[34m➜\x1b[0m'} ${`\x1b[1m\x1b[34mJob Details\x1b[0m`}`)
        console.log(`${'\x1b[34m➜\x1b[0m'} Files Uploaded: ${`\x1b[34m${success}\x1b[0m`}`)
        console.log(`${'\x1b[34m➜\x1b[0m'} Files not Uploaded (errors): ${`\x1b[34m${errors}\x1b[0m`}`)
        console.log(`${'\x1b[34m➜\x1b[0m'} Files Skipped (duplicates): ${`\x1b[34m${skippedFiles}\x1b[0m`}`)
        
        i = 0;
    }
}

module.exports = { uploadDocs };