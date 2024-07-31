const fs = require('fs');
const csv = require('csv-parser');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;
const { Worker } = require('worker_threads');
const path = require('path');
const os = require("os");
const { pipeline } = require('stream/promises');
const stream = require('stream');
const yargs = require('yargs');

const MAX_WORKERS = os.cpus().length;
const processedFiles = new Set();

let file = "filePath";
let mrnumber = "mrNumber";
let pat_id = "patID";
let mapping = "one"; //temp

//args
const argv = yargs
    .option('mapping', {
        alias: 'm',
        description: 'Provide a mapping value when uploading documents',
        type: 'string'
    })
    .help()
    .argv;

// //eventually switch
// mapping = "one";


//header names dependent on mapping
// if (mapping){
//     if (argv.mapping == "two") {
//         file = "filepath";
//         mrnumber = "mrnumber";
//         pat_id = "patid";
//     } else if (mapping != "one") {
//         log.createLog("error", "Invalid Flag");
//         throw new error.customError(error.INVALID_FLAG,  `The flag you provided for \"mapping\" is invalid.`);
//     }
// } else {
//     mapping = "one";
// }

// success file CSV writer
const successCSVWriter = createCsvWriter({
    path: './Upload Status/success.csv',
    header: [
        {id: 'file', title: 'FILE'},
        {id: 'pat_id', title: 'PAT_ID'},
        {id: 'mrnumber', title: 'MRNUMBER'},
        {id: 'status', title: 'STATUS'}
    ],
    append: true
});

// Error file CSV Writer
const errorCSVWriter = createCsvWriter({
    path: './Upload Status/errors.csv',
    header: [
        {id: 'file', title: 'FILE'},
        {id: 'pat_id', title: 'PAT_ID'},
        {id: 'mrnumber', title: 'MRNUMBER'},
        {id: 'status', title: 'STATUS'}
    ],
    append: true
});

//gather already-uploaded files
async function loadFiles(){
    if (fs.existsSync("./Upload Status/success.csv")){
        return new Promise((resolve, reject) => {
            fs.createReadStream("./Upload Status/success.csv")
                .pipe(csv())
                .on('data', (row) => {
                    if (row){
                        //adds files already uploaded to a set
                        processedFiles.add(getKey({file: row.FILE, pat_id: row.PAT_ID, mrnumber: row.MRNUMBER}));
                    }
                })
                .on('end', resolve)
                .on('error', reject);
        })
    } else {
        fs.writeFile("./Upload Status/success.csv", "FILE,PAT_ID,MRNUMBER,STATUS\n", 'utf8', () => {});
    }
}

function getKey(obj){
    return JSON.stringify(obj);
}

//import multiple documents through a CSV file
async function uploadDocs(csv_file){
    

    if (!fs.existsSync("./Upload Status")){
        fs.mkdirSync("./Upload Status", { recursive: true} )
    }
    
    //create errors.csv file if one does not already exist
    if (!fs.existsSync("./Upload Status/errors.csv")){
        fs.writeFile("./Upload Status/errors.csv", "FILE,PAT_ID,MRNUMBER,STATUS\n", 'utf8', () => {});
    }

    await loadFiles();
    const headers = [];
    const docQueue = [];
    let workerPromises = [];
    const success = [];
    const errors = [];
    const csvParser = csv({
        mapHeaders: ({ header }) => {
            headers.push(header);
            return header;
        }
    });

    //grabs all of the headers from the CSV file.
    csvParser.on('error', (err) => {
        // log.createLog("error", "Bad Request");
        // throw new error.customError(error.CSV_PARSING_ERROR,  `There was an error parsing your CSV file. Make sure it is formatted correctly. Error: ${err}`);
    })

    await pipeline(
        fs.createReadStream(csv_file),
        csvParser,
        new stream.Writable({
            objectMode: true,
            write(row, encoding, callback) {
                //if row is header, store it in an array
                if (!processedFiles.has(getKey({file: row[file], pat_id: row[pat_id] ? row[pat_id] : "null", mrnumber: row[mrnumber] ? row[mrnumber] : "null"}))){
                    processedFiles.add(getKey({file: row[file], pat_id: row[pat_id] ? row[pat_id] : "null", mrnumber: row[mrnumber] ? row[mrnumber] : "null"}));
                    docQueue.push(row); //push to queue for workers
                }
                callback();
            }
        })
    );

    for (i = 0; i < MAX_WORKERS; i++){
        const addWorker = new Promise((resolve) => {

            function newWorker(){
                const row = docQueue.shift();
                if (!row){
                    resolve();
                    return;
                }

                const worker = new Worker(path.join(__dirname, "uploadDoc.cjs"), { workerData: {row: row, URL: mie.URL.value, Cookie: mie.Cookie.value, Practice: mie.practice.value, Mapping: mapping}})
    
                worker.on('message', (message) => {
                    if (message.success == true){ 
                        log.createLog("info", `Document Upload Response:\nFilename \"${message.filename}\" was successfully uploaded: ${message.result}`);
                        success.push({ file: row[file], pat_id: row[pat_id] ? row[pat_id] : "null", mrnumber: row[mrnumber] ? row[mrnumber] : "null", status: 'Success'});
                        newWorker();
                    } else if (message.success == false) {
                        log.createLog("info", `Document Upload Response:\nFilename \"${message.filename}\" failed to upload: ${message.result}`);
                        errors.push({ file: row[file], pat_id: row[pat_id] ? row[pat_id] : "null", mrnumber: row[mrnumber] ? row[mrnumber] : "null", status: 'Failed Upload'})
                        newWorker();
                    } else {
                        log.createLog("error", "Bad Request");
                        errors.push({ file: row[file], pat_id: row[pat_id] ? row[pat_id] : "null", mrnumber: row[mrnumber] ? row[mrnumber] : "null", status: 'Failed Upload'})
                        newWorker();
                    }
                });
            }
            newWorker();
        })
        workerPromises.push(addWorker);
    }

    await Promise.all(workerPromises)
    console.log("Upload Document Job Completed.");

    //write results to appropriate CSV file
    if (success.length != 0){
        successCSVWriter.writeRecords(success);
    }
    if (errors.length != 0){
        errorCSVWriter.writeRecords(errors);  
    }

}

module.exports = { uploadDocs };