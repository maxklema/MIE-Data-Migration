const fs = require('fs');
const axios = require('axios');
const FormData = require('form-data');
const { workerData, parentPort } = require('worker_threads');
const { mapOne, mapTwo } = require('./docMappings.cjs')
const path = require('path');

//this function is used for multi-threading
async function uploadSingleDocument(upload_data, URL, Cookie, Practice, Mapping){

    let map;
    let filename;

    Mapping == "one" ? map = mapOne : map = mapTwo;

    function convertFile(extension_length, new_extension, new_storage){
        
        fs.rename(filename, (filename.slice(0, filename.length - extension_length) + new_extension), (err) => {
            if (err) {
                console.error(err);
            }
        })
        storageType = new_storage;
        filename = filename.slice(0, filename.length - extension_length) + new_extension
    }

    const form = new FormData();
    form.append('f', 'chart');
    form.append('s', 'upload');

    //iterate over each key
    for (const [key, value] of map.entries()){
        if (value == "file"){

            filename = upload_data[key];

            //convert HTM and TIF file types
            if (filename.endsWith(".htm")){
                convertFile(4, ".html", 4);
            } else if (filename.endsWith(".tif") || filename.endsWith(".tiff")) {
                filename.endsWith(".tif") == true ? convertFile(4, ".png", 3) : convertFile(5, ".png", 3);
            }

            process.stdout.write(`${'\x1b[33m➜\x1b[0m'} Uploading ${path.join('./Data', filename)}\r`);
            form.append(value, fs.createReadStream(path.join('./Data', filename)));

        } else {
            let headerValue;
            upload_data[key] ? headerValue = upload_data[key] : headerValue = "";
            form.append(value, headerValue);
        }
    }

    // log.createLog("info", `Document Upload Request:\nDocument Type: \"${upload_data['doc_type']}\"\nStorage Type: \"${upload_data['storage_type']}\"\n Patient ID: ${upload_data['pat_id']}`);
    axios.post(URL, form, {
        headers: {
            'Content-Type': 'multi-part/form-data', 
            'cookie': `wc_miehr_${Practice}_session_id=${Cookie}`
        }
    })
    .then(response => {
        const result = response.headers['x-status'];
        if (result != 'success'){
            parentPort.postMessage({ success: false, filename: filename, result: response.headers['x-status_desc'] });
        } else {
            parentPort.postMessage({ success: true, filename: filename, result: response.headers['x-status_desc'] });
        } 
    })
    .catch((err) => {
        parentPort.postMessage({ success: 'error', filename: filename, result: err.message});
    });

}

uploadSingleDocument(workerData['row'], workerData['URL'], workerData['Cookie'], workerData['Practice'], workerData['Mapping']);