const fs = require('fs');
const axios = require('axios');
const FormData = require('form-data');
const { parentPort } = require('worker_threads');
const { mapOne, mapTwo } = require('./docMappings.cjs')
const path = require('path');

const getBar = (uploadStatus) => {
    let percentage = parseFloat(((uploadStatus["uploaded"] / uploadStatus["total"]) * 100).toFixed(2));
    const total = 20; //length of bar
    const progress = Math.round(percentage / 5);
    const emptySpace = total - progress;

    return `${'\x1b[34m█\x1b[0m'.repeat(progress)}${'\x1b[34m▒\x1b[0m'.repeat(emptySpace)} ${`\x1b[34m${`${percentage}%`}\x1b[0m`} (${uploadStatus["uploaded"]}/${uploadStatus["total"]})`
}

//this function is used for multi-threading
async function uploadSingleDocument(upload_data, URL, Cookie, Practice, Mapping, Directory, uploadStatus){

    let map;
    let filename;

    if (typeof Mapping == 'object'){
        map = new Map(Object.entries(Mapping));
    } else {
        Mapping == "one" ? map = mapOne : map = mapTwo;
    }

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

            if (!filename){
                throw Error(`ERROR: Could not find the filepath for this upload. Is your mapping correct?`);
            }

            //convert HTM and TIF file types
            if (filename.endsWith(".htm")){
                convertFile(4, ".html", 4);
            } else if (filename.endsWith(".tif") || filename.endsWith(".tiff")) {
                filename.endsWith(".tif") == true ? convertFile(4, ".png", 3) : convertFile(5, ".png", 3);
            }

            const bar = getBar(uploadStatus);
            process.stdout.write('\x1b[2K'); //clear current line
            process.stdout.write(`${'\x1b[33m➜\x1b[0m'} Upload Status: ${bar} | Uploading ${`\x1b[34m${path.join(Directory, filename)}\x1b[0m`} \r`);

            form.append(value, fs.createReadStream(path.join(Directory, filename)));

        } else {
            let headerValue;
            upload_data[key] ? headerValue = upload_data[key] : headerValue = "";
            form.append(value, headerValue);
        }
    }

    axios.post(URL, form, {
        headers: {
            'Content-Type': 'multi-part/form-data', 
            'cookie': `wc_miehr_${Practice}_session_id=${Cookie}`
        }
    })
    .then(response => {
        const result = response.headers['x-status'];
        if (result != 'success'){
            parentPort.postMessage({ success: false, row: upload_data, result: response.headers['x-status_desc'] });
        } else {
            parentPort.postMessage({ success: true, row: upload_data, result: response.headers['x-status_desc'] });
        } 
    })
    .catch((err) => {
        parentPort.postMessage({ success: 'error', row: upload_data, result: err.message});
    });
}

parentPort.on('message', async (message) => {
    if (message.type == "job"){

        const uploadStatusData = {
            "total": message["data"]["total"],
            "uploaded": message["data"]["uploaded"]
        }

        const workerData = {
            row: message["row"], 
            URL: message["data"]["URL"],
            Cookie: message["data"]["cookie"],
            Practice: message["data"]["practice"],
            Mapping: message["data"]["Mapping"],
            Directory: message["data"]["Directory"],
            uploadStatus: uploadStatusData
        }

        uploadSingleDocument(workerData["row"], workerData["URL"], workerData["Cookie"], workerData["Practice"], workerData["Mapping"], workerData["Directory"], workerData["uploadStatus"]);
    } else if (message.type == "exit"){
        parentPort.close(); //terminate worker thread
    }
});