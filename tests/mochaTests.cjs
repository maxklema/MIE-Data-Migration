const assert = require('assert');
const fs = require('fs');
const path = require('path');
const migrationTemp = require('./migrationTemp.cjs');

describe('Data Migration Testing', async function() {

    afterEach(function() {
        const outputDir = path.join(__dirname, "Output");
        fs.rmSync(outputDir, { recursive: true, force: true});
    });

    it("Testing upload with mapping one - duplicates checked", async () => {

        let inputJSON = {
            "username": process.env.USERNAME,
            "password": process.env.PASSWORD,
            "handle": process.env.HANDLE,
            "url": process.env.URL,
            "mapping": "one",
            "input_data": ["./tests/data/uploadOne.csv"],
            "output_dir":  "./tests/Output/",
            "threads": 2
        }

        await migrationTemp(inputJSON);

        function delay(ms) {
            return new Promise(resolve => setTimeout(resolve, ms));
        }

        await delay(3000);

        //get success.csv file
        const fPath = path.join(__dirname, "Output/uploadOne/success.csv");
        const fileContent = fs.readFileSync(fPath, "utf8");
        const lines = fileContent.split("\n");
        let success = [lines[0]];

        for (let i = 1; i < 5; i++){
            success.push(lines[i].substring(0, lines[i].indexOf(",Document")));
        }

        let expected = ["filePath,patID,docType,mrNumber,status", "More Files/110.txt,null,US,10033", "More Files/29.jpg,null,WCPHOTO,10033", "More Files/Hart_109.txt,null,US,10033", "More Files/Doe.html,null,ORDIM,10033"] 
        assert.deepEqual(success.sort(), expected.sort());

        //Make sure duplicate files are not uploaded.
        await migrationTemp(inputJSON);
        await delay(3000);
        assert.deepEqual(success.sort(), expected.sort());

    }).timeout(10000000);

    it("Testing upload with mapping two - duplicates checked", async () => {

        let inputJSON = {
            "username": process.env.USERNAME,
            "password": process.env.PASSWORD,
            "handle": process.env.HANDLE,
            "url": process.env.URL,
            "mapping": "two",
            "input_data": ["./tests/data/uploadThree.csv"],
            "output_dir":  "./tests/Output/",
            "threads": 4
        }

        await migrationTemp(inputJSON);

        function delay(ms) {
            return new Promise(resolve => setTimeout(resolve, ms));
        }

        await delay(3000);

        //get success.csv file
        const fPath = path.join(__dirname, "Output/uploadThree/success.csv");
        const fileContent = fs.readFileSync(fPath, "utf8");
        const lines = fileContent.split("\n");
        let success = [lines[0]];

        for (let i = 1; i < 5; i++){
            success.push(lines[i].substring(0, lines[i].indexOf(",Document")));
        }

        let expected = ["filepath,patid,doctype,mrnumber,status", "More Files/110.txt,null,US,10033", "More Files/29.jpg,null,WCPHOTO,10033", "More Files/Hart_109.txt,null,US,10033", "More Files/Doe.html,null,ORDIM,10033"] 
        assert.deepEqual(success.sort(), expected.sort());

        //Make sure duplicate files are not uploaded.
        await migrationTemp(inputJSON);
        await delay(3000);
        assert.deepEqual(success.sort(), expected.sort());

    }).timeout(10000000);

    it("Testing upload with custom mapping - duplicates checked", async () => {

        let inputJSON = {
            "username": process.env.USERNAME,
            "password": process.env.PASSWORD,
            "handle": process.env.HANDLE,
            "url": process.env.URL,
            "mapping": {
                "FILEPATH": "file",
                "DOCTYPE": "doc_type",
                "MRNUMBER": "mrnumber",
                "PATID": "pat_id"
            },
            "input_data": ["./tests/data/uploadTwo.csv"],
            "output_dir":  "./tests/Output/",
            "threads": 8,
            "csv_delimiter": "|"
        }

        await migrationTemp(inputJSON);

        function delay(ms) {
            return new Promise(resolve => setTimeout(resolve, ms));
        }

        await delay(3000);

        //get success.csv file
        const fPath = path.join(__dirname, "Output/uploadTwo/success.csv");
        const fileContent = fs.readFileSync(fPath, "utf8");
        const lines = fileContent.split("\n");
        let success = [lines[0]];

        for (let i = 1; i < 3; i++){
            success.push(lines[i].substring(0, lines[i].indexOf(",Document")));
        }

        let expected = [
            "AccountNo,MRNUMBER,PatLName,PatFName,DOB,PatID,CustomName,FileName,DOCTYPE,ecwCategory,Description,FILEPATH,ScanDate,ScannedBy,EncID,status", 
            "42189,10019,Wiley,Damien,03/28/1982,78572,78572_892256917_InsuranceCardSideA.jpg,78572_892256917_InsuranceCardSideA.jpg,WCPHOTO,Patient Documents,Image document for patient: 78572,Files/29.jpg,2013-04-02 00:00:00.0,82280,0",
            "42189,10019,Wiley,Damien,03/28/1982,78572,<filename>,<filename>,ORDIM,Patient Documents,Image document for patient: 78572,More Files/Doe.html,2013-04-02 00:00:00.0,82280,0", 
        ] 
        assert.deepEqual(success.sort(), expected.sort());

        //Make sure duplicate files are not uploaded.
        await migrationTemp(inputJSON);
        await delay(3000);
        assert.deepEqual(success.sort(), expected.sort());

    }).timeout(10000000);

    it("Testing upload with two input CSV files - duplicates checked", async () => {

        function delay(ms) {
            return new Promise(resolve => setTimeout(resolve, ms));
        }

        let inputJSON = {
            "username": process.env.USERNAME,
            "password": process.env.PASSWORD,
            "handle": process.env.HANDLE,
            "url": process.env.URL,
            "mapping": "one",
            "input_data": ["./tests/data/uploadOne.csv", "./tests/data/uploadFour.csv"],
            "output_dir":  "./tests/Output/",
            "threads": 2
        }

        await migrationTemp(inputJSON);
        await delay(6000);

        //get success.csv file
        let fPath = path.join(__dirname, "Output/uploadOne/success.csv");
        let fileContent = fs.readFileSync(fPath, "utf8");
        let lines = fileContent.split("\n");
        let success = [lines[0]];

        for (let i = 1; i < 5; i++){
            success.push(lines[i].substring(0, lines[i].indexOf(",Document")));
        }

        let expected = ["filePath,patID,docType,mrNumber,status", "More Files/110.txt,null,US,10033", "More Files/29.jpg,null,WCPHOTO,10033", "More Files/Hart_109.txt,null,US,10033", "More Files/Doe.html,null,ORDIM,10033"] 
        assert.deepEqual(success.sort(), expected.sort());

        //Make sure duplicate files are not uploaded.
        await migrationTemp(inputJSON);
        await delay(3000);
        assert.deepEqual(success.sort(), expected.sort());

    }).timeout(10000000);

    it("Testing upload with custom mapping - 1 success 1 error - duplicates checked", async () => {

        let inputJSON = {
            "username": process.env.USERNAME,
            "password": process.env.PASSWORD,
            "handle": process.env.HANDLE,
            "url": process.env.URL,
            "mapping": {
                "FILEPATH": "file",
                "DOCTYPE": "doc_type",
                "MRNUMBER": "mrnumber",
                "PATID": "pat_id"
            },
            "input_data": ["./tests/data/uploadFive.csv"],
            "output_dir":  "./tests/Output/",
            "threads": 8,
            "csv_delimiter": "|"
        }

        await migrationTemp(inputJSON);

        function delay(ms) {
            return new Promise(resolve => setTimeout(resolve, ms));
        }

        await delay(3000);

        //get success.csv file
        const fPath = path.join(__dirname, "Output/uploadFive/success.csv");
        const fileContent = fs.readFileSync(fPath, "utf8");
        const lines = fileContent.split("\n");
        let success = [lines[0]];

        for (let i = 1; i < 2; i++){
            success.push(lines[i].substring(0, lines[i].indexOf(",Document")));
        }

        let expected = [
            "AccountNo,MRNUMBER,PatLName,PatFName,DOB,PatID,CustomName,FileName,DOCTYPE,ecwCategory,Description,FILEPATH,ScanDate,ScannedBy,EncID,status", 
            "42189,10019,Wiley,Damien,03/28/1982,78572,78572_892256917_InsuranceCardSideA.jpg,78572_892256917_InsuranceCardSideA.jpg,WCPHOTO,Patient Documents,Image document for patient: 78572,Files/29.jpg,2013-04-02 00:00:00.0,82280,0"
        ]

        assert.deepEqual(success.sort(), expected.sort());

        //get errors.csv
        const e_fPath = path.join(__dirname, "Output/uploadFive/success.csv");
        const e_fileContent = fs.readFileSync(e_fPath, "utf8");
        const e_lines = e_fileContent.split("\n");
        let errors = [e_lines[0]];

        for (i = 1; i < 2; i++){
            errors.push(e_lines[i].substring(0, lines[i].indexOf(",Document")));
        }

        expected = [
            "AccountNo,MRNUMBER,PatLName,PatFName,DOB,PatID,CustomName,FileName,DOCTYPE,ecwCategory,Description,FILEPATH,ScanDate,ScannedBy,EncID,status", 
            "42189,10019,Wiley,Damien,03/28/1982,78572,78572_892256917_InsuranceCardSideA.jpg,78572_892256917_InsuranceCardSideA.jpg,WCPHOTO,Patient Documents,Image document for patient: 78572,Files/29.jpg,2013-04-02 00:00:00.0,82280,0"
        ]
        
        assert.deepEqual(errors.sort(), expected.sort());

    }).timeout(10000000);

});