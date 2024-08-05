# MIE Data Migration

MIE Data Migration is a tool for migrating large volumes of labs and documents from EHRs to WebChart. To see a full list of features, see the [table of contents](#Table-of-Contents) below. To demo this yourself, see the [installation](#Installation) guide.

## Table of Contents

- [Installation](#Installation)
- [Set Up](#Set-Up)
  - [Configuration](#Configuration)
- [Mappings](#Mappings)
  - [Custom Mappings](#Custom-Mappings)
- [Outputs](#Outputs)
  - [Success](#Success)
  - [Errors](#Errors)

## Installation

Download or clone this repository.

```bash
git clone https://github.com/maxklema/mie-data-migration.git
```

Make sure you have Node.js and npm installed. Then, in the root directory of the repository, run:

```
npm ci
```

## Set Up

To execute this program, you will need to pass in a JSON file with all of your configurations.

Then, execute the program like this:

```
node migrate-data.cjs -c yourconfigfile.json
```

### Configuration

Your configuration.json should include everything needed to make a data migration, including your WebChart credentials, output directory, mapping, threads, etc.

The following table describes each available property.

| Property        | Required? | Type            | Description                                                                                                                                                                                                                                   |
| --------------- | --------- | --------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `username`      | Required  | String          | Your login username for your WebChart system.                                                                                                                                                                                                 |
| `password`      | Required  | String          | The login password for your WebChart system.                                                                                                                                                                                                  |
| `handle`        | Required  | String          | The name of your WebChart system.                                                                                                                                                                                                             |
| `url`           | Required  | String          | The URL of your WebChart. It should look something similar to `"https://practice.webchartnow.com/webchart.cgi"`.                                                                                                                              |
| `input_data`    | Required  | Array Object    | A list of filepaths to your input CSV file or files (you can have multiple data migrations).                                                                                                                                                  |
| `output_dir`    | Optional  | String          | The root output directory where you want the results of your migrations to be stored. The default directory is `Output/`.                                                                                                                     |
| `mapping`       | Required  | String / Object | The mapping translation for your input CSV headers. As of 1.0.0, pre-templated mappings `one` and `two` are available. To see these mappings, view [mappings](#Mappings). To create custom mappings, see [custom mappings](#Custom-Mappings). |
| `threads`       | Optional  | Integer         | The number of worker threads you want working at once. The default is your number of CPU cores.                                                                                                                                               |
| `csv_delimiter` | Variable  | String          | The character that separates data in your CSV files. The default delimiter is `,`. If you have a different delimiter, you need to include this header.                                                                                        |

## Mappings

Mappings translate your CSV headers to those that WebChart understands. This is necessary since different EHRs may have different headers and capitalizations.

For a detailed description of each header, see the 'Documents' API documentation on your WebChart.

| Mapping One [`one`] | Mapping One [`two`] | Translation         |
| ------------------- | ------------------- | ------------------- |
| filePath            | filepath            | file                |
| mrNumber            | mrnumber            | mrnumber            |
| docID               | docid               | doc_id              |
| revisionNumber      | revisionnumber      | revision_number     |
| userID              | userid              | user_id             |
| originID            | originid            | origin_id           |
| patID               | patid               | pat_id              |
| docType             | doctype             | doc_type            |
| storageType         | storagetype         | storage_type        |
| storageID           | storageid           | storage_id          |
| serviceLocation     | servicelocation     | service_location    |
| originDate          | origindate          | origin_date         |
| enterDate           | enterdate           | enter_date          |
| revisionDater       | revisiondate        | revision_date       |
| serviceDate         | servicedate         | service_date        |
| approxServiceDate   | approxservicedate   | approx_service_date |
| interface           | interface           | interface           |
| inpatient           | inpatient           | inpatient           |
| allowForTeaching    | allowforteaching    | allow_for_teaching  |
| subject             | subject             | subject             |

> **NOTE:** Some EHRs may export data with headers not included in this list. In that case, you do not have to remove them from the CSV import file. They will be ignored by the program.

### Custom Mappings

Custom mappings can also be created. In your config.json file, instead of assigning `"mapping"` to a string, assign it to an object (which will be converted to a map).

As an example, for an input CSV file with the headers `FILE_PATH`, `PAT_ID`, `MRNUMBER`, and `DOC_TYPE`, a custom mapping can be created like this:

_Config.json_

```JSON
"mapping": {
    "FILE_PATH": "file",
    "DOC_TYPE": "doc_type",
    "MRNUMBER": "mrnumber",
    "PAT_ID": "pat_id"
},
```

> **NOTE:** The keys can be anything you want, but ensure that the value is a valid header. To see a list of valid headers, see the 'translation' column in [mappings](#Mappings).

## Outputs

For each input CSV file, a folder will be created with the name of that file for the job output. Inside, there will be a success.csv and an errors.csv file.

### Success

The headers in the sucess.csv file will be the same headers as the input CSV file.

If you re-run a migration job, **do not delete your success.csv file**. This file is used to keep track of files that have already been uploaded and will avoid uploading duplicate files in future jobs.

### Errors

The headers in the errors.csv file will be the same headers as the input CSV file.

A new errors.csv file is generated for each migration job, so duplicate error messages will not appear.

## Author

- [@maxklema](https://www.github.com/maxklema) ~ PRs are welcomed and encouraged.
