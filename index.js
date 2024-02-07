const {Storage} = require("@google-cloud/storage");
const {BigQuery} = require('@google-cloud/bigquery');
const csv = require("csv-parser");
const { write } = require("fs");

const bq = new BigQuery();
const datasetId = 'weather_etl'
const tableId = 'weather_test';

exports.readObservation = (file, context) => {
    const gcs = new Storage();
    const dataFile = gcs.bucket(file.bucket).file(file.name);

    dataFile.createReadStream()
    .on('error', () => {
        //Basic Error Handling
        console.error(error);
    })
    .pipe(csv())
    .on('data', (row) => {
        // Log row data
        row['station'] = file.name.split('.')[0]
        normalizeData(row)
    })
    .on('end', () => {
        //Handle end of file
        console.log(`End!`)
    })
}

//HELPER FUNCTIONS
function normalizeData(row) {
    for (let key in row){
        //console.log(key + ' : ' + row[key]);
        if (row[key] == -9999 || row[key] == "-9999"){
            row[key] = undefined;
        }
        if (key == "airtemp" || key == "dewpoint" || key == "pressure" || key == "windspeed" || key == "precip1hour" || key == "precip6hour"){
            row[key] = row[key]/10;
        }
    }
    writeToBq(row)
}

async function writeToBq(obj) {
    //BQ expects an array of objects, but this function only receives 1
    var rows = []; //Empty Array
    rows.push(obj);

    // Insert the array of objects into the 'demo' table
    await bq
    .dataset(datasetId)
    .table(tableId)
    .insert(rows)
    .then( () => {
        rows.forEach ( (row) => { console.log(`Inserted data for station: ${obj.station}`) } )
    } )
    .catch( (err) => { 
        console.error(`ERROR: ${err}`) 
    } )
}