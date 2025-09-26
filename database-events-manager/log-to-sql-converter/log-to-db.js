const sqlite3 = require('sqlite3').verbose()
const fs = require('fs')
const mqtt = require('mqtt')
let timeseriesDB
let dbConnected = false
const dbLocation = `./database-events-manager/timeseries.db`

/***********************************
 * Table per Message
 * MSG1 - Date, Time, Source, Dest, Voltage, Current, SoC
 * MSG2 -  Date, Time, Source, Dest, Charge Limit, Dischathe Limit, Charge Voltage Target, Discharge Voltage Target,  
 * MSG3 -  Date, Time, Source, Dest, Charge Power, Discharge Power, BMS Status, SoP
 * MSG4 -  Date, Time, Source, Dest, Max Cell Voltage, Min Cell Voltage, Max Cell Temperature, Min Cell Temperature 
 */

function createDB(dbLocation) {
    //Initial database connection
    if (!fs.existsSync(dbLocation)) {
        timeseriesDB = new sqlite3.Database(dbLocation, (err) => {
            if (err) {
                console.error('Error connecting to database:', err.message)
            } else {
                dbConnected = true

                //MSG1 table
                timeseriesDB.exec(`
                CREATE TABLE messageOne(
                    date TEXT NOT NULL,
                    time TEXT NOT NULL,
                    invAddr TEXT NOT NULL,
                    bmsAddr TEXT NOT NULL,
                    voltage TEXT,
                    current TEXT,
                    soc TEXT,
                    soh TEXT

                ) STRICT
            `);

                //MSG2 table
                timeseriesDB.exec(`
                CREATE TABLE messageTwo(
                    date TEXT NOT NULL,
                    time TEXT NOT NULL,
                    invAddr TEXT NOT NULL,
                    bmsAddr TEXT NOT NULL,
                    chargeLimit TEXT,
                    dischargeCurrentLimit TEXT,
                    chargeVoltage TEXT,
                    dischargeVoltage TEXT

                ) STRICT
            `);

                //MSG3 table
                timeseriesDB.exec(`
                CREATE TABLE messageThree(
                    date TEXT NOT NULL,
                    time TEXT NOT NULL,
                    invAddr TEXT NOT NULL,
                    bmsAddr TEXT NOT NULL,
                    chargingPowAvl TEXT,
                    dischargingPowAvl TEXT,
                    bmsStatus TEXT,
                    sop TEXT

                ) STRICT
            `);

                //MSG4 table
                timeseriesDB.exec(`
                CREATE TABLE messageFour(
                    date TEXT NOT NULL,
                    time TEXT NOT NULL,
                    invAddr TEXT NOT NULL,
                    bmsAddr TEXT NOT NULL,
                    maxCellVoltage TEXT,
                    minCellVoltage TEXT,
                    maxCellTemperature TEXT,
                    minCellTemperature TEXT

                ) STRICT
            `);
            }
        })
    }
}

async function enterDBData(msgData) {

    return new Promise(async (resolve, reject) => {
        try {
        
            if (dbConnected) {
                const msg1 = timeseriesDB.prepare('INSERT INTO messageOne (date, time, invAddr, bmsAddr, voltage, current, soc, soh) VALUES (?, ?, ?, ?, ?, ?, ?, ?)')
                const msg2 = timeseriesDB.prepare('INSERT INTO messageTwo (date, time, invAddr, bmsAddr, chargeLimit, dischargeCurrentLimit, chargeVoltage, dischargeVoltage) VALUES (?, ?, ?, ?, ?, ?, ?, ?)')
                const msg3 = timeseriesDB.prepare('INSERT INTO messageThree (date, time, invAddr, bmsAddr, chargingPowAvl, dischargingPowAvl, bmsStatus, sop) VALUES (?, ?, ?, ?, ?, ?, ?, ?)')
                const msg4 = timeseriesDB.prepare('INSERT INTO messageFour (date, time, invAddr, bmsAddr, maxCellVoltage, minCellVoltage, maxCellTemperature, minCellTemperature) VALUES (?, ?, ?, ?, ?, ?, ?, ?)')

                const date = msgData.date
                const time = msgData.time

                for (let i = 0; i < msgData.messages.length; i++) {

                    const message = msgData.messages[i]

                    switch (message.type) {
                        case '1':
                            msg1.run(date, time, message.invAddr, message.bmsAddr, message.voltage, message.current, message.soc, message.soh)
                            break
                        case '2':
                            msg2.run(date, time, message.invAddr, message.bmsAddr, message.chargeLimit, message.dischargeCurrentLimit, message.chargeVoltage, message.dischargeVoltage)
                            break
                        case '3':
                            msg3.run(date, time, message.invAddr, message.bmsAddr, message.chargingPowAvl, message.dischargingPowAvl, message.bmsStatus, message.sop)
                            break
                        case '4':
                            msg4.run(date, time, message.invAddr, message.bmsAddr, message.maxCellVoltage, message.minCellVoltage, message.maxCellTemperature, message.minCellTemperature)
                            break
                    }
                }


                msg1.finalize()
                msg2.finalize()
                msg3.finalize()
                msg4.finalize()

                resolve(true)
            }     
        } catch (error) {
            //failed to write
            reject(false)
        }
    })
}

async function dbConnect(dbLocation) {
    //Connect to DB
    try {
        let dbConnectPromise = new Promise((resolve, reject) => {

            timeseriesDB = new sqlite3.Database(dbLocation, (err) => {
                if (err) {
                    reject(false)
                }
                else {
                    resolve(true)
                }

            })

        })

        dbConnected = await dbConnectPromise
    } catch {
        //DB Connection error
    }
}

async function dbDisconnect() {
    if (dbConnected) {
        //Disconnect from DB
        try {
            let dbDisconnectPromise = new Promise((resolve, reject) => {

                timeseriesDB.close((err) => {
                    if (err) {
                        //Log error
                        reject(true)
                    } else {
                        resolve(false)
                    }
                })

            })

            dbConnected = await dbDisconnectPromise
        } catch {

        }
    }
}

async function getDBData() {
    await dbConnect(dbLocation)
    if (dbConnected) {
        const tableData = eventsDB.prepare('SELECT * FROM events');
        const rows = tableData.all(); // Fetches all rows synchronously
        console.dir(rows)

        eventsDB.all('SELECT * FROM events', [], (err, rows) => {
            if (err) {
                console.log("couldn't get table data")
                return
            }
            console.dir(rows)
        })
    }
    dbDisconnect()
}

function start() {
    createDB(dbLocation)
    
    readFile()
}

async function readFile() {
    let dateStartIndex = 0

    const filePath = 'C:/Users/nkosi/Documents/1. Gateways/14. Client issues/0. EURO Steel/canFrameLogs.log';
    const bufferSize = 546 * 5; // Read in chunks of 2KB
    const buffer = Buffer.alloc(bufferSize);
    await dbConnect(dbLocation)
    const readStream = fs.createReadStream(filePath, { encoding: 'utf8' });

    readStream.on('data', async (chunk) => {
        readStream.pause(); // Pause the stream to prevent further data events
        // await someAsyncFunction(chunk);

        const dataRead = chunk
        const chunkNumMsgs = dataRead.split(', ').length - 1

        for (let i = 0; i < chunkNumMsgs; i++) {
            const dbData = {}
            const nextDateMark = dataRead.indexOf('-', dateStartIndex + (i * 546))
            const msgDataSet = dataRead.substring(nextDateMark - 4, nextDateMark + 542)

            //date
            const dateMark = msgDataSet.indexOf('-')
            dbData.date = msgDataSet.substring(dateMark - 4, dateMark + 6)

            //time
            const timeMark = msgDataSet.indexOf(':')
            dbData.time = msgDataSet.substring(timeMark - 2, timeMark + 10)

            //Get each message
            const messages = []
            const numMsg = msgDataSet.split('8818e').length - 1

            for (let j = 0; j < numMsg; j++) {
                const msgStart = msgDataSet.indexOf('8818e', 24 + (j * 26))
                const msg = msgDataSet.substring(msgStart, msgStart + 26)

                messages.push(getMessageData(msg))
            }
            dbData.messages = messages

            await enterDBData(dbData)
        }

        console.log('Processed chunk');
        readStream.resume(); // Resume the stream after the async operation is complete
    });
    readStream.on('end', () => {
        console.log('Finished reading the file.');
    });

    readStream.on('error', (err) => {
        console.error('Error reading file:', err);
    });
}

function getMessageData(msg) {
    const returnData = {}

    const msgStartIndex = msg.indexOf('8818e')
    const smgNum = msg.substring(msgStartIndex + 5, msgStartIndex + 6)

    returnData.invAddr = msg.substring(msgStartIndex + 6, msgStartIndex + 8)
    returnData.bmsAddr = msg.substring(msgStartIndex + 8, msgStartIndex + 10)

    /***********************************
     * Table per Message - Little Indien
     * MSG1 - Date, Time, Source, Dest, Voltage, Current, SoC
     * MSG2 -  Date, Time, Source, Dest, Charge Limit, Dischathe Limit, Charge Voltage Target, Discharge Voltage Target,  
     * MSG3 -  Date, Time, Source, Dest, Charge Power, Discharge Power, BMS Status, SoP
     * MSG4 -  Date, Time, Source, Dest, Max Cell Voltage, Min Cell Voltage, Max Cell Temperature, Min Cell Temperature 
     * */

    switch (smgNum) {
        case '1':
            returnData.type = '1'

            returnData.voltage = parseInt((msg.substring(msgStartIndex + 12, msgStartIndex + 14) + msg.substring(msgStartIndex + 10, msgStartIndex + 12)), 16) / 10
            returnData.current = parseInt((msg.substring(msgStartIndex + 16, msgStartIndex + 18) + msg.substring(msgStartIndex + 14, msgStartIndex + 16)), 16) / 10
            returnData.soc = parseInt((msg.substring(msgStartIndex + 20, msgStartIndex + 22) + msg.substring(msgStartIndex + 18, msgStartIndex + 20)), 16) / 10
            returnData.soh = parseInt((msg.substring(msgStartIndex + 24, msgStartIndex + 26) + msg.substring(msgStartIndex + 22, msgStartIndex + 24)), 16) / 10

            break
        case '2':
            returnData.type = '2'

            returnData.chargeLimit = parseInt((msg.substring(msgStartIndex + 12, msgStartIndex + 14) + msg.substring(msgStartIndex + 10, msgStartIndex + 12)), 16) / 10
            returnData.dischargeCurrentLimit = parseInt((msg.substring(msgStartIndex + 16, msgStartIndex + 18) + msg.substring(msgStartIndex + 14, msgStartIndex + 16)), 16) / 10
            returnData.chargeVoltage = parseInt((msg.substring(msgStartIndex + 20, msgStartIndex + 22) + msg.substring(msgStartIndex + 18, msgStartIndex + 20)), 16) / 10
            returnData.dischargeVoltage = parseInt((msg.substring(msgStartIndex + 24, msgStartIndex + 26) + msg.substring(msgStartIndex + 22, msgStartIndex + 24)), 16) / 10

            break
        case '3':
            returnData.type = '3'

            returnData.chargingPowAvl = parseInt((msg.substring(msgStartIndex + 12, msgStartIndex + 14) + msg.substring(msgStartIndex + 10, msgStartIndex + 12)), 16) / 10
            returnData.dischargingPowAvl = parseInt((msg.substring(msgStartIndex + 16, msgStartIndex + 18) + msg.substring(msgStartIndex + 14, msgStartIndex + 16)), 16) / 10
            returnData.bmsStatus = parseInt((msg.substring(msgStartIndex + 20, msgStartIndex + 22) + msg.substring(msgStartIndex + 18, msgStartIndex + 20)), 16)
            returnData.sop = parseInt((msg.substring(msgStartIndex + 24, msgStartIndex + 26) + msg.substring(msgStartIndex + 22, msgStartIndex + 24)), 16) / 10

            break
        case '4':
            returnData.type = '4'

            returnData.maxCellVoltage = parseInt((msg.substring(msgStartIndex + 12, msgStartIndex + 14) + msg.substring(msgStartIndex + 10, msgStartIndex + 12)), 16) / 1000
            returnData.minCellVoltage = parseInt((msg.substring(msgStartIndex + 16, msgStartIndex + 18) + msg.substring(msgStartIndex + 14, msgStartIndex + 16)), 16) / 1000
            returnData.maxCellTemperature = parseInt((msg.substring(msgStartIndex + 20, msgStartIndex + 22) + msg.substring(msgStartIndex + 18, msgStartIndex + 20)), 16) / 10
            returnData.minCellTemperature = parseInt((msg.substring(msgStartIndex + 24, msgStartIndex + 26) + msg.substring(msgStartIndex + 22, msgStartIndex + 24)), 16) / 10

            break
    }

    return returnData

}

start()