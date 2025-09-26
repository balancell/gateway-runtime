const sqlite3 = require('sqlite3').verbose()
const fs = require('fs')
const mqtt = require('mqtt') 

let localMQTTClient
let timeseriesDB
let dbConnected = false
const dbLocation = `./database-events-manager/databases/batterytimeseries.db`

function localMQTTClientConnect(topic, messageCallback) {
    localMQTTClient = mqtt.connect('mqtt://gw-000105')

    localMQTTClient.on('connect', () => {
        // TODO add for...in for topics if they are an array.
        localMQTTClient.subscribe(topic)
        mqttClientConnected = true
    })

    localMQTTClient.on('message', (topic, message) => {
        // log(topic + ' ----> ' + message.toString())
        if (messageCallback) messageCallback(topic, message)
    })

    localMQTTClient.on('error', (err) => {
        mqttClientConnected = false
    })

    localMQTTClient.on('disconnect', () => {
        mqttClientConnected = false
    })
}

function mqttClientTopicSub(topic) {
    if (localMQTTClient) localMQTTClient.subscribe(topic)
}

async function MQTTDataReceive(topic, message) {
    const messageObj = JSON.parse(message)
    await dbConnect(dbLocation)
    await enterDBData(messageObj)
    await dbDisconnect()
}

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
                CREATE TABLE batteryData(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    dateTime TEXT NOT NULL,
                    bankNumber TEXT NOT NULL,
                    voltage REAL,
                    current REAL,
                    chargingCurrent REAL,
                    soc REAL,
                    maxCellTemperature REAL,
                    maxCellVoltage REAL,
                    minCellVoltage REAL

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

                const msg1 = timeseriesDB.prepare('INSERT INTO batteryData (dateTime, bankNumber, voltage, current, chargingCurrent, soc, maxCellTemperature, maxCellVoltage, minCellVoltage) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)')

                const eventTimestamp = Date.now();
                const dateTime = new Date(eventTimestamp);
                const formattedDate = dateTime.toLocaleString();

                if (msgData.type == 'MultiBank Timeseries') {
                    for (const i in msgData.bankDataSet) {
                        let bankData = msgData.bankDataSet[i]
                        msg1.run(formattedDate, bankData.bankNumber, bankData.voltage, bankData.current, bankData.chargeCurrentLimit, bankData.soc, bankData.maxCellTemperature, bankData.maxCellVoltage, bankData.minCellVoltage)
                    }
                }
                else {
                    msg1.run(formattedDate, msgData.bankNumber, msgData.voltage, msgData.current, msgData.chargeCurrentLimit , msgData.soc, msgData.maxCellTemperature, msgData.maxCellVoltage, msgData.minCellVoltage)
                }

                msg1.finalize()
            }
            resolve(true)
        }

        catch (error) {
            //failed to write
            console.log(error)
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

function start() {
    createDB(dbLocation)
    localMQTTClientConnect('local/timeseries', MQTTDataReceive)
}
start() 