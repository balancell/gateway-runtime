const sqlite3 = require('sqlite3').verbose()
const fs = require('fs')
const mqtt = require('mqtt')

let clientLocal
let mqttClientConnected = false
const localEventMqttTopic = 'local/events'
let eventsDB
let dbConnected = false

//const dbLocation = `/data/databases/events.db`
const dbLocation = `../gateways/config/events.db`

/************************** Event Description Template********************************************
 EventCode: 'LLTT-Position' = `${HWLevelCode.cell}${typeCode.cellUnderVoltage}_${position}`
 Hardware Level Code (LL): cell 00, module 11, bank 22
 Code Type (TT):
    cellOverVoltage: "Cell ${bankNumber}_${stringNumber}_${moduleNumber}_${cellNumber} voltage HIGH at ${cellVoltage}." - TT = 01
    cellUnderVoltage: "Cell ${bankNumber}_${stringNumber}_${moduleNumber}_${cellNumber} voltage LOW at ${cellVoltage}." - TT = 02
    moduleOverCurrent: "Module ${bankNumber}_${stringNumber}_${moduleNumber}_${cellNumber} current  HIGH at ${moduleCurrent}." - TT = 03
    cellOverTemperature: "Cell ${bankNumber}_${stringNumber}_${moduleNumber}_${cellNumber} temperature HIGH at ${cellTemperature}." - TT = 04
    moduleOverTemperature: "Module ${bankNumber}_${stringNumber}_${moduleNumber} temperature HIGH at ${moduleTemperature}." - TT = 05
    moduleSocHigh: "Module ${bankNumber}_${stringNumber}_${moduleNumber} SoC HIGH at ${moduleSoC}." - TT = 06
    moduleSocLow: "Module ${bankNumber}_${stringNumber}_${moduleNumber} SoC LOW at ${moduleSoC}." - TT = 07
    bankModuleSocImbalance: "Bank ${bankNumber} Module SoC Imbalance with a diff of ${socDiff}." - TT = 08
*/

function startDB() {
  localMQTTClientConnect(localEventMqttTopic, mqttEventData)
  createDB(dbLocation)
}

async function receiveEvent(eventData) {
  await dbConnect(dbLocation)

  if (dbConnected)
    if ('events' in eventData)
      for (const i in eventData.events)
        enterDBData(eventData.events[i])

    else enterDBData(eventData)

  await dbDisconnect()
}

function mqttEventData(topic, message) {
  const messageObj = JSON.parse(message)
  receiveEvent(messageObj)
}

function createDB(dbLocation) {
  //Initial database connection
  if (!fs.existsSync(dbLocation)) {
    eventsDB = new sqlite3.Database(dbLocation, (err) => {
      if (err) {
        console.error('Error connecting to database:', err.message)
      } else {
        dbConnected = true

        // Execute SQL statements from strings.
        // table name is events
        eventsDB.exec(`
                CREATE TABLE events(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    dateTime TEXT NOT NULL,
                    type TEXT NOT NULL,
                    label TEXT NOT NULL,
                    description TEXT NOT NULL,
                    additionalInfo TEXT

                ) STRICT
            `);
      }
    })
  }
}

function enterDBData(eventData) {
  try {
    const dbDataInsert = eventsDB.prepare('INSERT INTO events (dateTime, type, label, description, additionalInfo) VALUES (?, ?, ?, ?, ?)')

    if ('events' in eventData) {

      for (const i in eventData.events) {
        const event = eventData.events[i]

        const eventTimestamp = Date.now();
        const dateTime = new Date(eventTimestamp);
        const formattedDate = dateTime.toLocaleString();

        dbDataInsert.run(formattedDate, event.type, event.label, event.description, ('additionalInfo' in event) ? event.additionalInfo : '')
      }

    } else {
      const event = eventData

      const eventTimestamp = Date.now();
      const dateTime = new Date(eventTimestamp);
      const formattedDate = dateTime.toLocaleString();

      dbDataInsert.run(formattedDate, event.type, event.label, event.description, ('additionalInfo' in event) ? event.additionalInfo : '')
    }

    dbDataInsert.finalize()
  } catch (error) {
    //failed to write
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

function localMQTTClientConnect(topic, messageCallback) {
  clientLocal = mqtt.connect('mqtt://localhost')

  clientLocal.on('connect', () => {
    // TODO add for...in for topics if they are an array.
    clientLocal.subscribe(topic)
    mqttClientConnected = true
  })

  clientLocal.on('message', (topic, message) => {
    // log(topic + ' ----> ' + message.toString())
    if (messageCallback) messageCallback(topic, message)
  })

  clientLocal.on('error', (err) => {
    mqttClientConnected = false
  })

  clientLocal.on('disconnect', () => {
    mqttClientConnected = false
  })
}

function mqttClientTopicSub(topic) {
  if (mqttClientConnected) clientLocal.subscribe(topic)
}

async function dbConnect(dbLocation) {
  //Connect to DB
  try {
    let dbConnectPromise = new Promise((resolve, reject) => {

      eventsDB = new sqlite3.Database(dbLocation, (err) => {
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

        eventsDB.close((err) => {
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

startDB()