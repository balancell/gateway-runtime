const { readFileSync } = require('fs');
const Client = require('ssh2').Client;
const mqtt = require('mqtt')

let localMQTTClient
let localMQTTClient_Connected = false

const connectionParameters = {
    port: 22,
    username: 'root',
    password: 'yeti'
}

const alarmLimits = {
    cellVoltageHigh: 4.2,
    cellVoltageLow: 2.4,
    moduleCurrentHighScale: 2,
    overTemperature: 55,
    moduleSocHigh: 100,
    moduleSocLow: 3,
    bankModuleSocImbalance: 20
}

const listOfGateways = ['gw-000101', 'gw-000109', 'gw-000113']
const listOfCommands = ['df -h | grep \'/dev/root\'', 'top -b']

function auditGateway(connectionParameters, command) {
    const sshClient = new Client()

    let cmdExec = new Promise((resolve, reject) => {

        sshClient.on('ready', () => {
            sshClient.exec(command, (err, stream) => {
                if (err) throw err

                stream.on('data', (data) => {

                    sshClient.end();
                    resolve('' + data)

                }).stderr.on('data', (data) => {
                    resolve('' + data)

                }).on('close', (code, signal) => {
                    resolve('Command exited with code ' + code)

                    sshClient.end(); // Close the SSH connection
                });
            });

        }).on('error', (err) => {
            reject(`${connectionParameters.host}`)

        }).connect(connectionParameters);
    })

    return cmdExec
}

async function startAudit() {
    for (const i in listOfGateways) {
        console.log('\nStarting test on device: ' + listOfGateways[i])
        const deviceAudit = await auditGatewaySet(listOfGateways[i])
        console.dir(deviceAudit)
    }
}

function auditGatewaySet(hostname) {

    let batteryAudit = new Promise(async (resolve, reject) => {
        const deviceReport = {}
        deviceReport.deviceInfo = hostname

        //System State Audit
        connectionParameters.host = hostname
        for (const j in listOfCommands) {
            try {

                const data = await auditGateway(connectionParameters, listOfCommands[j])

                const arrayData = data.split(/\s+/)

                switch (j) {
                    case '0':
                        deviceReport.remDisk = getRemaingDisk(arrayData) //GB

                        break
                    case '1':
                        if (arrayData.includes("top")) {
                            //CPU Idle Time
                            deviceReport.freeCPU = getFreeCPU(arrayData) //%

                            //Remaining RAM
                            deviceReport.remRAM = getRemaingRam(arrayData)  //Mb
                        }

                        break
                    default:
                        //console.dir(data)
                        break
                }

            }
            catch (error) {
                console.log("Couldn't connect to " + error)
                reject(error)
            }
        }

        //Battery State Audit
        startMQTTConnection(hostname, `iot/gateway/v1/${hostname}/batteryData`, (topic, data, hostName) => {
            deviceReport.batterySummary = getBatterySummary(JSON.parse(data))

            resolve(deviceReport)

            localMQTTClient.end(true)
        })
    })

    return batteryAudit
}

function getBatterySummary(data) {

    const batterySummary = {}
    let bankNumber = 0

    for (const i in data) {
        if (i.startsWith('bank')) {
            const bankData = JSON.parse(data[i])

            const bankSummary = {}

            bankSummary.soc = bankData.soc
            bankSummary.voltage = bankData.voltage / 1000
            bankSummary.current = bankData.current / 1000
            bankSummary.maxCellTemperature = bankData.maxCellTemperature
            bankSummary.maxCellVoltage = bankData.maxCellVoltage
            bankSummary.minCellVoltage = bankData.minCellVoltage

            batterySummary[`${i}`] = bankSummary
            bankNumber++
        }
    }
    return batterySummary
}

function getRemaingDisk(data) {
    return `${data[3]}`
}

function getFreeCPU(data) {
    return `${data[data.indexOf("id,") - 1]}%`
}

function getRemaingRam(data) {
    const freeIndex = data.indexOf("free,")
    if (data[freeIndex - 5] == 'Mem') return `${data[freeIndex - 1]}MB`

    return 0
}

function getSystemInfo(data) {

}

function startMQTTConnection(hostName, topic, messageCallback) {
    localMQTTClient = mqtt.connect(`mqtt://${hostName}`)

    localMQTTClient.on('connect', () => {
        console.log(`Connect to ${hostName}`)
        localMQTTClient_Connected = true
        localMQTTClient.subscribe(topic)
    })

    localMQTTClient.on('message', (topic, message) => {
        if (messageCallback) messageCallback(topic, message, hostName)
    })
    return localMQTTClient
}

function mqttDataRecieve(topic, message) {
    // If topic == 'iot/gateway/v1/Bumblebee/batteryData' xxx not hard-coded 'Bumblebee'
    const messageObj = JSON.parse(message)
    decodeMSG(messageObj)
    //console.dir(messageObj)
}

setInterval(startAudit, 10000)