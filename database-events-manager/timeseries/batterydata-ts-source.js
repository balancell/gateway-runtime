const mqtt = require('mqtt')
let localMQTTClient
let batteryTSData = {}
let topic = 'local/timeseries'


//Format - {bankNumber, Voltage, Current, SoC, MaxCellVoltage, MaxCellTemp}

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

function MQTTDataReceive(topic, message) {

    const messageObj = JSON.parse(message)
    console.log(topic)

    //Check topic is not slavemaster 
    if (!topic.includes('slaveMaster')) setBatteryTSData(messageObj)
}

function setBatteryTSData(message) {
    //console.dir(message)
    if (!('bank-1' in message)) {
        for (const i in message) {
            if (i.includes('bank')) {
                const bankData = JSON.parse(message[i])

                batteryTSData.type = 'SingleBank Timeseries'
                batteryTSData.bankNumber = i
                batteryTSData.voltage = bankData.voltage / 1000
                batteryTSData.current = bankData.current / 1000
                batteryTSData.chargeCurrentLimit = bankData.chargeCurrentLimit
                batteryTSData.soc = bankData.soc
                batteryTSData.maxCellTemperature = bankData.maxCellTemperature
                batteryTSData.maxCellVoltage = bankData.maxCellVoltage
                batteryTSData.minCellVoltage = bankData.minCellVoltage

                break
            }
        }
    }
    else{
        batteryTSData.type = 'MultiBank Timeseries'
        bankDataSet = []
        for (const i in message) {
            if (i.includes('bank')) {
                const bankData = JSON.parse(message[i])
                const bankTSData = {}

                bankTSData.type = 'Bank Data'
                bankTSData.bankNumber = i
                bankTSData.voltage = bankData.voltage / 1000
                bankTSData.current = bankData.current / 1000
                bankTSData.chargeCurrentLimit = bankData.chargeCurrentLimit
                bankTSData.soc = bankData.soc
                bankTSData.maxCellTemperature = bankData.maxCellTemperature
                bankTSData.maxCellVoltage = bankData.maxCellVoltage
                bankTSData.minCellVoltage = bankData.minCellVoltage

                bankDataSet.push(bankTSData)
            }
        }
        batteryTSData.bankDataSet = bankDataSet
    }
}

function publishTSData() {
    if (localMQTTClient && (Object.keys(batteryTSData).length > 0)) localMQTTClient.publish(topic, JSON.stringify(batteryTSData), { qos: 1, retain: false })
}

function start() {
    localMQTTClientConnect('iot/gateway/v1/+/batteryData', MQTTDataReceive)
    setInterval(publishTSData, 3000)
}
start()