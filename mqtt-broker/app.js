const aedes = require('aedes')()
let mqttServer3
const mqtt = require('mqtt')
const mqttPort = 1883
let localMQTTClient
let mqqtBrokerExists = false

let mqqtBrokerSetupTI
clearInterval(mqqtBrokerSetupTI)

const dataStored = []
const storedTopics = []

function createMQTTBroker() {

  const mqttServer = require('net').createServer(aedes.handle)

  mqttServer.listen(mqttPort, () => {
    console.log(`MQTT Broker listening at port ${mqttPort}`)

    clearInterval(mqqtBrokerSetupTI)
    if (dataStored.length != 0) {
      localMQTTClientConnect(MQTTDataReceived)
      if (localMQTTClient)
        for (let i in dataStored)
          localMQTTClient.publish(dataStored[i].topic, JSON.stringify(dataStored[i].message), { qos: 1, retain: true })
    }
  })

  mqttServer.on('error', (err) => {
    mqqtBrokerExists = false
    if (!localMQTTClient) {
      localMQTTClientConnect(MQTTDataReceived)
      MQTTSubscribe('iot/+/+/+/batteryData')
    }
  })

  return mqttServer
}


function MQTTDataReceived(topic, message) {
  const recMsg = {}
  recMsg.topic = topic
  recMsg.message = JSON.parse(message)

  if (!storedTopics.includes(topic)) {
    storedTopics.push(topic)
    dataStored.push(recMsg)
  } else 
    dataStored[storedTopics.indexOf(topic)].message = recMsg.message
}

function MQTTSubscribe(topic) {
  localMQTTClient.subscribe(topic)
}

function startSystem() {
  mqttServer3 = createMQTTBroker()
}

function localMQTTClientConnect(messageCallback) {
  localMQTTClient = mqtt.connect('mqtt://localhost')

  localMQTTClient.on('connect', () => {
    // TODO add for...in for topics if they are an array.
    console.log(`mqtt broker client connected`)
  })

  localMQTTClient.on('message', (topic, message) => {
    // log(topic + ' ----> ' + message.toString())
    if (messageCallback) messageCallback(topic, message)
  })

  localMQTTClient.on('error', (err) => {
  })

  localMQTTClient.on('disconnect', () => {
  })
}

mqqtBrokerSetupTI = setInterval(startSystem, 1000)