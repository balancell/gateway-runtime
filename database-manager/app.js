const aedes = require('aedes')()
const mqttServer = require('net').createServer(aedes.handle)
const mqttPort = 1883

const mqtt = require('mqtt')


function createMQTTBroker() {
    /**************** Create MQTT Broker ****************/
    mqttServer.listen(mqttPort, () => {
        console.log(`MQTT Broker listening at port ${mqttPort}`)
    })
}
createMQTTBroker()