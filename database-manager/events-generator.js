const mqtt = require('mqtt')
const { act } = require('react')

let localMQTTClient
let mqttClientConnected = false
const localEventMqttTopic = 'local/events'

const alarmLimits = {
  cellVoltageHigh: 4.2,
  cellVoltageLow: 2.4,
  moduleCurrentHighScale: 2,
  overTemperature: 55,
  moduleSocHigh: 100,
  moduleSocLow: 3,
  bankModuleSocImbalance: 20
}

const eventTypes = {
  alarm: 'Alarm',
  warning: 'Warning',
  system: 'System',
  inverter: 'Inverter',
  info: 'Info'
}

const eventLabel = {
  oc: 'Over Current',
  ov: 'Over Voltage',
  uv: 'Under Voltage',
  ot: 'Over Temperature',
  hiSoC: 'High SoC',
  loSoC: 'Low SoC',
  info_almClear: 'Alarm Cleared'
}

const HWLevelCode = {
  cell: '00',
  module: '11',
  bank: '22'
}

const typeCode = {
  cellOverVoltage: '00',
  cellUnderVoltage: '01',
  moduleOverCurrent: '10',
  cellOverTemperature: '03',
  moduleOverTemperature: '11',
  moduleSocHigh: '12',
  moduleSocLow: '13',
  bankModuleSocImbalance: '20'
}

const activeEvents = []

function startEventsGen() {
  localMQTTClientConnect(`iot/gateway/v1/+/batteryData`, mqttEventData)
}

function localMQTTClientConnect(topic, messageCallback) {
  localMQTTClient = mqtt.connect('mqtt://localhost')

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
  if (mqttClientConnected) localMQTTClient.subscribe(topic)
}

function mqttEventData(topic, message) {
  const messageObj = JSON.parse(message)
  getBatteryEvents(messageObj)
}

function getBatteryEvents(messageObj) {
  const newEvents = []

  /**************************************
   *                Module Alarms
   *  Module Over Current
   *  Module Over Temeperature
   *  Module Low SoC
   *  Module High SoC
   *************************************/

  for (const i in messageObj) {
    if (i.startsWith('module')) {
      const moduleData = JSON.parse(messageObj[i])

      /****************Generate Module Level Events***************** */

      const modulePos = (i.match(/_/gi).length == 1) ? `1_${i.replace('module-', '')}` : `${i.replace('module-', '')}`

      //Check module over current
      const moduleOCCode = `${HWLevelCode.module}${typeCode.moduleOverCurrent}_${modulePos}`

      if (!activeEvents.includes(moduleOCCode) && ((moduleData.current / 1000) > moduleData.chargeCurrentLimit * alarmLimits.moduleCurrentHighScale)) {
        const moduleOCAlarm = {}

        moduleOCAlarm.type = eventTypes.alarm
        moduleOCAlarm.label = eventLabel.oc
        moduleOCAlarm.description = `Module ${modulePos} current HIGH at ${moduleData.current / 1000}A.`
        moduleOCAlarm.code = moduleOCCode

        activeEvents.push(moduleOCAlarm.code) //Add to active events global list
        newEvents.push(moduleOCAlarm) //Add to new events list for DB Publish
      } else {

        if (activeEvents.includes(moduleOCCode) && !((moduleData.current / 1000) > moduleData.chargeCurrentLimit * alarmLimits.moduleCurrentHighScale)) {
          const clearModuleOCAlarm = {}

          clearModuleOCAlarm.type = eventTypes.info
          clearModuleOCAlarm.label = eventLabel.info_almClear
          clearModuleOCAlarm.description = `Cleared: Module ${modulePos} current HIGH, now at ${moduleData.current / 1000}A.`
          clearModuleOCAlarm.code = moduleOCCode

          newEvents.push(clearModuleOCAlarm)
        }
      }

      //Module Over Temperature 
      const moduleOTCode = `${HWLevelCode.module}${typeCode.moduleOverTemperature}_${modulePos}`

      if (!activeEvents.includes(moduleOTCode) && moduleData.temperature > alarmLimits.overTemperature) {
        const moduleOTAlarm = {}

        moduleOTAlarm.type = eventTypes.alarm
        moduleOTAlarm.label = eventLabel.ot
        moduleOTAlarm.description = `Module ${modulePos} temperature HIGH at ${moduleData.temperature}C.`
        moduleOTAlarm.code = moduleOTCode

        activeEvents.push(moduleOTAlarm.code) //Add to active events global list
        newEvents.push(moduleOTAlarm) //Add to new events list for DB Publish
      } else {
        if (activeEvents.includes(moduleOTCode) && !(moduleData.temperature > alarmLimits.overTemperature)) {
          const clearModuleOTAlarm = {}

          clearModuleOTAlarm.type = eventTypes.info
          clearModuleOTAlarm.label = eventLabel.info_almClear
          clearModuleOTAlarm.description = `Cleared: Module ${modulePos} temperature HIGH, now at ${moduleData.temperature}C.`
          clearModuleOTAlarm.code = moduleOTCode

          newEvents.push(clearModuleOTAlarm)
        }
      }

      //Module Low SoC
      const moduleLoSoCCode = `${HWLevelCode.module}${typeCode.moduleSocLow}_${modulePos}`

      if (!activeEvents.includes(moduleLoSoCCode) && moduleData.soc < alarmLimits.moduleSocLow) {
        const moduleLoSoCAlarm = {}

        moduleLoSoCAlarm.type = eventTypes.alarm
        moduleLoSoCAlarm.label = eventLabel.loSoC
        moduleLoSoCAlarm.description = `Module ${modulePos} LOW SoC at ${moduleData.soc}%.`
        moduleLoSoCAlarm.code = moduleLoSoCCode

        activeEvents.push(moduleLoSoCAlarm.code) //Add to active events global list
        newEvents.push(moduleLoSoCAlarm) //Add to new events list for DB Publish
      } else {
        if (activeEvents.includes(moduleLoSoCCode) && !(moduleData.soc < alarmLimits.moduleSocLow)) {
          const clearModuleLoSoCAlarm = {}

          clearModuleLoSoCAlarm.type = eventTypes.info
          clearModuleLoSoCAlarm.label = eventLabel.info_almClear
          clearModuleLoSoCAlarm.description = `Cleared: Module ${modulePos} LOW SoC, now at ${moduleData.soc}%.`
          clearModuleLoSoCAlarm.code = moduleLoSoCCode

          newEvents.push(clearModuleLoSoCAlarm)
        }
      }

      //Module High SoC
      const moduleHiSoCCode = `${HWLevelCode.module}${typeCode.moduleSocHigh}_${modulePos}`

      if (!activeEvents.includes(moduleHiSoCCode) && moduleData.soc > alarmLimits.moduleSocHigh) {
        const moduleHiSoCAlarm = {}

        moduleHiSoCAlarm.type = eventTypes.alarm
        moduleHiSoCAlarm.label = eventLabel.hiSoC
        moduleHiSoCAlarm.description = `Module ${modulePos} SoC  HIGH at ${moduleData.soc}%.`
        moduleHiSoCAlarm.code = moduleHiSoCCode

        activeEvents.push(moduleHiSoCAlarm.code) //Add to active events global list
        newEvents.push(moduleHiSoCAlarm) //Add to new events list for DB Publish
      } else {
        if (activeEvents.includes(moduleHiSoCCode) && !(moduleData.soc > alarmLimits.moduleSocHigh)) {
          const clearModuleHiSoCAlarm = {}

          clearModuleHiSoCAlarm.type = eventTypes.info
          clearModuleHiSoCAlarm.label = eventLabel.info_almClear
          clearModuleHiSoCAlarm.description = `Cleared: Module ${modulePos} SoC HIGH, now at ${moduleData.soc}%.`
          clearModuleHiSoCAlarm.code = moduleHiSoCCode

          newEvents.push(clearModuleHiSoCAlarm)
        }
      }

      /****************Generate Cell Level Events***************** */

      for (const j in moduleData.cellVoltages) {
        //Cell Over Voltage 
        const cellOVCode = `${HWLevelCode.cell}${typeCode.cellOverVoltage}_${modulePos}_${parseInt(j) + 1}`

        if (!activeEvents.includes(cellOVCode) && moduleData.cellVoltages[j] > alarmLimits.cellVoltageHigh) {
          const cellOVAlarm = {}

          cellOVAlarm.type = eventTypes.alarm
          cellOVAlarm.label = eventLabel.ov
          cellOVAlarm.description = `Cell ${modulePos}_${parseInt(j) + 1} Over Voltage at ${moduleData.cellVoltages[j]}V.`
          cellOVAlarm.code = cellOVCode

          activeEvents.push(cellOVAlarm.code) //Add to active events global list
          newEvents.push(cellOVAlarm) //Add to new events list for DB Publish
        } else {
          if (activeEvents.includes(CellOVCode) && !(moduleData.cellVoltages[j] > alarmLimits.cellVoltageHigh)) {
            const clearCellOVAlarm = {}

            clearCellOVAlarm.type = eventTypes.info
            clearCellOVAlarm.label = eventLabel.info_almClear
            clearCellOVAlarm.description = `Cleared: Cell ${modulePos}_${parseInt(j) + 1} Over Voltage, now at ${moduleData.cellVoltages[j]}V.`
            clearCellOVAlarm.code = CellOVCode

            newEvents.push(clearCellOVAlarm)
          }
        }

        //Cell Under Voltage
        const CellUVCode = `${HWLevelCode.cell}${typeCode.cellUnderVoltage}_${modulePos}_${parseInt(j) + 1}`

        if (!activeEvents.includes(CellUVCode) && moduleData.cellVoltages[j] < alarmLimits.cellVoltageLow) {
          const CellUVAlarm = {}

          CellUVAlarm.type = eventTypes.alarm
          CellUVAlarm.label = eventLabel.uv
          CellUVAlarm.description = `Cell ${modulePos}_${parseInt(j) + 1} Under Voltage at ${moduleData.cellVoltages[j]}V.`
          CellUVAlarm.code = CellUVCode

          activeEvents.push(CellUVAlarm.code) //Add to active events global list
          newEvents.push(CellUVAlarm) //Add to new events list for DB Publish
        } else {
          if (activeEvents.includes(CellUVCode) && !(moduleData.cellVoltages[j] < alarmLimits.cellVoltageLow)) {
            const clearCellUVAlarm = {}

            clearCellUVAlarm.type = eventTypes.info
            clearCellUVAlarm.label = eventLabel.info_almClear
            clearCellUVAlarm.description = `Cleared: Cell ${modulePos}_${parseInt(j) + 1} Under Voltage, now at ${moduleData.cellVoltages[j]}V.`
            clearCellUVAlarm.code = CellUVCode

            newEvents.push(clearCellUVAlarm)
          }
        }
      }

      for (const j in moduleData.cellTemperatures) {
        //Cell High Temperature
        const CellOTCode = `${HWLevelCode.Cell}${typeCode.CellSocHigh}_${modulePos}_${parseInt(j) + 1}`

        if (!activeEvents.includes(CellOTCode) && moduleData.cellTemperatures[j] > alarmLimits.overTemperature) {
          const CellOTAlarm = {}

          CellOTAlarm.type = eventTypes.alarm
          CellOTAlarm.label = eventLabel.ot
          CellOTAlarm.description = `Cell ${modulePos}_${parseInt(j) + 1} Over Temperature at ${moduleData.cellTemperatures[j]}%.`
          CellOTAlarm.code = CellOTCode

          activeEvents.push(CellOTAlarm.code) //Add to active events global list
          newEvents.push(CellOTAlarm) //Add to new events list for DB Publish
        } else {
          if (activeEvents.includes(CellOTCode) && !(moduleData.cellTemperatures[j] > alarmLimits.overTemperature)) {
            const clearCellOTAlarm = {}

            clearCellOTAlarm.type = eventTypes.info
            clearCellOTAlarm.label = eventLabel.info_almClear
            clearCellOTAlarm.description = `Cleared: Cell ${modulePos}_${parseInt(j) + 1} Over Temperature, now at ${moduleData.cellTemperatures[j]}%.`
            clearCellOTAlarm.code = CellOTCode

            newEvents.push(clearCellOTAlarm)
          }
        }
      }
    }
  }

  //Remove cleared alarms
  for (const i in newEvents)
    if (newEvents[i].label == 'Alarm Cleared')
      activeEvents.splice(activeEvents.indexOf(newEvents[i].code), 1)

  if(newEvents.length != 0){
      publishEvents(localEventMqttTopic, newEvents)
  }
}

function publishEvents(topic, event){
  const events = {}
  events.events = event
  if (localMQTTClient) localMQTTClient.publish(topic, JSON.stringify(events), { qos: 1, retain: false})
}

startEventsGen()