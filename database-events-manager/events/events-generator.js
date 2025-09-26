const mqtt = require('mqtt')
const { exit } = require('process')
const { act } = require('react')

let localMQTTClient
const localEventMqttTopic = 'local/events'

const alarmLimits = {
  cellVoltageHigh: 4200,
  cellVoltageLow: 2400,
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
  bt: 'Battery Trip',
  btc: 'Battery Trip Clear',
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
  procState: '14',
  procStateClear: '15',
  bankModuleSocImbalance: '20'
}

const activeEvents = []

const latestProcSts = []
const latestShrtAddr = []
let batteryData = []

function startEventsGen() {
  localMQTTClientConnect(`iot/gateway/v1/+/batteryData`, MQTTDataReceive)
  mqttClientTopicSub('local/bem/response/+/getProtectorState')
  mqttClientTopicSub('local/bem/response/+/getCellValues')
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
  if (localMQTTClient) localMQTTClient.subscribe(topic)
}

function MQTTDataReceive(topic, message) {
  const messageObj = JSON.parse(message)
  console.log(topic)

  if (topic.includes('batteryData')) {
    getBatteryDataEvents(topic, messageObj)
    storeBatteryData(topic, messageObj)
  }
  else if (topic.includes('getProtectorState')) getProtectorChangeEvent(topic, messageObj)
  else if (topic.includes('getCellValues')) getCellEvents(topic, messageObj)
}

function storeBatteryData(topic, messageObj) {
  //Check if topic already exists
  let topicFound = false
  let topicPos
  for (topicPos = 0; topicPos < batteryData; topicPos++) if (batteryData[i].topic = topic) {
    topicFound = true
    break
  }
  if (topicFound) batteryData[topicPos].message = messageObj
  else {
    const data = {}
    data.topic = topic
    data.message = messageObj
    batteryData.push(data)
  }
}

function getProtectorChangeEvent(topic, message) {
  const protectorState = {}

  protectorState.sourceAddr = message.sourceAddress
  protectorState.protectorStateChange_count = message.protector_state_change_count

  if (!latestShrtAddr.includes(message.sourceAddress)) {
    updateProcModuleInfo()

    for (let i in latestProcSts) if (latestProcSts[i].addr == message.sourceAddress)
      latestProcSts[i].protectorStateChange_count = message.protector_state_change_count
  }

  //Get string battery data from stored battery data
  for (let i in latestProcSts) {
    if (latestProcSts[i].addr == message.sourceAddress)
      if (latestProcSts[i].protectorStateChange_count != message.protector_state_change_count) {
        const moduleBTCode = `${HWLevelCode.module}${typeCode.procState}_${latestProcSts[i].serial}`

        let event = []
        const moduleOCAlarm = {}

        if ((protectorState.protectorStateChange_count % 2) == 1) {
          moduleOCAlarm.type = eventTypes.alarm
          moduleOCAlarm.label = eventLabel.bt
          moduleOCAlarm.description = `Module ${latestProcSts[i].serial} tripped.`
          moduleOCAlarm.code = moduleBTCode
        } else {

          const moduleBTCCode = `${HWLevelCode.module}${typeCode.procStateClear}_${latestProcSts[i].serial}`

          moduleOCAlarm.type = eventTypes.info
          moduleOCAlarm.label = eventLabel.btc
          moduleOCAlarm.description = `Module ${latestProcSts[i].serial} trip Cleared.`
          moduleOCAlarm.code = moduleBTCCode
        }
        event.push(moduleOCAlarm)
        publishEvents(localEventMqttTopic, event)
        latestProcSts[i].protectorStateChange_count = message.protector_state_change_count
      }
  }
}

function getCellEvents(topic, message) {
  const newEvents = []
  const protectorState = {}

  protectorState.sourceAddr = message.sourceAddress
  protectorState.protectorStateChange_count = message.protector_state_change_count

  if (!latestShrtAddr.includes(message.sourceAddress))
    updateProcModuleInfo()

  let serialNo
  for (let i in latestProcSts) if (latestProcSts[i].addr == message.sourceAddress) serialNo = latestProcSts[i].serial

  const moduleCellData = message.cells
  /****************Generate Cell Level Events***************** */

  for (const j in moduleCellData) {
    //Cell Over Voltage 
    const cellOVCode = `${HWLevelCode.cell}${typeCode.cellOverVoltage}_${serialNo}_${parseInt(j) + 1}`

    if (!activeEvents.includes(cellOVCode) && moduleCellData[j].cellVoltage > alarmLimits.cellVoltageHigh) {
      const cellOVAlarm = {}

      cellOVAlarm.type = eventTypes.alarm
      cellOVAlarm.label = eventLabel.ov
      cellOVAlarm.description = `Cell ${parseInt(j) + 1} in module ${serialNo} Over Voltage at ${moduleCellData[j].cellVoltage / 1000}V`
      cellOVAlarm.code = cellOVCode

      activeEvents.push(cellOVAlarm.code) //Add to active events global list
      newEvents.push(cellOVAlarm) //Add to new events list for DB Publish
    } else {
      if (activeEvents.includes(cellOVCode) && !(moduleCellData[j].cellVoltage > alarmLimits.cellVoltageHigh)) {
        const clearCellOVAlarm = {}

        clearCellOVAlarm.type = eventTypes.info
        clearCellOVAlarm.label = eventLabel.info_almClear
        clearCellOVAlarm.description = `Cleared: Cell ${parseInt(j) + 1} in module ${serialNo} Over Voltage, now at ${moduleCellData[j].cellVoltage / 1000}V.`
        clearCellOVAlarm.code = cellOVCode

        newEvents.push(clearCellOVAlarm)
      }
    }

    //Cell Under Voltage
    const cellUVCode = `${HWLevelCode.cell}${typeCode.cellUnderVoltage}_${serialNo}_${parseInt(j) + 1}`

    if (!activeEvents.includes(cellUVCode) && moduleCellData[j].cellVoltage < alarmLimits.cellVoltageLow) {
      const CellUVAlarm = {}

      CellUVAlarm.type = eventTypes.alarm
      CellUVAlarm.label = eventLabel.uv
      CellUVAlarm.description = `Cell ${parseInt(j) + 1} in module ${serialNo} Under Voltage at ${moduleCellData[j].cellVoltage / 1000}V.`
      CellUVAlarm.code = cellUVCode

      activeEvents.push(CellUVAlarm.code) //Add to active events global list
      newEvents.push(CellUVAlarm) //Add to new events list for DB Publish
    } else {
      if (activeEvents.includes(cellUVCode) && !(moduleCellData[j].cellVoltage < alarmLimits.cellVoltageLow)) {
        const clearCellUVAlarm = {}

        clearCellUVAlarm.type = eventTypes.info
        clearCellUVAlarm.label = eventLabel.info_almClear
        clearCellUVAlarm.description = `Cleared: Cell ${parseInt(j) + 1} in module ${serialNo} Under Voltage, now at ${moduleCellData[j].cellVoltage / 1000}V.`
        clearCellUVAlarm.code = cellUVCode

        newEvents.push(clearCellUVAlarm)
      }
    }
  }

  for (const j in moduleCellData) {
    //Cell High Temperature
    const CellOTCode = `${HWLevelCode.Cell}${typeCode.CellSocHigh}_${serialNo}_${parseInt(j) + 1}`

    if (!activeEvents.includes(CellOTCode) && moduleCellData[j].temperature > alarmLimits.overTemperature) {
      const CellOTAlarm = {}

      CellOTAlarm.type = eventTypes.alarm
      CellOTAlarm.label = eventLabel.ot
      CellOTAlarm.description = `Cell ${parseInt(j) + 1} in module ${serialNo} Over Temperature at ${moduleCellData[j].temperature}C.`
      CellOTAlarm.code = CellOTCode

      activeEvents.push(CellOTAlarm.code) //Add to active events global list
      newEvents.push(CellOTAlarm) //Add to new events list for DB Publish
    } else {
      if (activeEvents.includes(CellOTCode) && !(moduleCellData[j].temperature > alarmLimits.overTemperature)) {
        const clearCellOTAlarm = {}

        clearCellOTAlarm.type = eventTypes.info
        clearCellOTAlarm.label = eventLabel.info_almClear
        clearCellOTAlarm.description = `Cleared: Cell ${parseInt(j) + 1} in module ${serialNo} Over Temperature, now at ${moduleCellData[j].temperature}C.`
        clearCellOTAlarm.code = CellOTCode

        newEvents.push(clearCellOTAlarm)
      }
    }
  }

  //Remove cleared alarms
  for (const i in newEvents)
    if (newEvents[i].label == 'Alarm Cleared')
      activeEvents.splice(activeEvents.indexOf(newEvents[i].code), 1)

  if (newEvents.length != 0) {
    publishEvents(localEventMqttTopic, newEvents)
  }
}

function updateProcModuleInfo() {
  for (let i in batteryData) {
    if (!((batteryData[i].topic).includes('slaveMaster'))) {
      for (let j in batteryData[i].message) {
        if (j.startsWith('P')) {
          const moduleData = JSON.parse(batteryData[i].message[j])

          const moduleInfo = {}
          moduleInfo.addr = moduleData.address
          moduleInfo.serial = moduleData.serial
          moduleInfo.protectorStateChange_count = 0

          if (!latestShrtAddr.includes(moduleInfo.addr)) {
            latestShrtAddr.push(moduleInfo.addr)
            latestProcSts.push(moduleInfo)
          }
        }
      }
    }
  }
}

function getBatteryDataEvents(topic, messageObj) {
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
    }
  }

  //Remove cleared alarms
  for (const i in newEvents)
    if (newEvents[i].label == 'Alarm Cleared')
      activeEvents.splice(activeEvents.indexOf(newEvents[i].code), 1)

  if (newEvents.length != 0) {
    publishEvents(localEventMqttTopic, newEvents)
  }
}

function publishEvents(topic, event) {
  const events = {}
  events.events = event
  if (localMQTTClient) localMQTTClient.publish(topic, JSON.stringify(events), { qos: 1, retain: false })
}


module.exports.startEventsGen = startEventsGen