const net = require('net')
const mqtt = require('mqtt')
const { isObject } = require('util')

let allBatteries = [], listOfBatteries = []

let battertData = {
    uid: 0,
    shortAddress: 0
}

let getMeasuredVI_reply = {
    type: 'getMeasuredVI',
    isReply: true,
    hasShortSrcAdr: true,
    hasLongSrcAdr: false,
    hasShortDstAdr: false,
    hasLongDstAdr: false,
    sourceAddressLength: 1,
    destinationAddressLength: 0,
    multiDrop: true,
    sourceAddress: 5,
    voltage: 106952,
    current: 35703,
    batterySoCFraction: 247,
    battery_SoC: 48,
    shunt_temp: 27,
    internal_temp: 33,
    external_temp: 24,
    shunt2_temp: 26,
    shunt_direction: 1,
    pad: 0,
    timestamp: 1183936397

}

let getProtectorState_reply = {
    type: 'getProtectorState',
    isReply: true,
    hasShortSrcAdr: true,
    hasLongSrcAdr: false,
    hasShortDstAdr: false,
    hasLongDstAdr: false,
    sourceAddressLength: 1,
    destinationAddressLength: 0,
    multiDrop: true,
    sourceAddress: 2,
    t_q_in_start: -548573631,
    t_q_out_start: -548591329,
    t_read: -548574531,
    t_guard_start: 122,
    protector_state_change_count: 8,
    protector_OV_change_count: 0,
    protector_q_in_change_count: 4,
    protector_q_out_change_count: 0,
    v_cell_OV_flag: false,
    v_cell_OV: false,
    v_cell_q_read: false,
    v_cell_UV: false,
    v_cell_UV_flag: false,
    v_pack_OV: false,
    v_pack_full_warn: false,
    v_pack_q_in: false,
    v_pack_q_read: false,
    v_pack_flat_warn: false,
    v_pack_UV: false,
    soc_low: false,
    eswitch_control: true,
    eswitch_k_state: true,
    eswitch_q_in_state: true,
    eswitch_q_out_state: true,
    probe_OT: false,
    shunt_OT: false,
    cells_OT: false,
    probe_UT: false,
    shunt_UT: false,
    cells_UT: false,
    bump_event: false,
    tilt_event: false,
    q_in_state: 1,
    q_out_state: 3,
    cell_request: false,
    cell_pending: false,
    live_pending: false,
    em_pending: false,
    config_valid: true,
    live_valid: true,
    em_valid: true,
    padding: true,
    v_eswitch_drive: 3778,
    q_in_start: 3799003,
    q_out_start: 3798370,
    q_in_limit: 2078,
    q_out_limit: -66480,
    q_read: 3799189

}

let getBatteryConfig_reply = {
    type: 'getBatteryConfig',
    isReply: true,
    hasShortSrcAdr: true,
    hasLongSrcAdr: false,
    hasShortDstAdr: false,
    hasLongDstAdr: false,
    sourceAddressLength: 1,
    destinationAddressLength: 0,
    multiDrop: true,
    sourceAddress: 1,
    battery_name: 'P34-000191',
    company_name: 'P00034',
    installer_name: 'LEBO',
    name_plate_capacity: 277,
    fuse_rating: 0,
    eswitch_control: 1,
    shunt_direction: 1,
    usable_coulomb_capacity: 997200,
    v_pack_OV_trip: 3600,
    v_pack_OV_reset: 3550,
    v_pack_q_in_trip: 3590,
    v_pack_q_in_reset: 3550,
    v_pack_q_read_trip: 3480,
    v_pack_q_read_reset: 3450,
    v_pack_UV_trip: 2900,
    v_pack_UV_reset: 3000,
    v_pack_full_warn_trip: 3500,
    v_pack_full_warn_reset: 3450,
    v_pack_flat_warn_trip: 2900,
    v_pack_flat_warn_reset: 2950,
    v_cell_UV_trip: 2700,
    v_cell_UV_reset: 2900,
    v_cell_q_read_trip: 3480,
    v_cell_q_read_reset: 3450,
    v_cell_OV_trip: 3600,
    v_cell_OV_reset: 3550,
    soc_low_trip: 0,
    soc_low_trip_fraction: 2,
    soc_low_reset: 0,
    soc_low_reset_fraction: 5,
    k_cell_OT_trip: 84,
    k_cell_OT_reset: 50,
    k_cell_UT_trip: -10,
    k_cell_UT_reset: -5,
    k_shunt_OT_trip: 80,
    k_shunt_OT_reset: 70,
    k_shunt_UT_trip: -10,
    k_shunt_UT_reset: -5,
    k_probe_OT_trip: 55,
    k_probe_OT_reset: 50,
    k_probe_UT_trip: -20,
    k_probe_UT_reset: -15,
    q_limit_config: {
        q_out_flat_limit_hex: 'e2ffffffffffffff',
        q_out_bulk_limit_hex: '50fcfeffffffffff',
        q_in_float_limit_hex: '1e00000000000000',
        q_in_bulk_limit_hex: '1e08000000000000',
        q_read_trigger_hex: 'e503000000000000',
        t_q_in_limit: 10000,
        t_q_out_limit: 120000,
        t_read_trigger: 10000,
        t_guard_limit: 5000
    },
    soc_map: [
        { cell_v_min: 2951, cell_v_max: 3000, soc_min: 0, soc_max: 1536 },
        {
            cell_v_min: 3200,
            cell_v_max: 3400,
            soc_min: 768,
            soc_max: 25548
        },
        {
            cell_v_min: 3500,
            cell_v_max: 6000,
            soc_min: 25472,
            soc_max: 25600
        },
        { cell_v_min: 2900, cell_v_max: 2950, soc_min: 0, soc_max: 768 },
        { cell_v_min: 0, cell_v_max: 2899, soc_min: 0, soc_max: 0 }
    ]

}

let getCellValues_reply = {
    type: 'getCellValues',
    isReply: true,
    hasShortSrcAdr: true,
    hasLongSrcAdr: false,
    hasShortDstAdr: false,
    hasLongDstAdr: false,
    sourceAddressLength: 1,
    destinationAddressLength: 0,
    multiDrop: true,
    sourceAddress: 2,
    timestamp: -548574528,
    cells: [
        {
            cellVoltage: 3344,
            balanceCurrent: 0,
            temperature: 25,
            statusByte: 0
        },
        {
            cellVoltage: 3344,
            balanceCurrent: 0,
            temperature: 25,
            statusByte: 0
        },
        {
            cellVoltage: 3339,
            balanceCurrent: 0,
            temperature: 23,
            statusByte: 0
        },
        {
            cellVoltage: 3338,
            balanceCurrent: 0,
            temperature: 22,
            statusByte: 0
        },
        {
            cellVoltage: 3339,
            balanceCurrent: 0,
            temperature: 24,
            statusByte: 0
        },
        {
            cellVoltage: 3337,
            balanceCurrent: 0,
            temperature: 24,
            statusByte: 0
        },
        {
            cellVoltage: 3342,
            balanceCurrent: 0,
            temperature: 25,
            statusByte: 0
        },
        {
            cellVoltage: 3338,
            balanceCurrent: 0,
            temperature: 24,
            statusByte: 0
        },
        {
            cellVoltage: 3344,
            balanceCurrent: 0,
            temperature: 23,
            statusByte: 0
        },
        {
            cellVoltage: 3341,
            balanceCurrent: 0,
            temperature: 26,
            statusByte: 0
        },
        {
            cellVoltage: 3339,
            balanceCurrent: 0,
            temperature: 26,
            statusByte: 0
        },
        {
            cellVoltage: 3341,
            balanceCurrent: 0,
            temperature: 24,
            statusByte: 0
        },
        {
            cellVoltage: 3341,
            balanceCurrent: 0,
            temperature: 24,
            statusByte: 0
        },
        {
            cellVoltage: 3341,
            balanceCurrent: 0,
            temperature: 25,
            statusByte: 0
        },
        {
            cellVoltage: 3350,
            balanceCurrent: 0,
            temperature: 24,
            statusByte: 0
        },
        {
            cellVoltage: 3338,
            balanceCurrent: 0,
            temperature: 25,
            statusByte: 0
        },
        {
            cellVoltage: 3339,
            balanceCurrent: 0,
            temperature: 25,
            statusByte: 0
        },
        {
            cellVoltage: 3337,
            balanceCurrent: 0,
            temperature: 26,
            statusByte: 0
        },
        {
            cellVoltage: 3344,
            balanceCurrent: 0,
            temperature: 25,
            statusByte: 0
        },
        {
            cellVoltage: 3342,
            balanceCurrent: 0,
            temperature: 23,
            statusByte: 0
        },
        {
            cellVoltage: 3339,
            balanceCurrent: 0,
            temperature: 25,
            statusByte: 0
        },
        {
            cellVoltage: 3342,
            balanceCurrent: 0,
            temperature: 25,
            statusByte: 0
        },
        {
            cellVoltage: 3337,
            balanceCurrent: 0,
            temperature: 24,
            statusByte: 0
        },
        {
            cellVoltage: 3339,
            balanceCurrent: 0,
            temperature: 25,
            statusByte: 0
        },
        {
            cellVoltage: 3338,
            balanceCurrent: 0,
            temperature: 23,
            statusByte: 0
        },
        {
            cellVoltage: 3338,
            balanceCurrent: 0,
            temperature: 23,
            statusByte: 0
        },
        {
            cellVoltage: 3347,
            balanceCurrent: 0,
            temperature: 25,
            statusByte: 0
        },
        {
            cellVoltage: 3340,
            balanceCurrent: 0,
            temperature: 24,
            statusByte: 0
        },
        {
            cellVoltage: 3341,
            balanceCurrent: 0,
            temperature: 23,
            statusByte: 0
        },
        {
            cellVoltage: 3338,
            balanceCurrent: 0,
            temperature: 23,
            statusByte: 0
        },
        {
            cellVoltage: 3345,
            balanceCurrent: 0,
            temperature: 24,
            statusByte: 0
        },
        {
            cellVoltage: 3345,
            balanceCurrent: 0,
            temperature: 23,
            statusByte: 0
        }
    ]
}

let logIn_reply = {
    type: 'login',
    isReply: true,
    hasShortSrcAdr: false,
    hasLongSrcAdr: true,
    hasShortDstAdr: false,
    hasLongDstAdr: false,
    sourceAddressLength: 16,
    destinationAddressLength: 0,
    multiDrop: true,
    sourceAddress: {
        type: 'Buffer',
        data: []
    },
    level: 4
    /*
      multiDrop: true,
      hasShortSrcAdr: false,
      hasLongSrcAdr: false,
      hasShortDstAdr: false,
      hasLongDstAdr: true,
      sourceAddressLength: 0,
      destinationAddress: {
        type: 'Buffer',
        data: [
           1,  0, 209, 101, 54, 0,
          63, 62,  21,  32, 31, 0,
          69, 78,  69, 113
        ]
      },
      destinationAddressLength: 16,
      type: 'login',
      delay: 300*/


}

let setPortConfig_reply = {
    type: 'setPortConfig',
    isReply: true,
    hasShortSrcAdr: false,
    hasLongSrcAdr: true,
    hasShortDstAdr: false,
    hasLongDstAdr: false,
    sourceAddressLength: 16,
    destinationAddressLength: 0,
    multiDrop: true,
    sourceAddress: {
        type: 'Buffer',
        data: []
    }
}

let getLastUnackDtgrm = {

}

let getSwVers_reply = {
    type: 'getSwVers',
    isReply: true,
    hasShortSrcAdr: true,
    hasLongSrcAdr: false,
    hasShortDstAdr: false,
    hasLongDstAdr: false,
    sourceAddressLength: 1,
    destinationAddressLength: 0,
    multiDrop: true,
    sourceAddress: 2,
    hardware_revision: 64,
    hardware_revision_MSB: 0,
    hardware_variant: 0,
    hardware_variant_options: 0,
    sw_version_class: 5,
    sw_version_major: 4,
    sw_version_minor: 0,
    sw_version_patch: 9,
    swVersion: '4.0.9'

}

let getLatestDatagram = {

}

let localMQTTClient
let localMQTTClient_Connected = false
//

function startMQTTConnection(slaveIPAddress, topic, messageCallback) {
    console.log("BEM Sim Trying to connect at 1883")
    localMQTTClient = mqtt.connect(`mqtt://172.17.0.2:1883`)

    localMQTTClient.on('connect', () => {
        console.log("BEM Sim Connected")
        localMQTTClient_Connected = true

        // TODO add for...in for topics if they are an array.
        localMQTTClient.subscribe(topic)
    })
    localMQTTClient.on('message', (topic, message) => {
        //log(topic + ' ----> ' + message.toString())
        if (messageCallback) messageCallback(topic, message)
    })
    return localMQTTClient
}

function mqttDataRecieve(topic, message) {
    // If topic == 'iot/gateway/v1/Bumblebee/batteryData' xxx not hard-coded 'Bumblebee'
    const messageObj = JSON.parse(message)
    decodeMSG(messageObj)
    //console.dir(messageObj)
}

function subscribe(topic) {
    if (localMQTTClient) localMQTTClient.subscribe(topic)
}

function startSim(configStruct) {
    console.log('BEM Simulator started')

    setInterval(startMQTTConnection, 10000, `localhost`, `local/bem/request/+/+`, mqttDataRecieve)
    //subscribe(`local/bem/response/+/+`)
}

function decodeMSG(requestData) {
    //Check type

    const messageType = requestData.type;

    let response = {}
    let topic = ''

    switch (messageType) {
        case 'login':
            Object.assign(response, logIn_reply)
            response.sourceAddress.data = requestData.destinationAddress.data

            for (let i in response.sourceAddress.data)
                topic += response.sourceAddress.data[i].toString(16);

            topic = `${topic}/login`

            publishResponse(response, `local/bem/response/${topic}`)

            break
        case 'setPortConfig':
            if ((typeof requestData === 'object') && requestData != null && requestData != 0) {
                if ('destinationAddress' in requestData && requestData.destinationAddress != 0) {
                    Object.assign(response, setPortConfig_reply)

                    response.sourceAddress.data = requestData.destinationAddress.data

                    for (let i in response.sourceAddress.data)
                        topic += response.sourceAddress.data[i].toString(16);

                    let batt = {}
                    batt.uid = topic
                    batt.shortAddress = requestData.shortAddress

                    //Check if the battery does not already exists
                    let battPresent = false
                    for (let i in allBatteries) if (allBatteries[i].uid == topic) battPresent = true

                    if (!battPresent) allBatteries.push(batt)
                    topic = `${topic}/setPortConfig`

                    publishResponse(response, `local/bem/response/${topic}`)
                }
            }
            break
        case 'getBatteryConfig':
            Object.assign(response, getBatteryConfig_reply)
            response.sourceAddress = requestData.destinationAddress
            response.battery_name = (listOfBatteries.length < 10) ? `P34-00100${listOfBatteries.length}` : `P34-0004${listOfBatteries.length}`
            if (listOfBatteries.indexOf(response.battery_name) == -1) listOfBatteries.push(response.battery_name)

            topic = `${requestData.destinationAddress}/getBatteryConfig`

            for (let i in allBatteries) if (allBatteries[i].shortAddress == response.sourceAddress) allBatteries[i].battery_name = response.battery_name

            publishResponse(response, `local/bem/response/${topic}`)

            break
        case 'getSwVers':
            Object.assign(response, getSwVers_reply)
            response.sourceAddress = requestData.destinationAddress

            topic = `${requestData.destinationAddress}/getSwVers`
            publishResponse(response, `local/bem/response/${topic}`)
            break
        case 'getMeasuredVI':
            Object.assign(response, getMeasuredVI_reply)
            response.sourceAddress = requestData.destinationAddress
            response.timestamp = Date.now()


            let initValFilled = true
            let battPos
            for (let i in allBatteries)
                if (allBatteries[i].shortAddress == response.sourceAddress) {
                    initValFilled = 'getMeasuredVI' in allBatteries[i]
                    battPos = i
                }
            if (!initValFilled) {
                response.battery_SoC = response.battery_SoC + Math.random() * 5
                response.voltage = response.voltage + Math.random() * 3
                response.external_temp = response.external_temp + Math.random() * 2
                allBatteries[battPos].getMeasuredVI = response
            }

            response.battery_SoC = allBatteries[battPos].getMeasuredVI.battery_SoC + 0.01
            response.voltage = allBatteries[battPos].getMeasuredVI.voltage + 0.01

            allBatteries[battPos].getMeasuredVI.battery_SoC = response.battery_SoC
            topic = `${requestData.destinationAddress}/getMeasuredVI`

            publishResponse(response, `local/bem/response/${topic}`)

            break
        case 'getProtectorState':
            Object.assign(response, getProtectorState_reply)
            response.sourceAddress = requestData.destinationAddress

            topic = `${requestData.destinationAddress}/getProtectorState`
            publishResponse(response, `local/bem/response/${topic}`)

            break
        case 'getCellValues':
            Object.assign(response, getCellValues_reply)
            response.sourceAddress = requestData.destinationAddress
            response.timestamp = Date.now()

            topic = `${requestData.destinationAddress}/getCellValues`
            publishResponse(response, `local/bem/response/${topic}`)
            break
    }
    //console.log(topic)

}

function publishResponse(response, topic) {
    if (localMQTTClient_Connected) {
        //console.log(response)
        localMQTTClient.publish(topic, JSON.stringify(response), { qos: 1, retain: false })
    }
}

startSim()

exports.simBEMS = startSim