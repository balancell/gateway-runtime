const databaseManager = require('./events/events-db-manager.js')
const eventsGenerator = require('./events/events-generator.js')
const batteryTimeseries = require('./timeseries/batterydata-ts-source.js')
const timeseriesDBmanager = require('./timeseries/timeseries-db-manager.js')
function startSystem(){
    databaseManager.startDB()
    eventsGenerator.startEventsGen()
}
startSystem()