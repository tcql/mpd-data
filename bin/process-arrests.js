#!/usr/bin/env node

const _ = require('lodash')
const path = require('path')
const fs = require('fs')
const glob = require('glob')
const wards = require(path.resolve('./data/raw/wards.json'))
const turf = require('@turf/turf')
const ora = require('ora')
const util = require('util')
const csvParse = require('csv-parse')
const readFile = util.promisify(fs.readFile)

// Suppress ExperimentalWarning coming from csv-parse
const {emitWarning} = process
process.emitWarning = (warning, arg0, ...args) => {
  let type = arg0.type || arg0
  if (arg0 && type && type === 'ExperimentalWarning') return false
  return emitWarning(warning, arg0, ...args)
}

function findWard(long, lat) {
  if (long == "NA" || lat == "NA") return "Unknown"
  let pt = turf.point([long, lat])
  let wFeatures = wards.features
  for (let i = 0; i < wFeatures.length; i++) {
    if (turf.booleanPointInPolygon(pt, wFeatures[i])) {
      return wFeatures[i].properties["NAME"]
    }
  }
  return "Unknown"
}

async function mergeClean(files){
  let allData = []
  for await (let f of files) {
    let fileData = []
    const spinner = ora({ text: `Processing ${f}`, spinner: 'dots3' }).start()
    let parser = fs.createReadStream(path.resolve(f), 'utf-8')
      .pipe(csvParse({cast: true, columns: true}))

    for await (const record of parser) {
      let r = await cleanup(record)
      fileData.push(r)
    }
    let outName = f.replace('raw', 'processed').replace('.csv', '.json')
    fs.writeFileSync(outName, JSON.stringify(fileData))
    allData = allData.concat(fileData)
    spinner.succeed()
  }
  return allData
}

function rename(obj, mappings) {
  return _.mapKeys(obj, (v, k) => mappings[k] ? mappings[k] : k)
}

// Slightly akward async/promise resolution; If we did this
// synchronously, we'd block the pretty spinners from updating,
// so this is just for CLI aesthetics 😅
async function cleanup(row) {
  const dropCols = [
    "ccn",
    "arrest_number",
    "arrest_location_block_geo_x",
    "arrest_location_block_geo_y",
    "arrest_block_geox",
    "arrest_block_geoy",
    "offense_geoy",
    "offense_geox",
    "offense_block_geox",
    "offense_block_geoy",
  ]
  const mappings = {
    "arrestee_type" : "type",
    "arrest_year": "year",
    "arrest_date": "date",
    "arrest_hour": "hour",
    "arrest_category": "category",
    "charge_description": "charge",
    "arrest_location_psa": "arrest_psa",
    "arrest_location_district": "arrest_district",
    "arrest_latitude": "arrest_lat",
    "arrest_longitude": "arrest_long",
    "offense_latitude": "offense_lat",
    "offense_longitude": "offense_long",
  }
  const rename = k => mappings[k] ? mappings[k] : k

  let cleaned = _.chain(row)
    .mapKeys((v, k) => rename(_.snakeCase(k)))
    .assign({arrest_ward: findWard(row["Arrest Longitude"], row["Arrest Latitude"])})
    .omit(dropCols)
    .value()

  return Promise.resolve(cleaned)
}

async function main() {
  const files = glob.sync('data/raw/**/Arrests*.csv')
  const arrests = await mergeClean(files)

  // output will be one large array of all arrests, which are tagged by year
  // if we want to sort them out again. they'll also have the arrest_ward added
  // so we can generate aggregates of where arrests occur
  fs.writeFileSync(path.resolve('data/processed/ArrestsAllYears.json'), JSON.stringify(arrests, null, 2))
}

main()
