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
const csvStringify = require('csv-stringify')
const through2 = require('through2')
const readFile = util.promisify(fs.readFile)

// Suppress ExperimentalWarning coming from csv-parse
const {emitWarning} = process
process.emitWarning = (warning, arg0, ...args) => {
  console.log(warning, arg0)
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
    await transformFile(f)
    spinner.succeed()
  }
  return allData
}

async function transformFile(file) {
  const outName = file.replace('raw', 'processed')
  const columns = [
    "type",
    "year",
    "date",
    "hour",
    "age",
    "defendant_psa",
    "defendant_district",
    "defendant_race",
    "defendant_ethnicity",
    "defendant_sex",
    "category",
    "charge",
    "arrest_psa",
    "arrest_district",
    "offense_psa",
    "offense_district",
    "arrest_lat",
    "arrest_long",
    "offense_lat",
    "offense_long",
    "arrest_ward",
  ]
  return new Promise((resolve, reject) => {
    fs.createReadStream(path.resolve(file), 'utf-8')
      .pipe(csvParse({cast: true, columns: true}))
      .pipe(through2.obj(cleanup2))
      .pipe(csvStringify({header: true, columns: columns}))
      .pipe(fs.createWriteStream(outName))
      .on('close', () => {
        resolve()
      })
  })
  .catch(e => console.error(e))
}

function cleanup(row, enc, next) {
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
    "arrest_year": "year",
    "arrest_date": "date",
    "arrest_hour": "hour",
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
  next(null, cleaned)
}

async function main() {
  const files = glob.sync('data/raw/**/Arrests*.csv')
  await mergeClean(files)
}

main()
