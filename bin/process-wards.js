#!/usr/bin/env node

const _ = require('lodash')
const path = require('path')
const fs = require('fs')
const wards = require(path.resolve('./data/raw/wards.json'))

const pickToNumbers = (obj, pick = []) => {
  let picked = obj
  if (_.isArray(pick) && pick.length > 0) {
    picked = _.pick(obj, pick)
  } else if (_.isFunction(pick)) {
    picked = _.pickBy(obj, pick)
  } else if (_.isPlainObject(pick)) {
    picked = _.pick(obj, _.keys(pick))
  }
  let asInts = _.mapValues(picked, _.toNumber)
  if (!_.isPlainObject(pick)) return asInts

  return _.mapKeys(asInts, (v, k) => pick[k] ? pick[k] : k)
}

wards.features.forEach(w => {
  let props = _.mapKeys(w.properties, (val, key) => _.snakeCase(key))
  let meta = pickToNumbers(props, {
    "pop_2011_2015": "total_population",
    "areasqmi": "area_sq_miles",
    "median_hh_income": "median_household_income",
    "per_capita_income": null,
    "med_val_oou": "median_value_of_owner_occupied_units",
    "median_age": null,
    "total_hh": "total_households",
    "unemployment_rate": null
  })
  let ageDemos = pickToNumbers(props, (val, key) => key.indexOf("age_") === 0)
  let raceDemos = pickToNumbers(props, {
    "pop_asian": null,
    "pop_black": null,
    "pop_hawaiin": null,
    "pop_native_american": null,
    "pop_other_race": null,
    "two_or_more_races" : "two_or_more_races",
  })
  raceDemos = _.mapKeys(raceDemos, (v, k) => k.replace("pop_", ""))
  // white is just assumed to be anything else
  // left in population not counted by ^
  let nonWhiteSum = _.sum(_.values(raceDemos))
  raceDemos.white = meta.total_population - nonWhiteSum
  // pre-calculate percentages
  _.keys(raceDemos).forEach(k => {
    raceDemos[`pct_${k}`] = raceDemos[k] / meta.total_population
  })

  let genderDemos = pickToNumbers(props, {"pop_female": "female", "pop_male": "male"})
  let educationSummary = pickToNumbers(props, {
    "pop_25_plus_graduate": "graduates_degree",
    "assoc_degree_25_plus": "associates_degree",
    "bach_degree_25_plus": "bachelors_degree",
    "no_degree_25_plus": "some_college",
    "diploma_25_plus": "highschool_diploma",
    "no_diploma_25_plus": "no_highschool_diploma",
    "pop_25_plus_9th_grade": "below_highschool_education",
  })
  let householdsSummary = pickToNumbers(props, {
    "family_hh": "family_households",
    "nonfamily_hh": "nonfamily_households",
    // weirdly specific gender binary stuff too >:|
    "female_hh_no_husband": "female_householder_no_husband",
    "male_hh_no_wife": "male_householder_no_wife",
    "married_couple_family": null,
    "pct_family_hh": "pct_family_households",
    "pct_nonfamily_hh": "pct_nonfamily_households"
  })

  let povertySummary = pickToNumbers(props, {
    "pct_below_pov": "overall",
    "pct_below_pov_asian": "asian",
    "pct_below_pov_black": "black",
    "pct_below_pov_fam": "families",
    "pct_below_pov_hawaiin": "hawaiin",
    "pct_below_pov_hisp": "hispanic",
    "pct_below_pov_nat_amer": "native_american",
    "pct_below_pov_other": "other_race",
    "pct_below_pov_two_races": "two_or_more_races",
    "pct_below_pov_white": "white",
    "pct_below_pov_white_nohisp": "white_non_hispanic",
  })
  w.properties = {
    ward: props.ward,
    ward_name: props.name,
    meta: meta,
    age: ageDemos,
    gender: genderDemos,
    race: raceDemos,
    education_over_25: educationSummary,
    households: householdsSummary,
    below_poverty_percentages: povertySummary
  }
})
/*
Currently unused fields from raw ward data:

"HISPANIC_OR_LATINO": "1290",
"NOT_HISPANIC_OR_LATINO": "79843",
"REP_EMAIL": "twhite@dccouncil.us",
"REP_NAME": "Trayon White, Sr.",
"REP_OFFICE": "1350 Pennsylvania Ave, Suite 400, NW 20004",
"REP_PHONE": "(202) 724-8045",

*/
fs.writeFileSync(path.resolve("data/processed/wards.json"), JSON.stringify(wards, null, 2))
console.log("Num features:", wards.features.length)
