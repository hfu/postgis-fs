const tilebelt = require('@mapbox/tilebelt')
const { Pool, Query } = require('pg')
const wkx = require('wkx')

if (process.argv.length < 6) {
  console.log('usage: node index.js {schema.js} {z} {x} {y} {maxzoom}')
  console.log('  - {maxzoom} is optional.')
  process.exit()
}
const schema = require(process.argv[2])
const Z = Number(process.argv[3])
const X = Number(process.argv[4])
const Y = Number(process.argv[5])
const MAX_ZOOM = Number(process.argv[6])
const BBOX = tilebelt.tileToBBOX([X, Y, Z])
let layerCount = schema.data.length

const featureDump = (row, tippecanoe, modify) => {
  let f = {
    type: 'Feature',
    geometry: wkx.Geometry.parse(Buffer.from(row['_geom'], 'hex')).toGeoJSON(),
    tippecanoe: tippecanoe
  }
  delete row['_geom']
  f.properties = row
  if (modify) f = modify(f)
  if (!f) {

  } else if (Array.isArray(f)) {
    for (const part of f) {
      console.log(JSON.stringify(part))
    }
  } else {
    console.log(JSON.stringify(f))
  }
}

const dump = async (database, relation, geom, props, tippecanoe, modify) => {
  // console.error(`${relation}: MAX_ZOOM=${MAX_ZOOM}, tippecanoe.minzoom=${JSON.stringify(tippecanoe)}`)
  if (MAX_ZOOM && tippecanoe.minzoom > MAX_ZOOM) {
    console.error(`skip ${relation} because minzoom ${tippecanoe.minzoom} > MAX_ZOOM ${MAX_ZOOM}.`)
    return
  } else {
    console.error(`starting ${database}::${relation}`)
  }
  let pool = new Pool({
    host: schema.host,
    user: schema.user,
    password: schema.password,
    database: database,
    max: 2
  })
  pool.on('error', (err, client) => {
    console.error(`unexpected error on ${database}`)
    throw err
  })
  const client = await pool.connect()
  await client.query(new Query(`\
WITH 
  envelope AS 
    (SELECT ST_MakeEnvelope(${BBOX.join(', ')}, 4326) AS geom)
SELECT 
  ${props.join(', ')}${props.length === 0 ? '' : ', '} 
  (ST_Intersection(ST_MakeValid(${relation}.${geom}), envelope.geom)) AS _geom 
  FROM ${relation}
  JOIN envelope ON ${relation}.${geom} && envelope.geom 
`))
    .on('row', row => {
      featureDump(row, tippecanoe, modify)
    })
    .on('error', err => {
      console.error(`data error in ${database}::${relation} for ${tippecanoe.layer}:`)
      console.error(err.stack)
      client.release()
    })
    .on('end', () => {
      layerCount--
      client.release()
      console.error(`finished ${database} ${relation}`)
      if (layerCount === 0) {
        process.exit()
      }
    })
}

const main = async () => {
  for (let datum of schema.data) {
    const [database, relation] = datum[0].split('::')
    const props = datum[1]
    const tippecanoe = datum[2]
    const modify = datum[3]
    const geom = datum[4] ? datum[4] : 'geom'
    dump(database, relation, geom, props, tippecanoe, modify)
  }
}

main()
