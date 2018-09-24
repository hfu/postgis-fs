const tilebelt = require('@mapbox/tilebelt')
const { Pool, Query } = require('pg')
const wkx = require('wkx')

if (process.argv.length !== 6) {
  console.log('usage: node index.js {schema.js} {z} {x} {y}')
  process.exit()
}
const schema = require(process.argv[2])
const Z = Number(process.argv[3])
const X = Number(process.argv[4])
const Y = Number(process.argv[5])
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
  if (modify) { f = modify(f) }
  console.log(JSON.stringify(f))
}

const dump = async (database, relation, geom, props, tippecanoe, modify) => {
  let pool = new Pool({
    host: schema.host,
    user: schema.user,
    password: schema.password,
    database: database,
    max: 2
  })
  pool.on('error', (err, client) => {
    console.error(`unexpected error on ${database}`)
  })
  const client = await pool.connect()
  await client.query(new Query(`\
WITH envelope AS 
  (SELECT ST_MakeEnvelope(${BBOX.join(', ')}, 4326) as geom) 
SELECT ${props.join(', ')}${props.length === 0 ? '' : ', '} 
  ST_Intersection(ST_MakeValid(${relation}.${geom}), envelope.geom) AS _geom 
FROM ${relation} JOIN envelope ON ${relation}.${geom} && envelope.geom 
WHERE NOT ST_IsEmpty(ST_Intersection(${relation}.${geom}, envelope.geom))
`))
    .on('row', row => {
      featureDump(row, tippecanoe, modify)
    })
    .on('error', err => {
      console.log(err.stack)
      client.release()
    })
    .on('end', () => {
      layerCount--
      client.release()
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
    if (Z < 6 && tippecanoe.minzoom >= 6) continue // safeguard
    dump(database, relation, geom, props, tippecanoe, modify)
  }
}

main()
