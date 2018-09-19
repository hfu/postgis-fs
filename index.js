const tilebelt = require('@mapbox/tilebelt')
const { Pool, Query } = require('pg')
const wkx = require('wkx')

if (process.argv.length !== 6) {
  console.log('usage: node index.js {config.js} {z} {x} {y}')
  process.exit()
}
const config = require(process.argv[2])
const Z = Number(process.argv[3])
const X = Number(process.argv[4])
const Y = Number(process.argv[5])
const BBOX = tilebelt.tileToBBOX([X, Y, Z])
let layerCount = config.data.length

const featureDump = (row, geom, tippecanoe, modify) => {
  let f = {
    type: 'Feature',
    geometry: wkx.Geometry.parse(Buffer.from(row[geom], 'hex')).toGeoJSON(),
    tippecanoe: tippecanoe
  }
  delete row[geom]
  f.properties = row
  if (modify) { f = modify(f) }
  console.log(JSON.stringify(f))
}

const dump = async (database, relation, geom, props, tippecanoe, modify) => {
  let pool = new Pool({
    host: config.host,
    user: config.user,
    password: config.password,
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
  ST_Intersection(ST_MakeValid(${relation}.${geom}), envelope.geom) as geom 
FROM ${relation} JOIN envelope ON ${relation}.${geom} && envelope.geom
`))
    .on('row', row => {
      featureDump(row, geom, tippecanoe, modify)
    })
    .on('error', err => {
      console.log(err.stack)
      client.release()
    })
    .on('end', () => {
      layerCount--
      client.release()
      if (layerCount === 0) {
        console.log('end')
        process.exit()
      }
    })
}

const main = async () => {
  for (let datum of config.data) {
    const [database, relation] = datum[0].split('::')
    const geom = datum[1]
    const props = datum[2]
    const tippecanoe = datum[3]
    const modify = datum[4]
    if (Z < 6 && tippecanoe.minzoom >= 6) continue // safeguard
    dump(database, relation, geom, props, tippecanoe, modify)
  }
}

main()
