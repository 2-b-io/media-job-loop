import bodyParser from 'body-parser'
import express from 'express'

import initRoutes from './routes'

const app = express()

app.use(bodyParser.json())

initRoutes(app)

export default app
