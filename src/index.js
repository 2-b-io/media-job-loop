import asArray from 'as-array'

import api from 'services/api'
import config from 'infrastructure/config'
import { createConsumer } from 'services/work-queue/consumer'
import { createProducer } from 'services/work-queue/producer'

import * as handlers from './handlers'

let idleTime = new Date().toISOString()
let workingTime = new Date().toISOString()
let stateWorker = 'init'

const HANDLERS = {
  'CHECK_INFRASTRUCTURE': handlers.checkInfrastructure,
  'CREATE_INFRASTRUCTURE': handlers.createInfrastructure,
  'UPDATE_INFRASTRUCTURE': handlers.updateInfrastructure,

  'CHECK_INVALIDATION': handlers.checkInvalidation,
  'CREATE_INVALIDATION': handlers.createInvalidation,

  'SEND_EMAIL': handlers.sendEmail,

  'GET_METRIC_DATA': handlers.getMetricData,

  'PRUNE_ES': handlers.pruneEs,
  'SYNC_S3_TO_ES': handlers.syncS3ToEs

}

const handleJob = async (job) => {
  const handler = HANDLERS[ job.name ]

  if (!handler || typeof handler !== 'function') {
    // return job untouched
    return job
  }

  if (stateWorker !== 'working') {
    console.log('JOB WORKING')

    workingTime = new Date().toISOString()

    await api.call(
      'post',
      `/jobs/logs`,
      {
        time: new Date().toISOString(),
        state: 'working',
        lastState: stateWorker,
        lastTime: idleTime
      }
    )

    stateWorker = 'working'
  }


  return await handler(job)
}

const main = async () => {
  const consumer = createConsumer({
    host: config.amq.host,
    queue: config.amq.queue,
    prefix: config.amq.prefix,
    shortBreak: config.pulling.shortBreak,
    longBreak: config.pulling.longBreak
  })

  const producer = createProducer({
    host: config.amq.host,
    queue: config.amq.queue,
    prefix: config.amq.prefix,
  })

  const sendJobs = async (jobs) => {
    await Promise.all(
      jobs.map(
        (job) => producer.send(job)
      )
    )
  }

  consumer.onReceive(async (job) => {
    console.log(`RECEIVED JOB [${ job.name }] AT: ${ new Date().toISOString() }, SCHEDULED WHEN: ${ new Date(job.when).toISOString() } `)

    const nextJobs = asArray(
      job.when > Date.now() ?
        job : (await handleJob(job))
    )

    if (nextJobs && nextJobs.length) {
      await sendJobs(nextJobs)

      if (stateWorker !== 'idle') {
        console.log('JOB IDLE')

        idleTime = new Date().toISOString()

        await api.call(
          'post',
          `/jobs/logs`,
          {
            time: new Date().toISOString(),
            state: 'idle',
            lastState: stateWorker,
            lastTime: workingTime
          }
        )

        stateWorker = 'idle'
      }
    }
  })

  await Promise.all([
    producer.connect(),
    consumer.connect()
  ])

  console.log('WORKER BOOTSTRAPPED!')
}

main()
