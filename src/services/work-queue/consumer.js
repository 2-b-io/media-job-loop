import delay from 'delay'
import ms from 'ms'

import Connection from './connection'

class Consumer extends Connection {
  constructor(props) {
    super({
      shortBreak: '1s',
      longBreak: '5s',
      ...props
    })

    this.idle = false
  }

  onReceive(cb) {
    this.state.cb = cb

    return this
  }

  async connect() {
    await super.connect()

    this.consume()
  }

  async consume() {
    try {
      const { channel, queue } = this.state

      const msg = await channel.get(queue, {
        noAck: false
      })

      if (!msg) {
        if (!this.idle) {
          console.log(`NO MORE MESSAGE, RECHECK EVERY ${ this.props.longBreak }...`)

          this.idle = true
        }

        await delay(ms(this.props.longBreak))

        this.consume()

        return
      }

      try {
        console.log('HANDLE MESSAGE...')

        this.idle = false
        const job = JSON.parse(msg.content.toString())

        await this.state.cb(job)

        await channel.ack(msg)
        console.log('MESSAGE ACKNOWLEDGED')

      } catch (e) {
        console.warn(e)

        // send to dead-letter exchange
        await channel.reject(msg, false)
      } finally {
        console.log(`WAIT ${ this.props.shortBreak } THEN CONTINUE...`)
        await delay(ms(this.props.shortBreak))

        this.consume()
      }
    } catch (e) {
      console.error(e)
    }
  }
}

export const createConsumer = (options) => {
  return new Consumer(options)
}
