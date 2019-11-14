import { IncomingMessage } from 'http'
import stream from 'stream'
import { combine, compute, replayNormalized } from 'tardis-dev'
import url from 'url'
import { promisify } from 'util'
import WebSocket from 'ws'
import { debug } from '../debug'
import { constructDataTypeFilter, FilterAndStringifyStream, getComputables, getNormalizers, ReplayNormalizedOptions } from '../helpers'

const pipeline = promisify(stream.pipeline)

export async function replayNormalizedWS(ws: WebSocket, req: IncomingMessage) {
  try {
    const startTimestamp = new Date().getTime()
    const parsedQuery = url.parse(req.url!, true).query
    const optionsString = parsedQuery['options'] as string
    const replayNormalizedOptions = JSON.parse(optionsString) as ReplayNormalizedOptions

    debug('WebSocket /replay-normalized started, options: %o, include local timestamps', replayNormalizedOptions)

    const options = Array.isArray(replayNormalizedOptions) ? replayNormalizedOptions : [replayNormalizedOptions]

    const messagesIterables = options.map(option => {
      // let's map from provided options to options and normalizers that needs to be added for dataTypes provided in options
      const messages = replayNormalized(option, ...getNormalizers(option.dataTypes))
      // separately check if any computables are needed for given dataTypes
      const computables = getComputables(option.dataTypes)

      if (computables.length > 0) {
        return compute(messages, ...computables)
      }

      return messages
    })

    const filterByDataType = constructDataTypeFilter(options)
    const messages = messagesIterables.length === 1 ? messagesIterables[0] : combine(...messagesIterables)

    // pipe replayed messages through transform stream that filters those if needed and stringifies and then finally trough WebSocket stream
    const webSocketStream = (WebSocket as any).createWebSocketStream(ws, { decodeStrings: false })

    await pipeline(stream.Readable.from(messages), new FilterAndStringifyStream(filterByDataType), webSocketStream)

    const endTimestamp = new Date().getTime()

    debug(
      'WebSocket /replay-normalized finished, options: %o, time: %d seconds',
      replayNormalizedOptions,
      (endTimestamp - startTimestamp) / 1000
    )
  } catch (e) {
    ws.close(1011, e.toString())
    debug('WebSocket /replay-normalized  error: %o', e)
    console.error('WebSocket /replay-normalized error:', e)
  }
}