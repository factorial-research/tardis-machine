import { BookChange, Computable, DerivativeTicker, Exchange, NormalizedData, Trade, TradeBar, Writeable } from 'tardis-dev'

const DATE_MIN = new Date(-1)

type BarKind = 'time' | 'volume' | 'tick'

type DerivativeBarComputableOptions = { kind: BarKind; interval: number; name?: string }

type DerivativeBar = {
    type: 'derivative_bar'
    symbol: string
    exchange: Exchange
    name: string
    interval: number
    kind: BarKind
    openLastPrice: number | undefined
    openOi: number | undefined
    openFundingRate: number | undefined
    openIndexPrice: number | undefined
    openMarkPrice: number | undefined
    openTimestamp: Date
    closeLastPrice: number | undefined
    closeOi: number | undefined
    closeFundingRate: number | undefined
    closeIndexPrice: number | undefined
    closeMarkPrice: number | undefined
    closeTimestamp: Date
    deltaOi: number
    predictedFundingRate: number | undefined
    ticks: number
    fundingTimestamp: Date
    timestamp: Date
    localTimestamp: Date
}

export const computeDerivativeBars =
  (options: DerivativeBarComputableOptions): (() => Computable<DerivativeBar>) =>
  () =>
    new DerivativeBarComputable(options)

const kindSuffix: { [key in BarKind]: string } = {
  tick: 'ticks',
  time: 'ms',
  volume: 'vol'
}

class DerivativeBarComputable {
  // use book_change messages as workaround for issue when time passes for new bar to be produced but there's no trades,
  // so logic `compute` would not execute
  // assumption is that if one subscribes to book changes too then there's pretty good chance that
  // even if there are no trades, there's plenty of book changes that trigger computing new trade bar if time passess
  public readonly sourceDataTypes = ['derivative_ticker', 'book_change']

  private _inProgressBar: Writeable<DerivativeBar>
  private readonly _kind: BarKind
  private readonly _interval: number
  private readonly _name: string
  private readonly _type = 'derivative_bar'

  constructor({ kind, interval, name }: DerivativeBarComputableOptions) {
    this._kind = kind
    this._interval = interval

    if (name === undefined) {
      this._name = `${this._type}_${interval}${kindSuffix[kind]}`
    } else {
      this._name = name
    }

    this._inProgressBar = {} as any
    this._reset()
  }

  public *compute(message: DerivativeTicker | BookChange) {
    // first check if there is a new trade bar for new timestamp for time based trade bars
    if (this._hasNewBar(message.timestamp)) {
      yield this._computeBar(message)
    }

    if (message.type !== 'derivative_ticker') {
      return
    }

    // update in progress trade bar with new data
    this._update(message)

    // and check again if there is a new trade bar after the update (volume/tick based trade bars)
    if (this._hasNewBar(message.timestamp)) {
      yield this._computeBar(message)
    }
  }

  private _computeBar(message: NormalizedData) {
    this._inProgressBar.localTimestamp = message.localTimestamp
    this._inProgressBar.symbol = message.symbol
    this._inProgressBar.exchange = message.exchange

    const derivativeBar: DerivativeBar = { ...this._inProgressBar }

    this._reset()

    return derivativeBar
  }

  private _hasNewBar(timestamp: Date): boolean {
    // privided timestamp is an exchange trade timestamp in that case
    // we bucket based on exchange timestamps when bucketing by time not by localTimestamp
    if (this._inProgressBar.ticks === 0) {
      return false
    }

    if (this._kind === 'time') {
      const currentTimestampTimeBucket = this._getTimeBucket(timestamp)
      const openTimestampTimeBucket = this._getTimeBucket(this._inProgressBar.openTimestamp)
      if (currentTimestampTimeBucket > openTimestampTimeBucket) {
        // set the timestamp to the end of the period of given bucket
        this._inProgressBar.timestamp = new Date((openTimestampTimeBucket + 1) * this._interval)

        return true
      }

      return false
    }

    if (this._kind === 'volume') {
      return this._inProgressBar.deltaOi >= this._interval
    }

    if (this._kind === 'tick') {
      return this._inProgressBar.ticks >= this._interval
    }

    return false
  }

  private _update(derivativeTicker: DerivativeTicker) {
    const inProgressBar = this._inProgressBar
    const isNotOpenedYet = inProgressBar.ticks === 0

    if (isNotOpenedYet) {
      inProgressBar.openLastPrice = derivativeTicker.lastPrice
      inProgressBar.openOi = derivativeTicker.openInterest
      inProgressBar.openFundingRate = derivativeTicker.fundingRate
      inProgressBar.openIndexPrice = derivativeTicker.indexPrice
      inProgressBar.openMarkPrice = derivativeTicker.markPrice
      inProgressBar.openTimestamp = derivativeTicker.timestamp
    }

    inProgressBar.closeLastPrice = derivativeTicker.lastPrice
    inProgressBar.closeOi = derivativeTicker.openInterest
    inProgressBar.closeFundingRate = derivativeTicker.fundingRate
    inProgressBar.closeIndexPrice = derivativeTicker.indexPrice
    inProgressBar.closeMarkPrice = derivativeTicker.markPrice
    inProgressBar.closeTimestamp = derivativeTicker.timestamp

    inProgressBar.deltaOi = (inProgressBar.closeOi ?? 0) - (inProgressBar.openOi ?? 0)

    inProgressBar.predictedFundingRate = derivativeTicker.predictedFundingRate
    inProgressBar.ticks += 1
    inProgressBar.timestamp = derivativeTicker.timestamp
  }

  private _reset() {
    const barToReset = this._inProgressBar
    barToReset.type = this._type
    barToReset.symbol = ''
    barToReset.exchange = '' as any
    barToReset.name = this._name
    barToReset.interval = this._interval
    barToReset.kind = this._kind

    barToReset.openLastPrice = 0
    barToReset.openOi = undefined
    barToReset.openFundingRate = undefined
    barToReset.openIndexPrice = undefined
    barToReset.openMarkPrice = undefined
    barToReset.openTimestamp = DATE_MIN
    barToReset.closeLastPrice = undefined
    barToReset.closeOi = undefined
    barToReset.closeFundingRate = undefined
    barToReset.closeIndexPrice = undefined
    barToReset.closeMarkPrice = undefined
    barToReset.closeTimestamp = DATE_MIN
    barToReset.deltaOi = 0
    barToReset.predictedFundingRate = undefined
    barToReset.ticks = 0
    barToReset.timestamp = DATE_MIN    
  }

  private _getTimeBucket(timestamp: Date) {
    return Math.floor(timestamp.valueOf() / this._interval)
  }
}
