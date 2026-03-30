const Exchange = require('../exchange')
const { sleep } = require('../helper')

const SPOT_PAIR_REGEX = /-SPOT$/

class Bitget extends Exchange {
  constructor() {
    super()

    this.id = 'BITGET'
    this.maxConnectionsPerApi = 50

    this.endpoints = {
      PRODUCTS: [
        'https://api.bitget.com/api/v3/market/instruments?category=SPOT',
        'https://api.bitget.com/api/v3/market/instruments?category=USDT-FUTURES',
        'https://api.bitget.com/api/v3/market/instruments?category=COIN-FUTURES',
        'https://api.bitget.com/api/v3/market/instruments?category=USDC-FUTURES'
      ]
    }

    // UTA websocket public channels (publicTrade + liquidation)
    this.url = () => 'wss://ws.bitget.com/v3/ws/public'
  }

  formatProducts(responses) {
    const types = {}

    const categories = ['spot', 'usdt-futures', 'coin-futures', 'usdc-futures']

    for (let i = 0; i < responses.length; i++) {
      const response = responses[i]
      const type = categories[i]

      for (const product of response.data) {
        const symbol = this.formatRemoteToLocalPair(product.symbol, type)

        types[symbol] = type
      }
    }

    const products = Object.keys(types)

    return {
      products,
      types
    }
  }

  supportsOpenInterest(pair) {
    return !!this.types[pair] && this.types[pair] !== 'spot'
  }

  supportsOrderBook(pair) {
    return !!this.types[pair]
  }

  async fetchOpenInterests(pairs) {
    const openInterests = {}
    const pairsByType = pairs.reduce((output, pair) => {
      const type = this.types[pair]

      if (!type || type === 'spot') {
        return output
      }

      if (!output[type]) {
        output[type] = []
      }

      output[type].push(pair)

      return output
    }, {})

    await Promise.all(
      Object.keys(pairsByType).map(async type => {
        const response = await this.fetchJson(
          `https://api.bitget.com/api/v3/market/tickers?category=${type.toUpperCase()}`
        )
        const tickers = response.data || []
        const byPair = tickers.reduce((output, ticker) => {
          output[this.formatRemoteToLocalPair(ticker.symbol, type)] = ticker
          return output
        }, {})

        for (const pair of pairsByType[type]) {
          const ticker = byPair[pair]

          if (!ticker) {
            continue
          }

          const value = +ticker.openInterest
          const price = +ticker.markPrice || +ticker.lastPrice

          if (!isFinite(value) || !price) {
            continue
          }

          openInterests[pair] = value * price
        }
      })
    )

    return openInterests
  }

  getLiquidationArgs() {
    return ['usdt-futures', 'coin-futures', 'usdc-futures'].map(instType => ({
      instType,
      topic: 'liquidation'
    }))
  }

  formatRemoteToLocalPair(pair, type) {
    return type === 'spot'
      ? `${pair}-SPOT`
      : type === 'coin-futures'
        ? pair.replace(/_CM$/, '')
        : pair
  }

  formatLocalToRemotePair(pair, type) {
    return type === 'spot'
      ? pair.replace(SPOT_PAIR_REGEX, '')
      : type === 'coin-futures'
        ? `${pair}_CM`
        : pair
  }

  hasConnectedFuturesPairs(api) {
    return api._connected.some(pair => this.types[pair] && this.types[pair] !== 'spot')
  }

  getOrderBookArgs(pair) {
    const type = this.types[pair].toLowerCase()

    return {
      instType: type,
      topic: 'books',
      symbol: this.formatLocalToRemotePair(pair, type)
    }
  }

  isPairTrackedByApi(api, pair) {
    return api._connected.indexOf(pair) !== -1 || api._pending.indexOf(pair) !== -1
  }

  invalidateOrderBookState(api, pair) {
    this.clearOrderBook(pair)

    if (api._orderBookRebuilds) {
      delete api._orderBookRebuilds[pair]
    }
  }

  scheduleOrderBookRebuild(api, pair, reason) {
    if (
      !api ||
      !this.supportsOrderBook(pair) ||
      !this.isPairTrackedByApi(api, pair)
    ) {
      return false
    }

    if (api._orderBookRebuilds[pair]) {
      return false
    }

    api._orderBookRebuilds[pair] = true
    this.clearOrderBook(pair)

    console.warn(
      `[${this.id}] depth sequence broke on ${pair} (${reason}), rebuilding local book`
    )

    const args = [this.getOrderBookArgs(pair)]

    ;(async () => {
      try {
        if (api.readyState === 1) {
          api.send(
            JSON.stringify({
              op: 'unsubscribe',
              args
            })
          )
        }

        await sleep(150)

        if (api.readyState === 1 && this.isPairTrackedByApi(api, pair)) {
          api.send(
            JSON.stringify({
              op: 'subscribe',
              args
            })
          )
        }

        await sleep(300)
      } catch (error) {
        console.error(
          `[${this.id}] failed to rebuild order book on ${pair}`,
          error.message
        )
      } finally {
        delete api._orderBookRebuilds[pair]
      }
    })()

    return true
  }

  /**
   * Sub
   * @param {WebSocket} api
   * @param {string} pair
   */
  async subscribe(api, pair) {
    if (!(await super.subscribe.apply(this, arguments))) {
      return
    }

    const type = this.types[pair].toLowerCase()
    const isSpot = type === 'spot'
    const wsSymbol = this.formatLocalToRemotePair(pair, type)
    const orderBookArgs = this.getOrderBookArgs(pair)

    api.send(
      JSON.stringify({
        op: 'subscribe',
        args: [
          {
            instType: type,
            topic: 'publicTrade',
            symbol: wsSymbol
          },
          {
            ...orderBookArgs
          }
        ]
      })
    )

    if (!isSpot && !api._liquidationSubscribed) {
      const payload = {
        op: 'subscribe',
        args: this.getLiquidationArgs()
      }
      api.send(
        JSON.stringify(payload)
      )

      api._liquidationSubscribed = true
    }

    // this websocket api have a limit of about 10 messages per second.
    await sleep(150)
  }

  /**
   * Unsub
   * @param {WebSocket} api
   * @param {string} pair
   */
  async unsubscribe(api, pair) {
    if (!(await super.unsubscribe.apply(this, arguments))) {
      return
    }

    const type = this.types[pair].toLowerCase()
    const wsSymbol = this.formatLocalToRemotePair(pair, type)
    const orderBookArgs = this.getOrderBookArgs(pair)

    api.send(
      JSON.stringify({
        op: 'unsubscribe',
        args: [
          {
            instType: type,
            topic: 'publicTrade',
            symbol: wsSymbol
          },
          {
            ...orderBookArgs
          }
        ]
      })
    )

    if (api._liquidationSubscribed && !this.hasConnectedFuturesPairs(api)) {
      api.send(
        JSON.stringify({
          op: 'unsubscribe',
          args: this.getLiquidationArgs()
        })
      )

      api._liquidationSubscribed = false
    }

    // this websocket api have a limit of about 10 messages per second.
    await sleep(150)

    this.invalidateOrderBookState(api, pair)
  }

  formatTrade(trade, pair, instType) {
    // UTA publicTrade format: { p, v, S, T }
    const price = trade.p !== undefined ? trade.p : trade.px !== undefined ? trade.px : trade.price
    let size = trade.v !== undefined ? trade.v : trade.sz !== undefined ? trade.sz : trade.size
    const side = trade.S !== undefined ? trade.S : trade.side
    const timestamp = trade.T !== undefined ? trade.T : trade.ts
    const numericPrice = +price

    // UTA coin-futures publicTrade uses quote-value contracts; convert to base size.
    if (instType === 'coin-futures' && numericPrice > 0) {
      size = +size / numericPrice
    }

    return {
      exchange: this.id,
      pair,
      timestamp: +timestamp,
      price: numericPrice,
      size: +size,
      side: typeof side === 'string' ? side.toLowerCase() : side
    }
  }

  formatLiquidation(liquidation, pair) {
    const price = +liquidation.price
    // Liquidation amount unit is quote coin; convert to base size.
    const size = price > 0 ? +liquidation.amount / price : +liquidation.amount

    return {
      exchange: this.id,
      pair,
      timestamp: +liquidation.ts,
      price,
      size,
      side: liquidation.side === 'buy' ? 'sell' : 'buy',
      liquidation: true
    }
  }

  onMessage(event, api) {
    if (event.data === 'pong') {
      return
    }

    const json = JSON.parse(event.data)

    if (json.event === 'error') {
      console.error(
        `[${this.id}] websocket error`,
        json.code || '',
        json.msg || ''
      )
      return
    }

    if (!json.arg || !json.data || !json.data.length) {
      return
    }

    if (json.arg.topic === 'publicTrade') {
      // Ignore historical trades sent after subscription.
      if (json.action !== 'update') {
        return
      }

      const wsInstType = json.arg.instType
      const wsSymbol = json.arg.symbol
      const pair = this.formatRemoteToLocalPair(wsSymbol, wsInstType)

      return this.emitTrades(
        api.id,
        json.data.map(trade => this.formatTrade(trade, pair, wsInstType))
      )
    }

    if (json.arg.topic === 'books') {
      return this.handleOrderBookMessage(json, api)
    }

    if (json.arg.topic === 'liquidation') {
      const wsInstType = json.arg.instType
      const liquidations = []

      for (const liquidation of json.data) {
        const pair = this.formatRemoteToLocalPair(liquidation.symbol, wsInstType)

        if (api._connected.indexOf(pair) === -1) {
          continue
        }

        liquidations.push(this.formatLiquidation(liquidation, pair))
      }

      if (liquidations.length) {
        return this.emitLiquidations(api.id, liquidations)
      }
    }
  }

  handleOrderBookMessage(message, api) {
    const data = message.data && message.data[0]

    if (!data) {
      return
    }

    const pair = this.formatRemoteToLocalPair(
      message.arg.symbol || data.symbol,
      message.arg.instType
    )
    const bids = data.bids || data.b || []
    const asks = data.asks || data.a || []
    const timestamp = +(data.ts || Date.now())
    const sequence = +data.seq
    const previousSequence = +data.pseq
    const hasSequence = isFinite(sequence)
    const hasPreviousSequence = isFinite(previousSequence)

    if (message.action === 'snapshot') {
      this.resetOrderBook(pair, bids, asks, timestamp, {
        sequence: hasSequence ? sequence : undefined,
        sequenceSynced: false
      })

      return true
    }

    const orderBook = this.getOrderBook(pair)
    const lastSequence = +orderBook.sequence

    if (!orderBook.ready || !isFinite(lastSequence)) {
      return this.scheduleOrderBookRebuild(api, pair, 'missing snapshot')
    }

    if (!hasSequence || !hasPreviousSequence) {
      return this.scheduleOrderBookRebuild(api, pair, 'missing sequence')
    }

    if (sequence <= lastSequence) {
      return false
    }

    if (orderBook.sequenceSynced === false) {
      if (
        previousSequence === 0 ||
        previousSequence > lastSequence ||
        lastSequence > sequence
      ) {
        return this.scheduleOrderBookRebuild(api, pair, 'gap detected')
      }
    } else {
      if (previousSequence === 0) {
        return this.scheduleOrderBookRebuild(api, pair, 'sequence reset')
      }

      if (previousSequence !== lastSequence) {
        return this.scheduleOrderBookRebuild(api, pair, 'gap detected')
      }
    }

    this.applyOrderBookDelta(pair, bids, asks, timestamp, {
      sequence,
      sequenceSynced: true
    })

    return true
  }

  getOrderBookLevelNotional(pair, price, size) {
    return this.types[pair] === 'coin-futures' ? size : price * size
  }

  onApiCreated(api) {
    api._liquidationSubscribed = false
    api._orderBookRebuilds = {}
    this.startKeepAlive(api, 'ping', 30000)
  }

  onApiRemoved(api) {
    this.stopKeepAlive(api)
  }
}

module.exports = Bitget
