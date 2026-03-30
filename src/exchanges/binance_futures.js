const Exchange = require('../exchange')
const { sleep, getHms } = require('../helper')

class BinanceFutures extends Exchange {
  constructor() {
    super()

    this.id = 'BINANCE_FUTURES'
    this.lastSubscriptionId = 0
    this.subscriptions = {}
    this.restBlockedUntil = 0

    this.maxConnectionsPerApi = 100
    this.endpoints = {
      PRODUCTS: [
        'https://fapi.binance.com/fapi/v1/exchangeInfo',
        'https://dapi.binance.com/dapi/v1/exchangeInfo'
      ]
    }

    this.url = pair => {
      if (this.dapi[pair]) {
        return 'wss://dstream.binance.com/ws'
      } else {
        return 'wss://fstream.binance.com/ws'
      }
    }
  }

  formatProducts(response) {
    const products = []
    const specs = {}
    const dapi = {}

    for (const data of response) {
      const type = ['fapi', 'dapi'][response.indexOf(data)]

      for (const product of data.symbols) {
        if (
          (product.contractStatus && product.contractStatus !== 'TRADING') ||
          (product.status && product.status !== 'TRADING')
        ) {
          continue
        }

        const symbol = product.symbol.toLowerCase()

        if (type === 'dapi') {
          dapi[symbol] = true
        }

        if (product.contractSize) {
          specs[symbol] = product.contractSize
        }

        products.push(symbol)
      }
    }

    return {
      products,
      specs,
      dapi
    }
  }

  supportsOpenInterest() {
    return true
  }

  supportsOrderBook(pair) {
    return this.supportsOpenInterest(pair)
  }

  getRestBackoffDuration(error) {
    const status = error?.response?.status
    const retryAfterHeader =
      error?.response?.headers?.['retry-after'] ||
      error?.response?.headers?.['Retry-After']
    const retryAfterSeconds = +retryAfterHeader

    if (isFinite(retryAfterSeconds) && retryAfterSeconds > 0) {
      return retryAfterSeconds * 1000
    }

    if (status === 418) {
      return 5 * 60 * 1000
    }

    if (status === 429) {
      return 60 * 1000
    }

    return 0
  }

  async fetchBinanceJson(endpoint) {
    const now = Date.now()

    if (this.restBlockedUntil > now) {
      throw new Error(
        `Binance REST temporarily blocked for ${getHms(
          this.restBlockedUntil - now
        )}`
      )
    }

    try {
      return await this.fetchJson(endpoint)
    } catch (error) {
      const status = error?.response?.status
      const backoffDuration = this.getRestBackoffDuration(error)

      if (backoffDuration > 0) {
        const blockedUntil = Date.now() + backoffDuration

        if (blockedUntil > this.restBlockedUntil) {
          this.restBlockedUntil = blockedUntil
          console.warn(
            `[${this.id}] Binance REST returned ${status}, backing off for ${getHms(
              backoffDuration
            )}`
          )
        }
      }

      throw error
    }
  }

  async fetchOpenInterest(pair) {
    const isDapi = !!this.dapi[pair]
    const openInterestResponse = await this.fetchBinanceJson(
      isDapi
        ? `https://dapi.binance.com/dapi/v1/openInterest?symbol=${pair.toUpperCase()}`
        : `https://fapi.binance.com/fapi/v1/openInterest?symbol=${pair.toUpperCase()}`
    )
    const openInterest = +openInterestResponse.openInterest

    if (!isFinite(openInterest)) {
      return null
    }

    if (isDapi) {
      if (!this.specs[pair]) {
        return null
      }

      return openInterest * this.specs[pair]
    }

    const orderBookMid = this.getOrderBook(pair).mid
    let price = orderBookMid

    if (!isFinite(price) || price <= 0) {
      price = +(await this.fetchBinanceJson(
        `https://fapi.binance.com/fapi/v1/ticker/price?symbol=${pair.toUpperCase()}`
      )).price
    }

    if (!price) {
      return null
    }

    return openInterest * price
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

    this.subscriptions[pair] = ++this.lastSubscriptionId

    const params = [pair + '@trade', pair + '@forceOrder', pair + '@depth@100ms']

    api.send(
      JSON.stringify({
        method: 'SUBSCRIBE',
        params,
        id: this.subscriptions[pair]
      })
    )

    this.initializeOrderBook(api, pair).catch(error => {
      console.error(
        `[${this.id}] failed to initialize order book on ${pair}`,
        error.message
      )
    })

    // this websocket api have a limit of about 5 messages per second.
    await sleep(500 * this.apis.length)
  }

  /**
   * Unsub
   * @param {WebSocket} api
   * @param {string} pair
   */
  async unsubscribe(api, pair) {
    if (!(await super.unsubscribe.apply(this, arguments))) {
      delete this.subscriptions[pair]
      return
    }

    const params = [pair + '@trade', pair + '@forceOrder', pair + '@depth@100ms']

    api.send(
      JSON.stringify({
        method: 'UNSUBSCRIBE',
        params,
        id: this.subscriptions[pair]
      })
    )

    delete this.subscriptions[pair]
    this.invalidateOrderBookInitialization(api, pair)

    // this websocket api have a limit of about 5 messages per second.
    await sleep(500 * this.apis.length)
  }

  onMessage(event, api) {
    const json = JSON.parse(event.data)

    if (!json) {
      return
    } else if (json.e === 'depthUpdate') {
      return this.handleOrderBookUpdate(api, json)
    } else if (json.T && (!json.X || json.X === 'MARKET')) {
      return this.emitTrades(api.id, [
        this.formatTrade(json, json.s.toLowerCase())
      ])
    } else if (json.e === 'forceOrder') {
      return this.emitLiquidations(api.id, [this.formatLiquidation(json)])
    }
  }

  getSize(qty, price, symbol) {
    let size = +qty

    if (typeof this.specs[symbol] === 'number') {
      size = (size * this.specs[symbol]) / price
    }

    return size
  }

  /**
   *
   * @param {} trade
   * @param {*} symbol
   * @return {Trade}
   */
  formatTrade(trade, symbol) {
    return {
      exchange: this.id,
      pair: symbol,
      timestamp: trade.T,
      price: +trade.p,
      size: this.getSize(trade.q, trade.p, symbol),
      side: trade.m ? 'sell' : 'buy'
    }
  }

  formatLiquidation(trade) {
    const symbol = trade.o.s.toLowerCase()

    return {
      exchange: this.id,
      pair: symbol,
      timestamp: trade.o.T,
      price: +trade.o.p,
      size: this.getSize(trade.o.q, trade.o.p, symbol),
      side: trade.o.S === 'BUY' ? 'buy' : 'sell',
      liquidation: true
    }
  }

  invalidateOrderBookInitialization(api, pair) {
    api._orderBookInitTokens[pair] = (api._orderBookInitTokens[pair] || 0) + 1
    delete api._orderBookBuffers[pair]
  }

  rebuildOrderBook(api, pair, reason) {
    const orderBook = this.getOrderBook(pair)

    if (orderBook.initializing) {
      return true
    }

    console.warn(
      `[${this.id}] depth sequence broke on ${pair}${
        reason ? ` (${reason})` : ''
      }, rebuilding local book`
    )

    this.initializeOrderBook(api, pair, { resetBuffer: true }).catch(error => {
      console.error(
        `[${this.id}] failed to rebuild order book on ${pair}`,
        error.message
      )
    })

    return true
  }

  async initializeOrderBook(api, pair, options = {}) {
    if (!this.supportsOrderBook(pair)) {
      return
    }

    const { resetBuffer = false } = options
    const orderBook = this.getOrderBook(pair)

    if (orderBook.initializing) {
      return
    }

    const initToken = (api._orderBookInitTokens[pair] || 0) + 1

    api._orderBookInitTokens[pair] = initToken
    orderBook.initializing = true
    orderBook.synced = false
    orderBook.ready = false

    if (resetBuffer || !Array.isArray(api._orderBookBuffers[pair])) {
      api._orderBookBuffers[pair] = []
    }

    try {
      const response = await this.fetchBinanceJson(
        this.dapi[pair]
          ? `https://dapi.binance.com/dapi/v1/depth?symbol=${pair.toUpperCase()}&limit=1000`
          : `https://fapi.binance.com/fapi/v1/depth?symbol=${pair.toUpperCase()}&limit=1000`
      )

      if (api._orderBookInitTokens[pair] !== initToken) {
        return
      }

      this.resetOrderBook(pair, response.bids, response.asks, Date.now(), {
        lastUpdateId: +response.lastUpdateId,
        synced: false
      })

      this.getOrderBook(pair).initializing = false

      const bufferedUpdates = (api._orderBookBuffers[pair] || []).sort(
        (a, b) => a.U - b.U
      )

      api._orderBookBuffers[pair] = []

      for (const update of bufferedUpdates) {
        this.handleOrderBookUpdate(api, update)
      }
    } finally {
      if (api._orderBookInitTokens[pair] === initToken && this.orderBooks[pair]) {
        this.orderBooks[pair].initializing = false
      }
    }
  }

  handleOrderBookUpdate(api, update) {
    const pair = update.s.toLowerCase()
    const orderBook = this.getOrderBook(pair)
    const firstUpdateId = +update.U
    const lastUpdateId = +update.u
    const previousLastUpdateId = +update.pu

    if (orderBook.initializing || typeof orderBook.lastUpdateId !== 'number') {
      api._orderBookBuffers[pair] = api._orderBookBuffers[pair] || []
      api._orderBookBuffers[pair].push(update)
      return true
    }

    if (!isFinite(firstUpdateId) || !isFinite(lastUpdateId)) {
      return true
    }

    if (lastUpdateId <= orderBook.lastUpdateId) {
      return true
    }

    const expectedNextUpdateId = orderBook.lastUpdateId + 1

    if (!orderBook.synced) {
      if (
        firstUpdateId > expectedNextUpdateId ||
        lastUpdateId < expectedNextUpdateId
      ) {
        return this.rebuildOrderBook(api, pair, 'snapshot bridge missing')
      }
    } else {
      if (isFinite(previousLastUpdateId)) {
        if (previousLastUpdateId !== orderBook.lastUpdateId) {
          return this.rebuildOrderBook(api, pair, 'pu mismatch')
        }
      } else if (firstUpdateId > expectedNextUpdateId) {
        return this.rebuildOrderBook(api, pair, 'gap detected')
      }
    }

    this.applyOrderBookDelta(pair, update.b, update.a, update.E, {
      lastUpdateId,
      synced: true
    })

    return true
  }

  getOrderBookLevelNotional(pair, price, size) {
    return this.getSize(size, price, pair) * price
  }

  onApiCreated(api) {
    api._orderBookBuffers = {}
    api._orderBookInitTokens = {}
  }

  getMissingTrades(range, totalRecovered = 0) {
    const startTime = range.from
    let endpoint = `?symbol=${range.pair.toUpperCase()}&startTime=${startTime + 1
    }&endTime=${range.to}&limit=1000`
    if (this.dapi[range.pair]) {
      endpoint = 'https://dapi.binance.com/dapi/v1/aggTrades' + endpoint
    } else {
      endpoint = 'https://fapi.binance.com/fapi/v1/aggTrades' + endpoint
    }

    return this.fetchBinanceJson(endpoint)
      .then(response => {
        if (response.length) {
          const trades = response
            .filter(trade => trade.T > range.from && trade.T < range.to)
            .map(trade => ({
              ...this.formatTrade(trade, range.pair),
              count: trade.l - trade.f + 1
            }))

          if (trades.length) {
            this.emitTrades(null, trades)

            totalRecovered += trades.length
            range.from = trades[trades.length - 1].timestamp
          }

          const remainingMissingTime = range.to - range.from

          if (trades.length) {
            console.log(
              `[${this.id}.recoverMissingTrades] +${trades.length} ${range.pair
              } ... but theres more (${getHms(remainingMissingTime)} remaining)`
            )

            return this.waitBeforeContinueRecovery().then(() =>
              this.getMissingTrades(range, totalRecovered)
            )
          } else {
            console.log(
              `[${this.id}.recoverMissingTrades] +${trades.length} ${range.pair
              } (${getHms(remainingMissingTime)} remaining)`
            )
          }
        }

        return totalRecovered
      })
      .catch(err => {
        console.error(
          `[${this.id}] failed to get missing trades on ${range.pair}`,
          err.message
        )

        return totalRecovered
      })
  }
}

module.exports = BinanceFutures
