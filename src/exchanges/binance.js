const Exchange = require('../exchange')
const { sleep, getHms } = require('../helper')
const axios = require('axios')

class Binance extends Exchange {
  constructor() {
    super()

    this.id = 'BINANCE'
    this.lastSubscriptionId = 0
    this.maxConnectionsPerApi = 16
    this.subscriptions = {}

    this.endpoints = {
      PRODUCTS: 'https://data-api.binance.vision/api/v3/exchangeInfo'
    }

    this.url = () => 'wss://data-stream.binance.vision:9443/ws'
  }

  formatProducts(data) {
    return data.symbols.map(a => a.symbol.toLowerCase())
  }

  supportsOrderBook() {
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

    this.subscriptions[pair] = ++this.lastSubscriptionId

    const params = [pair + '@trade', pair + '@depth@100ms']

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
    await sleep(250 * this.apis.length)
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

    const params = [pair + '@trade', pair + '@depth@100ms']

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
    await sleep(250 * this.apis.length)
  }

  onMessage(event, api) {
    const json = JSON.parse(event.data)

    if (json.e === 'depthUpdate') {
      return this.handleOrderBookUpdate(api, json)
    }

    if (json.E) {
      return this.emitTrades(api.id, [
        this.formatTrade(json, json.s.toLowerCase())
      ])
    }
  }

  formatTrade(trade, symbol) {
    return {
      exchange: this.id,
      pair: symbol,
      timestamp: trade.T,
      price: +trade.p,
      size: +trade.q,
      side: trade.m ? 'sell' : 'buy'
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
      const response = await this.fetchJson(
        `https://data-api.binance.vision/api/v3/depth?symbol=${pair.toUpperCase()}&limit=5000`
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
    } else if (firstUpdateId > expectedNextUpdateId) {
      return this.rebuildOrderBook(api, pair, 'gap detected')
    }

    this.applyOrderBookDelta(pair, update.b, update.a, update.E, {
      lastUpdateId,
      synced: true
    })

    return true
  }

  onApiCreated(api) {
    api._orderBookBuffers = {}
    api._orderBookInitTokens = {}
  }

  getMissingTrades(range, totalRecovered = 0) {
    const startTime = range.from
    const below1HEndTime = Math.min(range.to, startTime + 1000 * 60 * 60)

    const endpoint = `https://data-api.binance.vision/api/v3/aggTrades?symbol=${range.pair.toUpperCase()}&startTime=${
      startTime + 1
    }&endTime=${below1HEndTime}&limit=1000`

    return axios
      .get(endpoint)
      .then(response => {
        if (response.data.length) {
          const trades = response.data.map(trade => ({
            ...this.formatTrade(trade, range.pair),
            count: trade.l - trade.f + 1,
            timestamp: trade.T
          }))

          this.emitTrades(null, trades)

          totalRecovered += trades.length
          range.from = trades[trades.length - 1].timestamp

          const remainingMissingTime = range.to - range.from

          if (remainingMissingTime > 1000) {
            console.log(
              `[${this.id}.recoverMissingTrades] +${trades.length} ${
                range.pair
              } ... (${getHms(remainingMissingTime)} remaining)`
            )
            return this.waitBeforeContinueRecovery().then(() =>
              this.getMissingTrades(range, totalRecovered)
            )
          } else {
            console.log(
              `[${this.id}.recoverMissingTrades] +${trades.length} ${
                range.pair
              } (${getHms(remainingMissingTime)} remaining)`
            )
          }
        }

        return totalRecovered
      })
      .catch(err => {
        console.error(
          `Failed to get historical trades on ${range.pair}`,
          err.message
        )
      })
  }
}

module.exports = Binance
