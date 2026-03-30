const config = require('../config')
const Exchange = require('../exchange')

class Deribit extends Exchange {
  constructor() {
    super()

    this.id = 'DERIBIT'

    this.endpoints = {
      PRODUCTS: [
        'https://www.deribit.com/api/v2/public/get_instruments?currency=BTC',
        'https://www.deribit.com/api/v2/public/get_instruments?currency=ETH',
        'https://www.deribit.com/api/v2/public/get_instruments?currency=USDC'
      ]
    }

    this.url = 'wss://www.deribit.com/ws/api/v2'
  }

  formatProducts(response) {
    const products = []
    const types = {}

    for (const data of response) {
      for (const product of data.result) {
        if (!product.is_active) {
          continue
        }

        types[product.instrument_name] = product.future_type

        products.push(product.instrument_name)
      }
    }

    return {
      products,
      types
    }
  }

  supportsOpenInterest() {
    return true
  }

  supportsOrderBook() {
    return true
  }

  async fetchOpenInterest(pair) {
    const response = await this.fetchJson(
      `https://www.deribit.com/api/v2/public/get_book_summary_by_instrument?instrument_name=${encodeURIComponent(
        pair
      )}`
    )
    const summary = response.result && response.result[0]

    if (!summary) {
      return null
    }

    const openInterest = +summary.open_interest

    if (!isFinite(openInterest)) {
      return null
    }

    if (this.types[pair] === 'reversed') {
      return openInterest
    }

    const markPrice = +summary.mark_price

    if (!markPrice) {
      return null
    }

    return openInterest * markPrice
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

    if (api._connected.length === 1) {
      if (!config.deribitClientId) {
        throw new Error(
          'As of 15 Jan 2022 Deribit will no longer allow unauthenticated connections to subscribe to raw feeds\n\nAdd deribitClientId & deribitClientSecret to the config and restart server'
        )
      }

      api.send(
        JSON.stringify({
          jsonrpc: '2.0',
          method: 'public/auth',
          params: {
            grant_type: 'client_credentials',
            client_id: config.deribitClientId,
            client_secret: config.deribitClientSecret
          }
        })
      )
    }

    api.send(
      JSON.stringify({
        access_token: this.accessToken,
        method: 'public/subscribe',
        params: {
          channels: ['trades.' + pair + '.raw', 'book.' + pair + '.100ms']
        }
      })
    )
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

    api.send(
      JSON.stringify({
        access_token: this.accessToken,
        method: 'public/unsubscribe',
        params: {
          channels: ['trades.' + pair + '.raw', 'book.' + pair + '.100ms']
        }
      })
    )
  }

  formatTrade(trade) {
    let size = trade.amount

    if (this.types[trade.instrument_name] === 'reversed') {
      size /= trade.price
    }

    return {
      exchange: this.id,
      pair: trade.instrument_name,
      timestamp: +trade.timestamp,
      price: +trade.price,
      size: size,
      side: trade.direction,
      liquidation: trade.liquidation
    }
  }

  onMessage(event, api) {
    const json = JSON.parse(event.data)

    if (!json || !json.params || !json.params.data) {
      return
    }

    if (/^book\./.test(json.params.channel)) {
      return this.handleOrderBookMessage(json.params.data)
    }

    if (!json.params.data.length) {
      return
    }

    return this.emitTrades(
      api.id,
      json.params.data.map(a => this.formatTrade(a))
    )
  }

  handleOrderBookMessage(book) {
    if (!book || !book.instrument_name) {
      return
    }

    if (book.type === 'snapshot') {
      this.resetOrderBook(
        book.instrument_name,
        this.normalizeDeribitLevels(book.bids),
        this.normalizeDeribitLevels(book.asks),
        +book.timestamp,
        {
          changeId: +book.change_id
        }
      )
      return true
    }

    const orderBook = this.getOrderBook(book.instrument_name)

    if (
      typeof orderBook.changeId === 'number' &&
      typeof book.prev_change_id === 'number' &&
      book.prev_change_id !== orderBook.changeId
    ) {
      console.warn(
        `[${this.id}] depth sequence broke on ${book.instrument_name}, waiting for next snapshot`
      )
      orderBook.ready = false
      return true
    }

    this.applyOrderBookDelta(
      book.instrument_name,
      this.normalizeDeribitLevels(book.bids),
      this.normalizeDeribitLevels(book.asks),
      +book.timestamp,
      {
        changeId: +book.change_id
      }
    )

    return true
  }

  normalizeDeribitLevels(levels) {
    if (!Array.isArray(levels)) {
      return []
    }

    return levels.reduce((output, level) => {
      if (!Array.isArray(level)) {
        return output
      }

      if (typeof level[0] === 'string') {
        const action = level[0]
        const price = +level[1]
        const amount = action === 'delete' ? 0 : +level[2]

        output.push([price, amount])
      } else {
        output.push([+level[0], +level[1]])
      }

      return output
    }, [])
  }

  getOrderBookLevelNotional(pair, price, size) {
    return this.types[pair] === 'reversed' ? size : price * size
  }

  onApiCreated(api) {
    this.startKeepAlive(api, { method: 'public/ping' }, 45000)
  }

  onApiRemoved(api) {
    this.stopKeepAlive(api)
  }
}

module.exports = Deribit
