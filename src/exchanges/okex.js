const Exchange = require('../exchange')
const axios = require('axios')
const { getHms, sleep } = require('../helper')

class Okex extends Exchange {
  constructor() {
    super()

    this.id = 'OKEX'

    this.endpoints = {
      LIQUIDATIONS: 'https://www.okx.com/api/v5/public/liquidation-orders',
      PRODUCTS: [
        'https://www.okx.com/api/v5/public/instruments?instType=SPOT',
        'https://www.okx.com/api/v5/public/instruments?instType=FUTURES',
        'https://www.okx.com/api/v5/public/instruments?instType=SWAP'
      ]
    }

    this.liquidationProducts = []
    this.liquidationProductsReferences = {}

    this.url = 'wss://ws.okx.com:8443/ws/v5/public'
  }

  formatProducts(response) {
    const products = []
    const specs = {}
    const aliases = {}
    const types = {}
    const inversed = {}

    for (let data of response) {
      for (let product of data.data) {
        const type = product.instType
        const pair = product.instId

        if (type === 'FUTURES') {
          // futures

          specs[pair] = +product.ctVal
          aliases[pair] = product.alias

          if (product.ctType === 'inverse') {
            inversed[pair] = true
          }
        } else if (type === 'SWAP') {
          // swap

          specs[pair] = +product.ctVal

          if (product.ctType === 'inverse') {
            inversed[pair] = true
          }
        }

        types[pair] = type
        products.push(pair)
      }
    }

    return {
      products,
      specs,
      aliases,
      types,
      inversed
    }
  }

  supportsOpenInterest(pair) {
    return !!this.types[pair] && this.types[pair] !== 'SPOT'
  }

  supportsOrderBook(pair) {
    return !!this.types[pair]
  }

  getOrderBookArgs(pair) {
    return {
      channel: 'books',
      instId: pair
    }
  }

  isPairTrackedByApi(api, pair) {
    return api._connected.indexOf(pair) !== -1 || api._pending.indexOf(pair) !== -1
  }

  async fetchOpenInterests(pairs) {
    const openInterests = {}
    const pairsByType = pairs.reduce((output, pair) => {
      const type = this.types[pair]

      if (!type || type === 'SPOT') {
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
          `https://www.okx.com/api/v5/public/open-interest?instType=${type}`
        )
        const byPair = ((response && response.data) || []).reduce(
          (output, ticker) => {
            output[ticker.instId] = ticker
            return output
          },
          {}
        )

        for (const pair of pairsByType[type]) {
          const ticker = byPair[pair]

          if (!ticker || !ticker.oiUsd) {
            continue
          }

          const value = +ticker.oiUsd

          if (!isFinite(value)) {
            continue
          }

          openInterests[pair] = value
        }
      })
    )

    return openInterests
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

    api.send(
      JSON.stringify({
        op: 'subscribe',
        args: [
          {
            channel: 'trades',
            instId: pair
          },
          {
            channel: 'books',
            instId: pair
          }
        ]
      })
    )

    if (this.types[pair] !== 'SPOT') {
      api.send(
        JSON.stringify({
          op: 'subscribe',
          args: [
            {
              channel: 'liquidation-orders',
              instType: this.types[pair]
            }
          ]
        })
      )
    }

    this.initializeOrderBook(api, pair, {
      resetBuffer: true,
      clearBook: true
    })
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
        op: 'unsubscribe',
        args: [
          {
            channel: 'trades',
            instId: pair
          },
          {
            channel: 'books',
            instId: pair
          }
        ]
      })
    )

    if (this.types[pair] !== 'SPOT') {
      api.send(
        JSON.stringify({
          op: 'unsubscribe',
          args: [
            {
              channel: 'liquidation-orders',
              instType: this.types[pair]
            }
          ]
        })
      )
    }

    this.invalidateOrderBookInitialization(api, pair)
    this.clearOrderBook(pair)
  }

  onMessage(event, api) {
    if (event.data === 'pong') {
      return
    }

    const json = JSON.parse(event.data)

    if (!json) {
      return
    }

    if (json.event === 'error') {
      console.warn(
        `[${this.id}] websocket error${
          json.arg && json.arg.channel ? ` on ${json.arg.channel}` : ''
        }: ${json.msg || json.code || 'unknown error'}`
      )
      return true
    }

    if (!json.data) {
      return
    }

    if (json.arg.channel === 'liquidation-orders') {
      const liqs = json.data.reduce((acc, pairData) => {
        if (api._connected.indexOf(pairData.instId) === -1) {
          return acc
        }

        return acc.concat(
          pairData.details.map(liquidation =>
            this.formatLiquidation(liquidation, pairData.instId)
          )
        )
      }, [])

      return this.emitLiquidations(api.id, liqs)
    }

    if (json.arg.channel === 'books') {
      return this.handleOrderBookMessage(json, api)
    }

    return this.emitTrades(
      api.id,
      json.data.map(trade => this.formatTrade(trade))
    )
  }

  invalidateOrderBookInitialization(api, pair) {
    api._orderBookInitTokens[pair] = (api._orderBookInitTokens[pair] || 0) + 1
    delete api._orderBookBuffers[pair]

    if (api._orderBookSnapshotTimeouts[pair]) {
      clearTimeout(api._orderBookSnapshotTimeouts[pair])
      delete api._orderBookSnapshotTimeouts[pair]
    }
  }

  rebuildOrderBook(api, pair, reason) {
    if (!api || !this.isPairTrackedByApi(api, pair)) {
      return true
    }

    if (api._orderBookRebuilds[pair]) {
      return true
    }

    api._orderBookRebuilds[pair] = true

    console.warn(
      `[${this.id}] depth sequence broke on ${pair}${
        reason ? ` (${reason})` : ''
      }, rebuilding local book`
    )

    this.initializeOrderBook(api, pair, {
      resetBuffer: true,
      clearBook: true
    })

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

  initializeOrderBook(api, pair, options = {}) {
    if (!this.supportsOrderBook(pair)) {
      return false
    }

    const { resetBuffer = false, clearBook = false } = options

    if (clearBook) {
      this.clearOrderBook(pair)
    }

    const orderBook = this.getOrderBook(pair)
    const initToken = (api._orderBookInitTokens[pair] || 0) + 1

    api._orderBookInitTokens[pair] = initToken
    orderBook.initializing = true
    orderBook.ready = false
    delete orderBook.seqId
    delete orderBook.prevSeqId

    if (resetBuffer || !Array.isArray(api._orderBookBuffers[pair])) {
      api._orderBookBuffers[pair] = []
    }

    if (api._orderBookSnapshotTimeouts[pair]) {
      clearTimeout(api._orderBookSnapshotTimeouts[pair])
    }

    api._orderBookSnapshotTimeouts[pair] = setTimeout(() => {
      if (api._orderBookInitTokens[pair] !== initToken) {
        return
      }

      const latestBook = this.orderBooks[pair]

      if (latestBook && latestBook.initializing) {
        this.rebuildOrderBook(api, pair, 'snapshot timeout')
      }
    }, 5000)

    return true
  }

  handleOrderBookMessage(message, api) {
    const book = message.data && message.data[0]
    const pair = (message.arg && message.arg.instId) || (book && book.instId)

    if (!book || !pair) {
      return
    }

    const orderBook = this.getOrderBook(pair)
    const seqId = +book.seqId
    const prevSeqId = +book.prevSeqId
    const timestamp = +book.ts || Date.now()

    if (message.action === 'snapshot') {
      if (api && api._orderBookSnapshotTimeouts[pair]) {
        clearTimeout(api._orderBookSnapshotTimeouts[pair])
        delete api._orderBookSnapshotTimeouts[pair]
      }

      this.resetOrderBook(pair, book.bids, book.asks, timestamp, {
        seqId,
        prevSeqId
      })

      const latestOrderBook = this.getOrderBook(pair)
      latestOrderBook.initializing = false

      const bufferedUpdates = api
        ? (api._orderBookBuffers[pair] || [])
          .filter(update => {
            const updateBook = update.data && update.data[0]
            return updateBook && +updateBook.seqId > seqId
          })
          .sort((a, b) => {
            const aBook = a.data && a.data[0]
            const bBook = b.data && b.data[0]

            return (
              (+aBook?.seqId || +aBook?.ts || 0) - (+bBook?.seqId || +bBook?.ts || 0)
            )
          })
        : []

      if (api) {
        api._orderBookBuffers[pair] = []
      }

      for (const update of bufferedUpdates) {
        this.handleOrderBookMessage(update, api)
      }

      return true
    }

    if (api && (orderBook.initializing || !isFinite(orderBook.seqId))) {
      api._orderBookBuffers[pair] = api._orderBookBuffers[pair] || []
      api._orderBookBuffers[pair].push(message)
      return true
    }

    if (
      (!Array.isArray(book.bids) || !book.bids.length) &&
      (!Array.isArray(book.asks) || !book.asks.length) &&
      isFinite(prevSeqId) &&
      isFinite(seqId) &&
      prevSeqId === seqId &&
      seqId === orderBook.seqId
    ) {
      orderBook.timestamp = timestamp
      return true
    }

    if (!isFinite(seqId) || !isFinite(prevSeqId)) {
      if (api) {
        return this.rebuildOrderBook(api, pair, 'missing sequence metadata')
      }

      return true
    }

    if (seqId < prevSeqId) {
      if (api) {
        return this.rebuildOrderBook(api, pair, 'sequence reset')
      }

      return true
    }

    if (isFinite(orderBook.seqId) && prevSeqId !== orderBook.seqId) {
      if (api) {
        return this.rebuildOrderBook(api, pair, 'prevSeqId mismatch')
      }

      return true
    }

    this.applyOrderBookDelta(pair, book.bids, book.asks, timestamp, {
      seqId,
      prevSeqId
    })

    return true
  }

  formatTrade(trade) {
    let size

    if (typeof this.specs[trade.instId] !== 'undefined') {
      size =
        (trade.sz * this.specs[trade.instId]) /
        (this.inversed[trade.instId] ? trade.px : 1)
    } else {
      size = trade.sz
    }

    return {
      exchange: this.id,
      pair: trade.instId,
      timestamp: +trade.ts,
      price: +trade.px,
      size: +size,
      side: trade.side
    }
  }

  formatLiquidation(liquidation, pair) {
    const size =
      (liquidation.sz * this.specs[pair]) /
      (this.inversed[pair] ? liquidation.bkPx : 1)

    return {
      exchange: this.id,
      pair: pair,
      timestamp: +liquidation.ts,
      price: +liquidation.bkPx,
      size: size,
      side: liquidation.side,
      liquidation: true
    }
  }

  getOrderBookLevelNotional(pair, price, size) {
    if (typeof this.specs[pair] !== 'number') {
      return price * size
    }

    return this.inversed[pair]
      ? size * this.specs[pair]
      : size * this.specs[pair] * price
  }

  onApiCreated(api) {
    api._orderBookBuffers = {}
    api._orderBookInitTokens = {}
    api._orderBookRebuilds = {}
    api._orderBookSnapshotTimeouts = {}
    this.startKeepAlive(api, 'ping', 25000)
  }

  onApiRemoved(api) {
    Object.keys(api._orderBookSnapshotTimeouts).forEach(pair => {
      clearTimeout(api._orderBookSnapshotTimeouts[pair])
    })

    this.stopKeepAlive(api)
  }

  getLiquidationsUrl(range) {
    // after query param = before
    // (get the 100 trades preceding endTimestamp)
    return `${this.endpoints.LIQUIDATIONS}?instId=${range.pair
    }&instType=SWAP&uly=${range.pair.replace('-SWAP', '')}&state=filled&after=${range.to
    }`
  }

  /**
   * Fetch pair liquidations before timestamp
   * @param {*} range
   * @returns
   */
  async fetchLiquidationOrders(range) {
    const url = this.getLiquidationsUrl(range)

    try {
      const response = await axios.get(url)
      if (response.data.data && response.data.data.length) {
        return response.data.data[0].details
      }
      return []
    } catch (error) {
      throw new Error(`Error fetching data: ${error}`)
    }
  }

  async fetchAllLiquidationOrders(range) {
    const allLiquidations = []
    let hasMore = true

    while (hasMore) {
      const liquidations = await this.fetchLiquidationOrders(range)

      if (!liquidations || liquidations.length === 0) {
        hasMore = false
        continue
      }

      for (const liquidation of liquidations) {
        if (liquidation.ts < range.from) {
          return allLiquidations
        }

        allLiquidations.push(liquidation)
      }

      range.to = +liquidations[liquidations.length - 1].ts
    }

    return allLiquidations
  }

  async getMissingTrades(range, totalRecovered = 0, first = true) {
    if (this.types[range.pair] !== 'SPOT' && first) {
      try {
        const liquidations = await this.fetchAllLiquidationOrders({ ...range })
        console.log(
          `[${this.id}.recoverMissingTrades] +${liquidations.length} liquidations for ${range.pair}`
        )

        if (liquidations.length) {
          this.emitLiquidations(
            null,
            liquidations.map(liquidation =>
              this.formatLiquidation(liquidation, range.pair)
            )
          )
        }
      } catch (error) {
        console.error(
          `[${this.id}] failed to get missing liquidations on ${range.pair}:`,
          error.message
        )
      }
    }

    const endpoint = `https://www.okx.com/api/v5/market/history-trades?instId=${range.pair}&type=2&limit=100&after=${range.to}`

    try {
      const response = await this.retryWithDelay(
        () => axios.get(endpoint),
        5, // Retry up to 5 times
        1, // Start with a multiplier of 1
        range
      )

      if (response.data.data.length) {
        const trades = response.data.data
          .filter(
            trade =>
              Number(trade.ts) > range.from &&
              Number(trade.ts) < range.to
          )
          .map(trade => this.formatTrade(trade))

        if (trades.length) {
          this.emitTrades(null, trades)
          totalRecovered += trades.length
          range.to = trades[trades.length - 1].timestamp
        }

        const remainingMissingTime = range.to - range.from

        if (trades.length) {
          console.log(
            `[${this.id}.recoverMissingTrades] +${trades.length} ${range.pair
            } ... but there's more (${getHms(remainingMissingTime)} remaining)`
          )
          return this.waitBeforeContinueRecovery().then(() =>
            this.getMissingTrades(range, totalRecovered, false)
          )
        } else {
          console.log(
            `[${this.id}.recoverMissingTrades] +${trades.length} ${range.pair
            } (${getHms(remainingMissingTime)} remaining)`
          )
        }
      }

      return totalRecovered
    } catch (err) {
      console.error(
        `[${this.id}] failed to get missing trades on ${range.pair} after retries:`,
        err.message
      )
      return totalRecovered
    }
  }

  /**
   * Retry a function with delays between attempts, using backoff
   * @param {Function} fn The function to execute
   * @param {number} retries Number of retry attempts
   * @param {number} multiplier Multiplier for backoff delays (starts at 1)
   * @param {Object} range The range object for logging retries (optional)
   * @returns {Promise<any>} The result of the function or an error if retries fail
   */
  async retryWithDelay(fn, retries, multiplier = 1, range = {}) {
    try {
      return await fn()
    } catch (err) {
      if (retries > 0) {
        console.warn(
          `[${this.id}] Retrying with delay (${range.pair || 'unknown pair'}) attempt ${multiplier
          }...`
        )
        await this.waitBeforeContinueRecovery(multiplier)
        return this.retryWithDelay(fn, retries - 1, multiplier + 1, range)
      }
      console.error(
        `[${this.id}] Exceeded retry limit for ${range.pair || 'unknown pair'
        }:`,
        err.message
      )
      throw err // Propagate the error after exceeding retries
    }
  }
}

module.exports = Okex
