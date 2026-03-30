const Exchange = require('../exchange')
const config = require('../config')
const { readProducts, saveProducts } = require('../services/catalog')
const axios = require('axios')

class Hyperliquid extends Exchange {
  constructor() {
    super()
    this.id = 'HYPERLIQUID'
    this.l4Orders = {}

    this.endpoints = {
      PRODUCTS: 'https://api.hyperliquid.xyz/info'
    }
  }

  async getUrl() {
    return this.getOrderBookServerUrl() || 'wss://api.hyperliquid.xyz/ws'
  }

  getOrderBookServerUrl() {
    if (
      typeof config.hyperliquidOrderBookServerUrl === 'string' &&
      config.hyperliquidOrderBookServerUrl.trim().length
    ) {
      return config.hyperliquidOrderBookServerUrl.trim()
    }

    return null
  }

  useOrderBookServer() {
    return !!this.getOrderBookServerUrl()
  }

  getOrderBookServerChannel() {
    if (!this.useOrderBookServer()) {
      return null
    }

    return config.hyperliquidOrderBookServerChannel === 'l2Book'
      ? 'l2Book'
      : 'l4Book'
  }

  getConfiguredDepthLevels() {
    const levels = +config.hyperliquidOrderBookLevels

    if (!isFinite(levels) || levels <= 0) {
      return 100
    }

    return Math.max(1, Math.min(100, Math.round(levels)))
  }

  getOrderBookSubscription(pair) {
    if (this.getOrderBookServerChannel() === 'l4Book') {
      return {
        type: 'l4Book',
        coin: pair
      }
    }

    const subscription = {
      type: 'l2Book',
      coin: pair,
      nSigFigs: null
    }

    if (this.useOrderBookServer()) {
      subscription.nLevels = this.getConfiguredDepthLevels()
    }

    return subscription
  }

  clearL4Orders(pair) {
    delete this.l4Orders[pair]
  }

  getL4Orders(pair) {
    if (!this.l4Orders[pair]) {
      this.l4Orders[pair] = {}
    }

    return this.l4Orders[pair]
  }

  getL4OrderKey(order) {
    if (!order || typeof order.oid === 'undefined' || order.oid === null) {
      return null
    }

    const oid = `${order.oid}`
    const user =
      typeof order.user === 'string'
        ? order.user
        : order.user && typeof order.user.toString === 'function'
          ? order.user.toString()
          : ''

    return user ? `${user}:${oid}` : oid
  }

  normalizeL4Order(order, fallbackPair) {
    if (!order || typeof order !== 'object') {
      return null
    }

    const side = order.side === 'B' ? 'bid' : order.side === 'A' ? 'ask' : null
    const price = +(order.limitPx || order.limit_px || order.px)
    const size = +order.sz
    const pair = order.coin || fallbackPair

    if (!side || !pair || !isFinite(price) || price <= 0 || !isFinite(size) || size <= 0) {
      return null
    }

    return {
      user: order.user,
      oid: order.oid,
      pair,
      side,
      price,
      size
    }
  }

  isL4OrderInserted(status) {
    if (!status || !status.order) {
      return false
    }

    const isTrigger = !!status.order.isTrigger
    const tif = status.order.tif

    return (
      (status.status === 'open' && !isTrigger && tif !== 'Ioc') ||
      (isTrigger && status.status === 'triggered')
    )
  }

  normalizeL4BookLevels(levels, pair) {
    const orders = {}

    for (const sideLevels of Array.isArray(levels) ? levels : []) {
      for (const level of Array.isArray(sideLevels) ? sideLevels : []) {
        const normalizedOrder = this.normalizeL4Order(level, pair)
        const key = this.getL4OrderKey(level)

        if (!normalizedOrder || !key) {
          continue
        }

        orders[key] = normalizedOrder
      }
    }

    return orders
  }

  exportL4Orders(pair) {
    const aggregated = {
      bid: {},
      ask: {}
    }
    const orders = this.getL4Orders(pair)

    for (const order of Object.values(orders)) {
      if (!order || order.pair !== pair) {
        continue
      }

      const side = aggregated[order.side]

      if (!side) {
        continue
      }

      const key = `${order.price}`
      side[key] = (side[key] || 0) + order.size
    }

    return {
      bids: Object.entries(aggregated.bid).map(([price, size]) => [+price, size]),
      asks: Object.entries(aggregated.ask).map(([price, size]) => [+price, size])
    }
  }

  parseL4OrderDiff(rawDiff) {
    if (!rawDiff) {
      return null
    }

    if (typeof rawDiff === 'string') {
      if (rawDiff === 'Remove' || rawDiff === 'remove') {
        return { type: 'remove' }
      }

      return null
    }

    if (rawDiff.New || rawDiff.new) {
      const value = rawDiff.New || rawDiff.new
      return {
        type: 'new',
        size: +(value.sz || value.size)
      }
    }

    if (rawDiff.Update || rawDiff.update) {
      const value = rawDiff.Update || rawDiff.update
      return {
        type: 'update',
        newSize: +(value.newSz || value.new_sz),
        originalSize: +(value.origSz || value.orig_sz)
      }
    }

    if (rawDiff.Remove || rawDiff.remove) {
      return { type: 'remove' }
    }

    return null
  }

  handleL4BookSnapshot(snapshot) {
    if (!snapshot || !snapshot.coin) {
      return false
    }

    const pair = snapshot.coin

    this.l4Orders[pair] = this.normalizeL4BookLevels(snapshot.levels, pair)

    const { bids, asks } = this.exportL4Orders(pair)

    this.resetOrderBook(pair, bids, asks, +snapshot.time || Date.now(), {
      height: +snapshot.height || null
    })

    return true
  }

  handleL4BookUpdates(update) {
    if (!update) {
      return false
    }

    const statuses = update.orderStatuses || update.order_statuses || []
    const diffs = update.bookDiffs || update.book_diffs || []
    const pair =
      (statuses[0] && statuses[0].order && statuses[0].order.coin) ||
      (diffs[0] && diffs[0].coin)

    if (!pair) {
      return false
    }

    const orders = this.getL4Orders(pair)

    for (const status of statuses) {
      const order = this.normalizeL4Order(status.order, pair)
      const key = this.getL4OrderKey({
        user: status.user || (status.order && status.order.user),
        oid: status.order && status.order.oid
      })

      if (!key) {
        continue
      }

      if (this.isL4OrderInserted(status) && order) {
        orders[key] = {
          ...order,
          user: status.user || order.user
        }
      } else {
        delete orders[key]
      }
    }

    for (const diff of diffs) {
      const key = this.getL4OrderKey(diff)

      if (!key || !orders[key]) {
        continue
      }

      const parsedDiff = this.parseL4OrderDiff(diff.rawBookDiff || diff.raw_book_diff)

      if (!parsedDiff) {
        continue
      }

      if (parsedDiff.type === 'remove') {
        delete orders[key]
        continue
      }

      const newSize =
        parsedDiff.type === 'update' ? parsedDiff.newSize : parsedDiff.size
      const newPrice = +(diff.px || orders[key].price)

      if (!isFinite(newSize) || newSize <= 0) {
        delete orders[key]
        continue
      }

      if (isFinite(newPrice) && newPrice > 0) {
        orders[key].price = newPrice
      }

      orders[key].size = newSize
    }

    const { bids, asks } = this.exportL4Orders(pair)

    this.resetOrderBook(pair, bids, asks, +update.time || Date.now(), {
      height: +update.height || null
    })

    return true
  }

  // Custom getProducts implementation for POST API
  async getProducts(forceRefreshProducts = false) {
    let formatedProducts

    // Load from cache if not forcing refresh
    if (!forceRefreshProducts) {
      try {
        formatedProducts = await readProducts(this.id)
      } catch (error) {
        console.error(`[${this.id}/getProducts] failed to read products`, error)
      }
    }

    // Fetch new products if no cache available
    if (!formatedProducts) {
      try {
        console.log(`[${this.id}] fetching products via POST API`)

        const response = await axios.post(
          this.endpoints.PRODUCTS,
          { type: 'meta' },
          {
            headers: { 'Content-Type': 'application/json' }
          }
        )

        formatedProducts = this.formatProducts(response.data) || []

        // Save to cache
        await saveProducts(this.id, formatedProducts)
        console.log(
          `[${this.id}] saved ${
            formatedProducts.products?.length || 0
          } products`
        )
      } catch (error) {
        console.error(
          `[${this.id}/getProducts] failed to fetch products`,
          error.message
        )
        throw error
      }
    }

    // Set products to instance
    if (formatedProducts && formatedProducts.products) {
      this.products = formatedProducts.products
    }

    return this.products
  }

  formatProducts(response) {
    const products = []

    if (response && response.universe && response.universe.length) {
      for (const product of response.universe) {
        products.push(product.name)
      }
    }

    console.log(`[${this.id}] formatted ${products.length} products`)
    return { products }
  }

  supportsOpenInterest() {
    return true
  }

  supportsOrderBook() {
    return true
  }

  async fetchOpenInterests(pairs) {
    const openInterests = {}
    const response = await this.fetchJson({
      url: 'https://api.hyperliquid.xyz/info',
      method: 'POST',
      data: {
        type: 'metaAndAssetCtxs'
      },
      headers: {
        'Content-Type': 'application/json'
      }
    })
    const universe = response && response[0] && response[0].universe
    const contexts = response && response[1]

    if (!universe || !contexts) {
      return openInterests
    }

    const contextsByPair = universe.reduce((output, product, index) => {
      output[product.name] = contexts[index]
      return output
    }, {})

    for (const pair of pairs) {
      const context = contextsByPair[pair]

      if (!context) {
        continue
      }

      const openInterest = +context.openInterest
      const price = +context.markPx || +context.oraclePx

      if (!isFinite(openInterest) || !price) {
        continue
      }

      openInterests[pair] = openInterest * price
    }

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
        method: 'subscribe',
        subscription: {
          type: 'trades',
          coin: pair
        }
      })
    )

    api.send(
      JSON.stringify({
        method: 'subscribe',
        subscription: this.getOrderBookSubscription(pair)
      })
    )

    return true
  }

  /**
   * Sub
   * @param {WebSocket} api
   * @param {string} pair
   */
  async unsubscribe(api, pair) {
    if (!(await super.unsubscribe.apply(this, arguments))) {
      return
    }

    api.send(
      JSON.stringify({
        method: 'unsubscribe',
        subscription: {
          type: 'trades',
          coin: pair
        }
      })
    )

    api.send(
      JSON.stringify({
        method: 'unsubscribe',
        subscription: this.getOrderBookSubscription(pair)
      })
    )

    if (this.getOrderBookServerChannel() === 'l4Book') {
      this.clearL4Orders(pair)
      this.clearOrderBook(pair)
    }

    return true
  }

  onMessage(event, api) {
    const json = JSON.parse(event.data)

    if (json && json.channel === 'trades') {
      return this.emitTrades(
        api.id,
        json.data.map(trade => this.formatTrade(trade))
      )
    }

    if (
      json &&
      (json.channel === 'l4Book' || json.channel === 'l4book') &&
      json.data
    ) {
      const snapshot = json.data.Snapshot || json.data.snapshot
      const updates = json.data.Updates || json.data.updates

      if (snapshot) {
        return this.handleL4BookSnapshot(snapshot)
      }

      if (updates) {
        return this.handleL4BookUpdates(updates)
      }
    }

    if (
      json &&
      (json.channel === 'l2Book' || json.channel === 'l2book') &&
      json.data
    ) {
      this.resetOrderBook(
        json.data.coin,
        json.data.levels && json.data.levels[0]
          ? json.data.levels[0].map(level => [level.px, level.sz])
          : [],
        json.data.levels && json.data.levels[1]
          ? json.data.levels[1].map(level => [level.px, level.sz])
          : [],
        +json.data.time
      )

      return true
    }
  }

  formatTrade(trade) {
    return {
      exchange: this.id,
      pair: trade.coin,
      timestamp: +new Date(trade.time),
      price: +trade.px,
      size: +trade.sz,
      side: trade.side === 'B' ? 'buy' : 'sell'
    }
  }
}

module.exports = Hyperliquid
