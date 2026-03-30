const fs = require('fs')
const path = require('path')
const config = require('../config')
const { ensureDirectoryExists } = require('../helper')
const { parseMarket } = require('../services/catalog')

class DepthStorage {
  constructor() {
    this.name = this.constructor.name
    this.format = 'depth'

    if (!fs.existsSync(config.depthLocation)) {
      fs.mkdirSync(config.depthLocation, { recursive: true })
    }
  }

  getSnapshotPath(market, timestamp) {
    const [, exchange, pair] = market.match(/([^:]*):(.*)/)
    const date = new Date(timestamp)
    const day = `${date.getUTCFullYear()}-${(`0${date.getUTCMonth() + 1}`).slice(
      -2
    )}-${(`0${date.getUTCDate()}`).slice(-2)}`
    const sanitizedPair = pair.replace(/[/:]/g, '-')

    return path.join(config.depthLocation, exchange, sanitizedPair, `${day}.jsonl`)
  }

  getDaysBetween(from, to) {
    const days = []
    const current = new Date(from)
    current.setUTCHours(0, 0, 0, 0)
    const end = new Date(to)
    end.setUTCHours(0, 0, 0, 0)

    while (+current <= +end) {
      days.push(
        `${current.getUTCFullYear()}-${(`0${current.getUTCMonth() + 1}`).slice(
          -2
        )}-${(`0${current.getUTCDate()}`).slice(-2)}`
      )
      current.setUTCDate(current.getUTCDate() + 1)
    }

    return days
  }

  async save(snapshots) {
    if (!snapshots || !snapshots.length) {
      return
    }

    const groupedSnapshots = snapshots.reduce((output, snapshot) => {
      const filePath = this.getSnapshotPath(snapshot.market, snapshot.time)

      if (!output[filePath]) {
        output[filePath] = []
      }

      output[filePath].push(snapshot)

      return output
    }, {})

    await Promise.all(
      Object.keys(groupedSnapshots).map(async filePath => {
        await ensureDirectoryExists(filePath)

        const lines = groupedSnapshots[filePath]
          .map(snapshot =>
            JSON.stringify({
              t: snapshot.time,
              m: snapshot.mid,
              s: snapshot.priceStep,
              b: snapshot.bids,
              a: snapshot.asks,
              rp: snapshot.rangePercent
            })
          )
          .join('\n')

        await fs.promises.appendFile(filePath, `${lines}\n`)
      })
    )
  }

  async fetch({ from, to, markets = [] }) {
    const results = []

    await Promise.all(
      markets.map(async market => {
        const days = this.getDaysBetween(from, to)
        const paths = days.map(day => {
          const [, exchange, pair] = market.match(/([^:]*):(.*)/)
          const sanitizedPair = pair.replace(/[/:]/g, '-')

          return path.join(
            config.depthLocation,
            exchange,
            sanitizedPair,
            `${day}.jsonl`
          )
        })

        for (const filePath of paths) {
          if (!fs.existsSync(filePath)) {
            continue
          }

          const content = await fs.promises.readFile(filePath, 'utf8')

          if (!content) {
            continue
          }

          const lines = content.split('\n').filter(Boolean)

          for (const line of lines) {
            let snapshot

            try {
              snapshot = JSON.parse(line)
            } catch (_error) {
              continue
            }

            if (
              typeof snapshot.t !== 'number' ||
              snapshot.t < from ||
              snapshot.t >= to
            ) {
              continue
            }

            results.push({
              ...this.normalizeSnapshot(snapshot, market),
              time: snapshot.t,
              market
            })
          }
        }
      })
    )

    results.sort((a, b) => {
      if (a.time === b.time) {
        return a.market.localeCompare(b.market)
      }

      return a.time - b.time
    })

    return {
      format: this.format,
      results
    }
  }

  normalizeDepthLevelMap(levels) {
    if (!levels || typeof levels !== 'object' || Array.isArray(levels)) {
      return {}
    }

    return Object.entries(levels).reduce((output, [price, value]) => {
      const numericPrice = +price
      const numericValue = +value

      if (
        !isFinite(numericPrice) ||
        numericPrice <= 0 ||
        !isFinite(numericValue) ||
        numericValue <= 0
      ) {
        return output
      }

      output[price] = numericValue
      return output
    }, {})
  }

  normalizeSnapshot(snapshot, market) {
    if (
      typeof snapshot.s === 'number' &&
      snapshot.s > 0 &&
      snapshot.b &&
      !Array.isArray(snapshot.b) &&
      snapshot.a &&
      !Array.isArray(snapshot.a)
    ) {
      return {
        mid: +snapshot.m,
        priceStep: +snapshot.s,
        bids: this.normalizeDepthLevelMap(snapshot.b),
        asks: this.normalizeDepthLevelMap(snapshot.a),
        rangePercent:
          typeof snapshot.rp === 'number'
            ? snapshot.rp
            : config.depthRangePercent
      }
    }

    return this.normalizeLegacySnapshot(snapshot, market)
  }

  getConfiguredPriceStep(market) {
    const configuredSteps = config.depthPriceSteps || {}
    const match = market && market.match(/([^:]*):(.*)/)

    if (!match) {
      return null
    }

    const exchange = match[1]
    const pair = match[2]
    const parsedMarket = parseMarket(exchange, pair, false)
    const candidates = [
      market,
      pair,
      parsedMarket && parsedMarket.base,
      parsedMarket && parsedMarket.local
    ].filter(Boolean)

    for (const candidate of candidates) {
      const step = +configuredSteps[candidate]

      if (isFinite(step) && step > 0) {
        return step
      }
    }

    return null
  }

  normalizeLegacySnapshot(snapshot, market) {
    const mid = +snapshot.m
    const bucketPercent =
      typeof snapshot.bp === 'number'
        ? snapshot.bp
        : config.depthBucketPercent
    const rangePercent =
      typeof snapshot.rp === 'number'
        ? snapshot.rp
        : config.depthRangePercent
    const priceStep = this.getConfiguredPriceStep(market) || (mid * bucketPercent) / 100
    const bids = {}
    const asks = {}
    const firstBidPrice = Math.floor(mid / priceStep) * priceStep
    const firstAskPrice = Math.ceil(mid / priceStep) * priceStep

    if (Array.isArray(snapshot.b)) {
      for (let i = 0; i < snapshot.b.length; i++) {
        const value = +snapshot.b[i]

        if (!isFinite(value) || value <= 0) {
          continue
        }

        const price = firstBidPrice - i * priceStep
        bids[this.formatPriceKey(price, priceStep)] = value
      }
    }

    if (Array.isArray(snapshot.a)) {
      for (let i = 0; i < snapshot.a.length; i++) {
        const value = +snapshot.a[i]

        if (!isFinite(value) || value <= 0) {
          continue
        }

        const price = firstAskPrice + i * priceStep
        asks[this.formatPriceKey(price, priceStep)] = value
      }
    }

    return {
      mid,
      priceStep,
      bids,
      asks,
      rangePercent
    }
  }

  getPriceStepPrecision(step) {
    const normalizedStep = +step
    let precision = 0

    while (
      precision < 8 &&
      Math.abs(
        Math.round(normalizedStep * Math.pow(10, precision)) -
          normalizedStep * Math.pow(10, precision)
      ) > 1e-8
    ) {
      precision++
    }

    return precision
  }

  formatPriceKey(price, step) {
    const precision = this.getPriceStepPrecision(step)
    const factor = Math.pow(10, precision)
    const normalizedPrice = Math.round(price * factor) / factor

    return precision
      ? normalizedPrice.toFixed(precision).replace(/\.?0+$/, '')
      : Math.round(normalizedPrice).toString()
  }
}

module.exports = DepthStorage
