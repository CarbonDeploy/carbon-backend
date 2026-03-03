import { Inject, Injectable } from '@nestjs/common';
import { Strategy } from '../../strategy/strategy.entity';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { CACHE_MANAGER } from '@nestjs/cache-manager';
import { Cache } from 'cache-manager';
import { Deployment } from '../../deployment/deployment.service';
import { Quote } from '../../quote/quote.entity';

@Injectable()
export class RoiService {
  constructor(
    @InjectRepository(Strategy) private strategy: Repository<Strategy>,
    @Inject(CACHE_MANAGER) private cacheManager: Cache,
  ) {}

  async update(deployment: Deployment, quotes: Quote[]): Promise<void> {
    const roi = await this.getROI(deployment, quotes);
    const cacheKey = this.getCacheKey(deployment);
    await this.cacheManager.set(cacheKey, roi);
  }

  async getCachedROI(deployment: Deployment): Promise<any> {
    const cacheKey = this.getCacheKey(deployment);
    const cache = await this.cacheManager.get(cacheKey);
    return cache;
  }

  private async getROI(deployment: Deployment, quotes: Quote[]): Promise<any> {
    // Create a temporary table with the quotes, ensuring proper type casting and null handling
    const quoteValues = quotes
      .filter((q) => q && q.token && q.token.address && q.usd)
      .map((q) => `('${q.token.address.toLowerCase()}', ${q.usd}::double precision)`)
      .join(',');

    // If no quotes, return empty result
    if (!quoteValues) {
      return [];
    }

    const query = `
WITH temp_quotes AS (
  SELECT * FROM (VALUES ${quoteValues}) AS t(contract_address, price)
),
created AS (
    SELECT timestamp as evt_block_time, "blockId" as evt_block_number, s."strategyId" as id, s."pairId" as pair_id, order0, order1, 
    LOWER(t0.address) as token0, t0.symbol as symbol0, t0.decimals as decimals0,
    LOWER(t1.address) as token1, t1.symbol as symbol1, t1.decimals as decimals1,
    2 as reason 
    FROM "strategy-created-events" s
    left join tokens t0 on t0.id = s."token0Id"
    left join tokens t1 on t1.id = s."token1Id"
    WHERE s."blockchainType" = '${deployment.blockchainType}' AND s."exchangeId" = '${deployment.exchangeId}'
),
updated AS (
    SELECT timestamp as evt_block_time, "blockId" as evt_block_number, s."strategyId" as id, s."pairId" as pair_id, order0, order1, 
    LOWER(t0.address) as token0, t0.symbol as symbol0, t0.decimals as decimals0,
    LOWER(t1.address) as token1, t1.symbol as symbol1, t1.decimals as decimals1,
    reason 
    FROM "strategy-updated-events" s
    left join tokens t0 on t0.id = s."token0Id"
    left join tokens t1 on t1.id = s."token1Id"
    WHERE s."blockchainType" = '${deployment.blockchainType}' AND s."exchangeId" = '${deployment.exchangeId}'
    
),
all_txs AS (
    SELECT *
    FROM created
    UNION
    SELECT *
    FROM updated
),
current_orders3 AS (
    SELECT *,
    (CASE WHEN (order0::json->>'y') IS NOT NULL THEN (order0::json->>'y')::double precision ELSE 0 END) AS y0,
    (CASE WHEN (order1::json->>'y') IS NOT NULL THEN (order1::json->>'y')::double precision ELSE 0 END) AS y1
    FROM all_txs
),
current_orders4 AS (
    SELECT evt_block_time, evt_block_number, current_orders3.id, pair_id, token0, token1, reason, y0, y1,
        symbol0, decimals0, symbol1, decimals1,
        y0 / POW(10, decimals0) AS liquidity0,
        y1 / POW(10, decimals1) AS liquidity1
    FROM current_orders3
),
order_lifespan AS (
    SELECT *,
        COALESCE((y0 - LAG(y0, 1) OVER (PARTITION BY id ORDER BY evt_block_number)) / POW(10, decimals0), 0) AS y0_delta,
        COALESCE((y1 - LAG(y1, 1) OVER (PARTITION BY id ORDER BY evt_block_number)) / POW(10, decimals1), 0) AS y1_delta
    FROM current_orders4
),
dep_with AS (
    SELECT *,
        (CASE WHEN reason = 2 THEN liquidity0 ELSE 0 END) + (CASE WHEN (reason = 0 AND y0_delta > 0) THEN y0_delta ELSE 0 END) AS y0_deposited,
        (CASE WHEN reason = 2 THEN liquidity1 ELSE 0 END) + (CASE WHEN (reason = 0 AND y1_delta > 0) THEN y1_delta ELSE 0 END) AS y1_deposited,
        (CASE WHEN reason = 4 THEN -liquidity0 ELSE 0 END) + (CASE WHEN (reason = 0 AND y0_delta < 0) THEN y0_delta ELSE 0 END) AS y0_withdrawn,
        (CASE WHEN reason = 4 THEN -liquidity1 ELSE 0 END) + (CASE WHEN (reason = 0 AND y1_delta < 0) THEN y1_delta ELSE 0 END) AS y1_withdrawn
    FROM order_lifespan
),
cuml_tkn_change AS (
    SELECT *,
        SUM(y0_deposited) OVER (PARTITION BY id ORDER BY evt_block_number) AS cuml_y0_deposit,
        SUM(y1_deposited) OVER (PARTITION BY id ORDER BY evt_block_number) AS cuml_y1_deposit,
        SUM(y0_withdrawn) OVER (PARTITION BY id ORDER BY evt_block_number) AS cuml_y0_withdrawn,
        SUM(y1_withdrawn) OVER (PARTITION BY id ORDER BY evt_block_number) AS cuml_y1_withdrawn
    FROM dep_with
),
adescriptions AS (
    SELECT *,
        CASE 
            WHEN reason = 2 THEN 'Created' 
            WHEN reason = 0 AND y0_delta > 0 THEN 'Deposited TKN0'
            WHEN reason = 0 AND y1_delta > 0 THEN 'Deposited TKN1'
            WHEN reason = 0 AND y0_delta < 0 THEN 'Withdrew TKN0'
            WHEN reason = 0 AND y1_delta < 0 THEN 'Withdrew TKN1'
            WHEN reason = 0 AND (y0_delta = 0 AND y1_delta = 0) THEN 'Updated Price'
            WHEN reason = 1 THEN 'Trade occurred'
            WHEN reason = 4 THEN 'Deleted'
            ELSE '0' 
        END AS descr
    FROM cuml_tkn_change
),
descriptions AS (
    SELECT * 
    FROM adescriptions
    WHERE descr != 'Updated Price'
),
add_new_creation AS (
    SELECT evt_block_time, evt_block_number, id, pair_id, token0, token1, reason, symbol0, decimals0, symbol1, decimals1, 
        y0, y1, liquidity0, liquidity1,
        y0_delta, y1_delta, y0_deposited, y1_deposited, y0_withdrawn, y1_withdrawn, cuml_y0_deposit, cuml_y1_deposit, cuml_y0_withdrawn, cuml_y1_withdrawn, 
        descr
    FROM descriptions
    UNION
    SELECT evt_block_time, evt_block_number, id, pair_id, token0, token1, reason, symbol0, decimals0, symbol1, decimals1, 
        y0, y1, liquidity0, liquidity1,
        y0_delta, y1_delta, y0_deposited, y1_deposited, y0_withdrawn, y1_withdrawn, cuml_y0_deposit, cuml_y1_deposit, cuml_y0_withdrawn, cuml_y1_withdrawn, 
        'ZCreate Substrategy' AS descr
    FROM descriptions
    WHERE (reason = 0 AND y0_delta != 0) OR (reason = 0 AND y1_delta != 0)
    UNION
    SELECT date_trunc('minute', current_timestamp) AS evt_block_time, evt_block_number, id, pair_id, token0, token1, 0 AS reason, symbol0, decimals0, symbol1, decimals1, 
        y0, y1, liquidity0, liquidity1, 
        y0_delta, y1_delta, y0_deposited, y1_deposited, y0_withdrawn, y1_withdrawn, cuml_y0_deposit, cuml_y1_deposit, cuml_y0_withdrawn, cuml_y1_withdrawn, 
        'ZFinal Novation' AS descr
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY id ORDER BY evt_block_number DESC) AS rn
        FROM descriptions
    ) t
    WHERE rn = 1
),
perform_novation AS (
    SELECT evt_block_time, evt_block_number, id, pair_id, token0, token1, reason, 
        CASE WHEN descr IN ('Deposited TKN0', 'Deposited TKN1', 'Withdrew TKN0', 'Withdrew TKN1', 'ZFinal Novation') THEN 0 ELSE y0 END AS y0, 
        CASE WHEN descr IN ('Deposited TKN0', 'Deposited TKN1', 'Withdrew TKN0', 'Withdrew TKN1', 'ZFinal Novation') THEN 0 ELSE y1 END AS y1, 
        symbol0, decimals0, symbol1, decimals1, 
        CASE WHEN descr IN ('Deposited TKN0', 'Deposited TKN1', 'Withdrew TKN0', 'Withdrew TKN1') THEN 'Novation' ELSE descr END AS descr
    FROM add_new_creation
),
add_row_nums AS (
    SELECT *, ROW_NUMBER() OVER(ORDER BY id, evt_block_number, descr) AS row_num
    FROM perform_novation
),
add_substrategy AS (
    SELECT *, SUM(CASE WHEN descr = 'ZCreate Substrategy' THEN 1 ELSE 0 END) OVER (PARTITION BY id ORDER BY row_num ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS substrategy
    FROM add_row_nums
    ORDER BY row_num
),
calculate_substrat_time AS (
    SELECT id, substrategy, EXTRACT(EPOCH FROM MAX(evt_block_time) - MIN(evt_block_time)) AS seconds_active
    FROM add_substrategy
    GROUP BY id, substrategy
),
joined_times AS (
    SELECT s.evt_block_time, s.evt_block_number, s.id, s.pair_id, s.token0, s.token1, s.reason, s.y0, s.y1, s.symbol0, s.decimals0, s.symbol1, s.decimals1, s.descr, 
            s.row_num, s.substrategy, c.seconds_active
    FROM add_substrategy s
    LEFT JOIN calculate_substrat_time c ON (c.id = s.id AND c.substrategy = s.substrategy)
),
recalc_y_ydelta AS (
    SELECT *,
    y0 / POW(10, decimals0) AS liquidity0,
    y1 / POW(10, decimals1) AS liquidity1,
    COALESCE((y0 - LAG(y0, 1) OVER (PARTITION BY id ORDER BY evt_block_number, descr)) / POW(10, decimals0), 0) AS y0_delta,
    COALESCE((y1 - LAG(y1, 1) OVER (PARTITION BY id ORDER BY evt_block_number, descr)) / POW(10, decimals1), 0) AS y1_delta
    FROM joined_times
),
recalc_ydepos AS (
    SELECT *,
        (CASE WHEN reason = 2 THEN liquidity0 ELSE 0 END) + (CASE WHEN (reason = 0 AND y0_delta > 0) THEN y0_delta ELSE 0 END) AS y0_deposited,
        (CASE WHEN reason = 2 THEN liquidity1 ELSE 0 END) + (CASE WHEN (reason = 0 AND y1_delta > 0) THEN y1_delta ELSE 0 END) AS y1_deposited,
        (CASE WHEN reason = 4 THEN -liquidity0 ELSE 0 END) + (CASE WHEN (reason = 0 AND y0_delta < 0) THEN y0_delta ELSE 0 END) AS y0_withdrawn,
        (CASE WHEN reason = 4 THEN -liquidity1 ELSE 0 END) + (CASE WHEN (reason = 0 AND y1_delta < 0) THEN y1_delta ELSE 0 END) AS y1_withdrawn
    FROM recalc_y_ydelta
),
recalc_cuml_y AS (
    SELECT evt_block_time, evt_block_number, id, pair_id, token0, token1, reason, symbol0, symbol1, liquidity0, liquidity1, 
            SUM(y0_deposited) OVER (PARTITION BY id, substrategy ORDER BY evt_block_number, descr) AS cuml_y0_deposit,
            SUM(y1_deposited) OVER (PARTITION BY id, substrategy ORDER BY evt_block_number, descr) AS cuml_y1_deposit,
            SUM(y0_withdrawn) OVER (PARTITION BY id, substrategy ORDER BY evt_block_number, descr) AS cuml_y0_withdrawn,
            SUM(y1_withdrawn) OVER (PARTITION BY id, substrategy ORDER BY evt_block_number, descr) AS cuml_y1_withdrawn,
            descr, substrategy, seconds_active, row_num
    FROM recalc_ydepos
),
db_quotes AS (
    SELECT DISTINCT ON (LOWER(t.address))
        LOWER(t.address) AS contract_address,
        COALESCE(NULLIF(q.usd, '')::double precision, 0) AS price
    FROM quotes q
    JOIN tokens t ON t.id = q."tokenId"
    WHERE q."blockchainType" = '${deployment.blockchainType}' AND t."exchangeId" = '${deployment.exchangeId}'
    ORDER BY LOWER(t.address), q.timestamp DESC
),
prices AS (
    SELECT DISTINCT ON (contract_address) contract_address, price
    FROM (
        -- temp_quotes (live prices) takes priority over db_quotes (cached)
        SELECT LOWER(contract_address) as contract_address, price, 1 as priority
        FROM temp_quotes
        UNION ALL
        SELECT contract_address, price, 2 as priority
        FROM db_quotes
    ) combined
    ORDER BY contract_address, priority
),
current_orders8 AS (
    SELECT evt_block_time, evt_block_number, id, pair_id, token0, token1, reason,
        symbol0, symbol1, liquidity0, liquidity1,
        cuml_y0_deposit, cuml_y1_deposit, cuml_y0_withdrawn, cuml_y1_withdrawn, descr, substrategy, seconds_active, row_num,
        p0.price AS current_price0, p1.price AS current_price1
    FROM recalc_cuml_y co
    LEFT JOIN prices p0 ON p0.contract_address = co.token0
    LEFT JOIN prices p1 ON p1.contract_address = co.token1
),
current_orders9 AS (
    SELECT evt_block_time, evt_block_number, id, pair_id, token0, token1, reason,
        symbol0, symbol1, liquidity0, liquidity1,
        cuml_y0_deposit, cuml_y1_deposit, cuml_y0_withdrawn, cuml_y1_withdrawn, descr, substrategy, seconds_active, row_num,
        current_price0, current_price1,
    CASE WHEN reason = 4 THEN 0 ELSE liquidity0::double precision * current_price0::double precision END AS TVL0_usd,
        CASE WHEN reason = 4 THEN 0 ELSE liquidity1::double precision * current_price1::double precision END AS TVL1_usd
    FROM current_orders8
),
totals AS (
    SELECT *,
        COALESCE(TVL0_usd, 0) + COALESCE(TVL1_usd, 0) AS stratTVL_usd,
    cuml_y0_deposit * CAST(current_price0 AS double precision) + cuml_y1_deposit * CAST(current_price1 AS double precision) AS total_deposit_usd,
    cuml_y0_withdrawn * CAST(current_price0 AS double precision) + cuml_y1_withdrawn * CAST(current_price1 AS double precision) AS total_withdrawn_usd
    FROM current_orders9
),
sub_rois AS (
    SELECT *,
        id AS strategyid,
        COALESCE(stratTVL_usd - total_deposit_usd - total_withdrawn_usd, 0) AS substrat_profit,
        CASE WHEN total_deposit_usd = 0 THEN 0 ELSE COALESCE(stratTVL_usd - total_deposit_usd - total_withdrawn_usd, 0) / total_deposit_usd END AS row_roi_perc
    FROM totals
),
aggregate_substrats AS (
    SELECT *,
        CASE 
            WHEN SUM(total_deposit_usd) OVER (PARTITION BY id) = 0 THEN 0
            ELSE total_deposit_usd / SUM(total_deposit_usd) OVER (PARTITION BY id)
        END AS liq_weight_perc,
        
        CASE 
            WHEN SUM(total_deposit_usd) OVER (PARTITION BY id) = 0 THEN 0
            ELSE total_deposit_usd * total_deposit_usd / SUM(total_deposit_usd) OVER (PARTITION BY id)
        END AS weighted_deposit_usd,
        
        SUM(substrat_profit) OVER (PARTITION BY id ORDER BY evt_block_number, descr) AS cuml_substrat_profit
    FROM sub_rois
    WHERE descr IN ('Novation', 'ZFinal Novation')
),
roi_for_each_novation AS (
    SELECT *,
        SUM(weighted_deposit_usd) OVER (PARTITION BY id) AS lw_deposit,
        
        CASE 
            WHEN SUM(weighted_deposit_usd) OVER (PARTITION BY id) = 0 THEN 0
            ELSE cuml_substrat_profit / SUM(weighted_deposit_usd) OVER (PARTITION BY id)
        END AS lw_ROI
    FROM aggregate_substrats
),
create_delete AS (
    SELECT c."timestamp" AS date_created, c."strategyId" as id, c."pairId" as pair_id, d."timestamp" AS date_most_recent
    FROM "strategy-created-events" c
    LEFT JOIN "strategy-deleted-events" d ON c."strategyId" = d."strategyId"
    WHERE c."blockchainType" = '${deployment.blockchainType}' AND c."exchangeId" = '${deployment.exchangeId}'
),
most_recent AS (
    SELECT *, CASE WHEN date_most_recent IS NULL THEN NOW() ELSE date_most_recent END AS date_most_recent2
    FROM create_delete
),
lifetime AS (
    SELECT id, pair_id,
        CASE 
            WHEN date_part('day', date_most_recent2 - date_created) = 0 THEN 1
            ELSE date_part('day', date_most_recent2 - date_created) 
        END AS days_live 
    FROM most_recent
),
trades AS (
    SELECT "strategyId" AS id,
           COUNT(*) FILTER (WHERE reason = 1) AS trades_count
    FROM "strategy-updated-events"
    WHERE "blockchainType" = '${deployment.blockchainType}' AND "exchangeId" = '${deployment.exchangeId}'
    GROUP BY "strategyId"
),
recent_roi_only AS (
    SELECT n.id, n.pair_id, n.token0, n.token1, days_live, lw_deposit AS average_value_onhand, cuml_substrat_profit AS cuml_profit,
        CASE 
            WHEN CAST(lw_ROI * 100 AS VARCHAR) = 'NaN' THEN 0.0 
            ELSE CAST(lw_ROI * 100 AS DOUBLE PRECISION)
        END AS ROI  
    FROM roi_for_each_novation n
    LEFT JOIN lifetime l ON l.id = n.id
    WHERE descr = 'ZFinal Novation'
),
fee_token_prices AS (
    SELECT t.id AS token_id, t.decimals,
        COALESCE(NULLIF(q.usd, '')::double precision, 0) AS price
    FROM tokens t
    LEFT JOIN quotes q ON q."tokenId" = t.id
    WHERE t."blockchainType" = '${deployment.blockchainType}' AND t."exchangeId" = '${deployment.exchangeId}'
),
fees_7d AS (
    SELECT tte."pairId" AS pair_id,
        SUM(
            COALESCE(NULLIF(tte."tradingFeeAmount", '')::double precision, 0) 
            / NULLIF(POW(10, ftp.decimals), 0) 
            * COALESCE(ftp.price, 0)
        ) AS fees_usd
    FROM "tokens-traded-events" tte
    JOIN fee_token_prices ftp ON ftp.token_id = tte."targetTokenId"
    WHERE tte."blockchainType" = '${deployment.blockchainType}' AND tte."exchangeId" = '${deployment.exchangeId}'
        AND tte.timestamp >= NOW() - INTERVAL '7 days'
    GROUP BY tte."pairId"
),
fees_30d AS (
    SELECT tte."pairId" AS pair_id,
        SUM(
            COALESCE(NULLIF(tte."tradingFeeAmount", '')::double precision, 0) 
            / NULLIF(POW(10, ftp.decimals), 0) 
            * COALESCE(ftp.price, 0)
        ) AS fees_usd
    FROM "tokens-traded-events" tte
    JOIN fee_token_prices ftp ON ftp.token_id = tte."targetTokenId"
    WHERE tte."blockchainType" = '${deployment.blockchainType}' AND tte."exchangeId" = '${deployment.exchangeId}'
        AND tte.timestamp >= NOW() - INTERVAL '30 days'
    GROUP BY tte."pairId"
),
latest_tvl_ts AS (
    SELECT "pairId" AS pair_id, "strategyId" AS strategy_id, MAX(evt_block_time) AS max_time
    FROM tvl
    WHERE "blockchainType" = '${deployment.blockchainType}' AND "exchangeId" = '${deployment.exchangeId}'
    GROUP BY "pairId", "strategyId"
),
tvl_latest AS (
    SELECT t."pairId" AS pair_id,
        SUM(
            COALESCE(NULLIF(t.tvl, '')::double precision, 0) * COALESCE(p.price, 0)
        ) AS tvl_usd
    FROM tvl t
    JOIN latest_tvl_ts lt ON lt.pair_id = t."pairId" AND lt.strategy_id = t."strategyId" AND lt.max_time = t.evt_block_time
    LEFT JOIN prices p ON p.contract_address = LOWER(t.address)
    WHERE t."blockchainType" = '${deployment.blockchainType}' AND t."exchangeId" = '${deployment.exchangeId}'
    GROUP BY t."pairId"
),
pair_metrics AS (
    SELECT p.id AS pair_id,
        COALESCE(tl.tvl_usd, 0) AS tvl_usd,
        CASE 
            WHEN COALESCE(tl.tvl_usd, 0) = 0 THEN 0
            ELSE COALESCE(f7.fees_usd, 0) / tl.tvl_usd * (365.0 / 7.0) * 100
        END AS apr_7d,
        CASE 
            WHEN COALESCE(tl.tvl_usd, 0) = 0 THEN 0
            ELSE COALESCE(f30.fees_usd, 0) / tl.tvl_usd * (365.0 / 30.0) * 100
        END AS apr_30d
    FROM pairs p
    LEFT JOIN fees_7d f7 ON f7.pair_id = p.id
    LEFT JOIN fees_30d f30 ON f30.pair_id = p.id
    LEFT JOIN tvl_latest tl ON tl.pair_id = p.id
    WHERE p."blockchainType" = '${deployment.blockchainType}' AND p."exchangeId" = '${deployment.exchangeId}'
)
SELECT r.id, r.pair_id, r.token0, r.token1, r.ROI, COALESCE(t.trades_count, 0) AS trades_count,
    COALESCE(pm.apr_7d, 0) AS pair_apr_7d,
    COALESCE(pm.apr_30d, 0) AS pair_apr_30d,
    COALESCE(pm.tvl_usd, 0) AS pair_tvl
FROM recent_roi_only r
LEFT JOIN trades t ON t.id = r.id
LEFT JOIN pair_metrics pm ON pm.pair_id = r.pair_id
ORDER BY ROI DESC;
    
    `;

    const result = await this.strategy.query(query);

    const strategies = result.map((r) => {
      const roiValue =
        r.roi === null || Number(r.roi) <= -100 ? 0 : Number(r.roi);

      return {
        id: r.id,
        ROI: roiValue,
        token0: r.token0,
        token1: r.token1,
        trades: Number(r.trades_count ?? 0),
        pairApr7d: Number(r.pair_apr_7d ?? 0),
        pairApr30d: Number(r.pair_apr_30d ?? 0),
        pairTvl: Number(r.pair_tvl ?? 0),
      };
    });

    const pairsMap = new Map<
      string,
      {
        baseTokenAddress: string;
        quoteTokenAddress: string;
        roiMin: number;
        roiMax: number;
        totalTrades: number;
        strategyCount: number;
        strategies: Array<{ id: string; ROI: number; trades: number }>;
        apr7d: number;
        apr30d: number;
        tvl: number;
      }
    >();

    strategies.forEach((strategy) => {
      if (!strategy.token0 || !strategy.token1) {
        return;
      }

      const [baseToken, quoteToken] = [strategy.token0, strategy.token1]
        .map((t) => String(t).toLowerCase())
        .sort();
      const pairKey = `${baseToken}-${quoteToken}`;

      const existing = pairsMap.get(pairKey) || {
        baseTokenAddress: baseToken,
        quoteTokenAddress: quoteToken,
        roiMin: Number.POSITIVE_INFINITY,
        roiMax: Number.NEGATIVE_INFINITY,
        totalTrades: 0,
        strategyCount: 0,
        strategies: [],
        apr7d: 0,
        apr30d: 0,
        tvl: 0,
      };

      existing.roiMin = Math.min(existing.roiMin, strategy.ROI);
      existing.roiMax = Math.max(existing.roiMax, strategy.ROI);
      existing.totalTrades += strategy.trades ?? 0;
      existing.strategyCount += 1;
      existing.strategies.push({
        id: strategy.id,
        ROI: strategy.ROI,
        trades: strategy.trades,
      });
      existing.apr7d = strategy.pairApr7d ?? existing.apr7d;
      existing.apr30d = strategy.pairApr30d ?? existing.apr30d;
      existing.tvl = strategy.pairTvl ?? existing.tvl;

      pairsMap.set(pairKey, existing);
    });

    const pairs = Array.from(pairsMap.values())
      .map((p) => ({
        baseTokenAddress: p.baseTokenAddress,
        quoteTokenAddress: p.quoteTokenAddress,
        roiRange: {
          min: p.roiMin === Number.POSITIVE_INFINITY ? 0 : p.roiMin,
          max: p.roiMax === Number.NEGATIVE_INFINITY ? 0 : p.roiMax,
        },
        apr7d: p.apr7d,
        apr30d: p.apr30d,
        tvl: p.tvl,
        totalTrades: p.totalTrades,
        strategyCount: p.strategyCount,
        strategies: p.strategies,
      }))
      .sort((a, b) => b.roiRange.max - a.roiRange.max);

    return pairs;
  }

  private getCacheKey(deployment: Deployment): string {
    return `${deployment.blockchainType}:${deployment.exchangeId}:roi`;
  }
}
