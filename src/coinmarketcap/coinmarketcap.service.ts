// coinmarketcap.service.ts

import { Injectable, Logger } from '@nestjs/common';
import axios, { AxiosResponse } from 'axios';
import { ConfigService } from '@nestjs/config';
import { toTimestamp } from '../utilities';
import moment from 'moment';
import { NATIVE_TOKEN } from '../deployment/deployment.service';

export interface PriceObject {
  timestamp: number;
  price: number;
}

const MAX_RESULTS_PER_CALL = 10000;
const INTERVAL_IN_MINUTES = 360;

const ETH_ID = 1027;
const BNB_ID = 1839;

const PLATFORM_MAPPING = {
  'ethereum': 'ethereum',
  'bsc': 'binance-smart-chain', // CoinMarketCap's slug for BSC
};

const NATIVE_TOKEN_IDS = {
  'ethereum': ETH_ID,
  'bsc': BNB_ID,
};

@Injectable()
export class CoinMarketCapService {
  private ethAddress;
  private bnbAddress;

  constructor(private readonly configService: ConfigService) {
    this.ethAddress = NATIVE_TOKEN;
    this.bnbAddress = NATIVE_TOKEN;
  }

  private getApiKey(): string {
    return this.configService.get<string>('COINMARKETCAP_API_KEY');
  }

  private async getTokenIds(tokenAddresses: string[], platform: string): Promise<string[]> {
    const apiKey = this.getApiKey();
    const infoUrl = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/map';

    try {
      const response = await axios.get(infoUrl, {
        params: {
          aux: 'platform',
        },
        headers: { 'X-CMC_PRO_API_KEY': apiKey },
      });

      const data = response.data.data;
      const platformSlug = PLATFORM_MAPPING[platform];
      const nativeTokenId = NATIVE_TOKEN_IDS[platform];

      const tokenIds = tokenAddresses.map((address) => {
        if (address.toLowerCase() === NATIVE_TOKEN.toLowerCase()) {
          return nativeTokenId.toString();
        }
        const foundToken = data.find(
          (token) => 
            token.platform?.token_address.toLowerCase() === address.toLowerCase() &&
            token.platform?.slug === platformSlug
        );
        return foundToken ? foundToken.id.toString() : null;
      });

      return tokenIds.filter((id) => id !== null);
    } catch (error) {
      throw error;
    }
  }

  private async getV3CryptocurrencyQuotesHistorical(params: any): Promise<AxiosResponse> {
    const apiKey = this.getApiKey();
    const url = 'https://pro-api.coinmarketcap.com/v3/cryptocurrency/quotes/historical';

    try {
      const response = await axios.get(url, { params, headers: { 'X-CMC_PRO_API_KEY': apiKey } });
      return response;
    } catch (error) {
      throw error;
    }
  }

  private async getV1CryptocurrencyListingsLatest(platform: string ): Promise<any> {
    const apiUrl = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest';
    const apiKey = this.getApiKey();
    const maxStart = 5000;
    const defaultLimit = 5000;
    const result: any[] = [];
    const platformSlug = PLATFORM_MAPPING[platform];

    try {
      let start = 1;

      while (start <= maxStart) {
        // API constraint: start + limit <= 5000
        // Calculate max allowed limit based on current start position
        const maxAllowedLimit = 5000 - start;
        if (maxAllowedLimit <= 0) {
          break;
        }

        const remaining = maxStart - start + 1;
        const pageLimit = Math.min(defaultLimit, remaining, maxAllowedLimit);
        
        try {
          const response = await axios.get(apiUrl, {
            params: {
              convert: 'USD',
              limit: pageLimit,
              start,
              cryptocurrency_type: 'tokens',
            },
            headers: { 'X-CMC_PRO_API_KEY': apiKey },
          });

          const responseData = response.data.data;
          if (!responseData || responseData.length === 0) {
            break;
          }

          responseData.forEach((d) => {
            // Support both Ethereum and BSC
            if (d.platform && d.platform.slug === platformSlug) {
              result.push({
                tokenAddress: d.platform.token_address.toLowerCase(),
                usd: d.quote.USD.price,
                timestamp: d.last_updated,
                provider: 'coinmarketcap',
              });
            }
          });

          start += responseData.length;

          if (responseData.length < pageLimit) {
            break;
          }
        } catch (error) {
          throw error;
        }
      }

      return result;
    } catch (error) {
      // Handle errors here
      throw error;
    }
  }

  private async getV1CryptocurrencyMapTokens(platform: string): Promise<any[]> {
    const apiUrl = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/map';
    const apiKey = this.getApiKey();
    const maxStart = 5000;
    const defaultLimit = 5000;
    const result: any[] = [];
    const platformSlug = PLATFORM_MAPPING[platform];
    const nativeTokenId = NATIVE_TOKEN_IDS[platform];

    try {
      let start = 1;

      while (start <= maxStart) {
        // API constraint: start + limit <= 5000
        // Calculate max allowed limit based on current start position
        const maxAllowedLimit = 5000 - start;
        if (maxAllowedLimit <= 0) {
          break;
        }

        const remaining = maxStart - start + 1;
        const pageLimit = Math.min(defaultLimit, remaining, maxAllowedLimit);
        
        try {
          const response = await axios.get(apiUrl, {
            params: {
              listing_status: 'active',
              limit: pageLimit,
              start,
            },
            headers: { 'X-CMC_PRO_API_KEY': apiKey },
          });

          const responseData = response.data.data;
          if (!responseData || responseData.length === 0) {
            break;
          }

          const platformTokens = responseData.filter(
            (token) => token.platform && token.platform.slug === platformSlug
          );

          result.push(...platformTokens);
          start += responseData.length;

          if (responseData.length < pageLimit) {
            break;
          }
        } catch (error) {
          throw error;
        }
      }

      result.push({
        id: nativeTokenId,
        platform: { token_address: NATIVE_TOKEN.toLowerCase() },
      });

      return result;
    } catch (error) {
      // Handle errors here
      throw error;
    }
  }

  private async getV2CryptocurrencyQuotesLatest(ids: number[], platform: string): Promise<any> {
    const apiUrl = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest';
    const apiKey = this.getApiKey();

    try {
      const response = await axios.get(apiUrl, {
        params: {
          convert: 'USD',
          id: ids.join(','),
        },
        headers: { 'X-CMC_PRO_API_KEY': apiKey },
      });

      const data = response.data.data;
      const result = [];
      Object.keys(data).forEach((key) => {
        const q = data[key];
        const tokenAddress = q.id === NATIVE_TOKEN_IDS[platform] ? NATIVE_TOKEN.toLowerCase() : q.platform.token_address.toLowerCase();
        result.push({
          tokenAddress,
          usd: q.quote.USD.price,
          timestamp: q.last_updated,
          provider: 'coinmarketcap',
        });
      });
      return result;
    } catch (error) {
      // Handle errors here
      throw error;
    }
  }

  async getHistoricalQuotes(
    tokenAddresses: string[],
    start: number,
    end: number,
    platform: string,
  ): Promise<{ [key: string]: PriceObject[] }> {
    try {
      const tokenIds = await this.getTokenIds(tokenAddresses, platform);

      const totalDataPoints = Math.ceil(((end - start) / (INTERVAL_IN_MINUTES * 60)) * tokenAddresses.length);
      const batches = Math.ceil(totalDataPoints / MAX_RESULTS_PER_CALL);
      const intervalInSeconds = Math.ceil((end - start) / batches);

      const requests = [];

      for (let i = 0; i < batches; i++) {
        const intervalStart = moment.unix(start + i * intervalInSeconds).toISOString(true);
        const intervalEnd = moment.unix(Math.min(start + (i + 1) * intervalInSeconds, end)).toISOString(true);

        const params = {
          id: tokenIds.join(','),
          time_start: intervalStart,
          time_end: intervalEnd,
          interval: '6h',
        };

        requests.push(this.getV3CryptocurrencyQuotesHistorical(params));
      }

      const responses: AxiosResponse[] = await Promise.all(requests);

      const result = {};
      responses.forEach((response) => {
        Object.keys(response.data.data).forEach((id) => {
          const tokenAddress = tokenAddresses[tokenIds.indexOf(id)];
          const prices = response.data.data[id].quotes.map((q) => {
            const { price, timestamp } = q.quote.USD;
            return { price, timestamp: toTimestamp(timestamp), address: tokenAddress.toLowerCase() };
          });

          result[tokenAddress] = (result[tokenAddress] || []).concat(prices);
        });
      });

      return result;
    } catch (error) {
      throw error;
    }
  }

  async getLatestQuotes(platform: string): Promise<any> {
    const latestQuotes = await this.getV1CryptocurrencyListingsLatest(platform);
    const eth = await this.getV2CryptocurrencyQuotesLatest([NATIVE_TOKEN_IDS[platform]], platform);
    return [...latestQuotes, ...eth];
  }

  async getAllTokens(platform: string): Promise<any[]> {
    return await this.getV1CryptocurrencyMapTokens(platform);
  }
}
