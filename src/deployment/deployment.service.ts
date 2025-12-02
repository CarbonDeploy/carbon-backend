// deployment.service.ts
import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { EventTypes } from '../events/event-types';

export const NATIVE_TOKEN = '0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE';

export enum BlockchainType {
  Ethereum = 'ethereum',
  Bsc = 'bsc',
}

export enum ExchangeId {
  OGEthereum = 'ethereum',
  Bsc = 'bsc',
}

export interface GasToken {
  name: string;
  symbol: string;
  address: string;
}

export interface Deployment {
  exchangeId: ExchangeId;
  blockchainType: BlockchainType;
  rpcEndpoint: string;
  harvestEventsBatchSize: number;
  harvestConcurrency: number;
  harvestSleep?: number;
  multicallAddress: string;
  gasToken: GasToken;
  startBlock: number;
  nativeTokenAlias?: string;
  mapEthereumTokens?: {
    [deploymentTokenAddress: string]: string;
  };
  pricingIgnoreList?: string[];
  tokenIgnoreList?: string[]; // Addresses to skip during token creation
  graphPriceAnchors?: {
    primary: {
      localAddress: string;
      ethereumAddress: string;
    };
    secondary?: {
      localAddress: string;
      ethereumAddress: string;
    };
  };
  contracts: {
    [contractName: string]: {
      address: string;
    };
  };
  notifications?: {
    explorerUrl: string;
    carbonWalletUrl: string;
    disabledEvents?: EventTypes[];
    regularGroupEvents?: EventTypes[];
    title: string;
    telegram: {
      botToken: string;
      bancorProtectionToken?: string;
      threads: {
        carbonThreadId?: number;
        fastlaneId?: number;
        vortexId?: number;
        bancorProtectionId?: number;
      };
    };
  };
}

export type LowercaseTokenMap = { [lowercaseAddress: string]: string };

@Injectable()
export class DeploymentService {
  private deployments: Deployment[];
  constructor(private configService: ConfigService) {
    this.deployments = this.initializeDeployments();
  }

  private initializeDeployments(): Deployment[] {
    return [
      {
        exchangeId: ExchangeId.Bsc,
        blockchainType: BlockchainType.Bsc,
        rpcEndpoint: this.configService.get('BSC_RPC_ENDPOINT'),
        harvestEventsBatchSize: 5000, // Optimized for faster processing
        harvestConcurrency: 3,
        harvestSleep: 2000, // Reduced delay for faster processing
        multicallAddress: '0xcA11bde05977b3631167028862bE2a173976CA11',
        startBlock: parseInt(this.configService.get('BSC_START_BLOCK')) || 62404561, 
        gasToken: {
          name: 'Binance Coin',
          symbol: 'BNB',
          address: NATIVE_TOKEN,
        },
        nativeTokenAlias: '0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c', // WBNB
        contracts: {
          CarbonController: {
            address: '0xafc43faE32302D725fC4d448525c44c522a9a1B9',
          },
          CarbonVortex: {
            address: '0x12248C95a85fc343e1D69772c2d04f4bF7327EAb',
          },
          // CarbonPOL: {
          //   address: 'YOUR_BSC_CARBON_POL_ADDRESS',
          // },
          CarbonVoucher: {
            address: '0x94A62c18786Bc7e808a26285602296E438FF4B5c',
          },
        },
        mapEthereumTokens: {
          '0x55d398326f99059ff775485246999027b3197955': '0xdAC17F958D2ee523a2206206994597C13D831ec7', // USDT BSC -> USDT ETH
          '0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d': '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48', // USDC BSC -> USDC ETH
          '0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c': '0x418D75f65a02b3D53B2418FB8E1fe493759c7605', // WBNB BSC -> WBNB ETH
          "0x2170ed0880ac9a755fd29b2688956bd959f933f8": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",  // Binance-Peg ETH (WETH)
          "0x7130d2a12b9bcbfae4f2634d864a1ee1ce3ead9c": "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599",  // BTCB -> WBTC
          "0xe9e7cea3dedca5984780bafc599bd69add087d56": "0x4fabb145d64652a948d72533023f6e7a623c7c53",  // BUSD
          "0xf8a0bf9cf54bb92f17374d9e9a321e6a111a51bd": "0x514910771af9ca656af840dff83e8264ecf986ca",  // LINK
          "0xbf5140a22578168fd562dccf235e5d43a02ce9b1": "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984",  // UNI
          "0xfb6115445bff7b52feb98650c87f44907e58f802": "0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9",  // AAVE
          "0x52ce071bd9b1c4b00a0b92d298c512478cad67e8": "0xc00e94cb662c3520282e6f5717214004a7f26888",  // COMP
          "0x947950bcc74888a40ffa2593c5798f11fc9124c4": "0x6b3595068778dd592e39a122f4f5a5cf09c90fe2",   // SUSHI
          "0x8d0d000ee44948fc98c9b98a4fa4921476f08b0d": "0x8d0d000ee44948fc98c9b98a4fa4921476f08b0d",  // USD1
        },
        tokenIgnoreList: [
          '0xC3c4f4be7075b11C43D01AE0Daf77b4362D1326f',
          '0xe7E4fE29540CAa46329C1B00EE40680220aB479e', // Invalid token address that causes NaN errors
          '0x510ff0B69b1c8d31181Af13CED8d5bCAf9b17f91',
          '0xee382fEb41732Cc7528256E2ABA6b76aE0613D2f',
          '0x7C3DbBe067538706E9f6d9dE59Ac7eAd6D2b841B',
          '0xdBe15ecB0c2d60e57288869D57a0BD288Bb69272',
        ],
        notifications: {
          explorerUrl: 'https://bscscan.com/tx/',
          carbonWalletUrl: this.configService.get('BSC_CARBON_WALLET_URL') || 'https://app.carbondefi.xyz/wallet/',
          title: 'BSC',
          telegram: {
            botToken: this.configService.get('BSC_TELEGRAM_BOT_TOKEN'),
            threads: {
              carbonThreadId: parseInt(this.configService.get('BSC_CARBON_THREAD_ID')),
              fastlaneId: parseInt(this.configService.get('BSC_FASTLANE_THREAD_ID')),
              vortexId: parseInt(this.configService.get('BSC_VORTEX_THREAD_ID')),
            },
          },
        },
      },
      {
        exchangeId: ExchangeId.OGEthereum,
        blockchainType: BlockchainType.Ethereum,
        rpcEndpoint: this.configService.get('ETHEREUM_RPC_ENDPOINT'),
        harvestEventsBatchSize: 1000,
        harvestConcurrency: 10,
        multicallAddress: '0x5Eb3fa2DFECdDe21C950813C665E9364fa609bD2',
        startBlock: 23439972,
        gasToken: {
          name: 'Ethereum',
          symbol: 'ETH',
          address: '0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE',
        },
        contracts: {
          CarbonController: {
            address: '0xC537e898CD774e2dCBa3B14Ea6f34C93d5eA45e1',
          },
          CarbonVortex: {
            address: '0xD053Dcd7037AF7204cecE544Ea9F227824d79801',
          },
          CarbonPOL: {
            address: '0xD06146D292F9651C1D7cf54A3162791DFc2bEf46',
          },
          CarbonVoucher: {
            address: '0x3660F04B79751e31128f6378eAC70807e38f554E',
          },
          BancorArbitrage: {
            address: '0x41Eeba3355d7D6FF628B7982F3F9D055c39488cB',
          },
          BancorArbitrageV2: {
            address: '0x0f54099D787e26c90c487625B4dE819eC5A9BDAA',
          },
          LiquidityProtectionStore: {
            address: '0xf5FAB5DBD2f3bf675dE4cB76517d4767013cfB55',
          },
        },
        notifications: {
          explorerUrl: this.configService.get('ETHEREUM_EXPLORER_URL'),
          carbonWalletUrl: this.configService.get('ETHEREUM_CARBON_WALLET_URL'),
          title: 'Ethereum',
          regularGroupEvents: [EventTypes.ProtectionRemovedEvent],
          telegram: {
            botToken: this.configService.get('ETHEREUM_TELEGRAM_BOT_TOKEN'),
            bancorProtectionToken: this.configService.get('ETHEREUM_BANCOR_PROTECTION_TOKEN'),
            threads: {
              carbonThreadId: this.configService.get('ETHEREUM_CARBON_THREAD_ID'),
              fastlaneId: this.configService.get('ETHEREUM_FASTLANE_THREAD_ID'),
              vortexId: this.configService.get('ETHEREUM_VORTEX_THREAD_ID'),
              bancorProtectionId: this.configService.get('ETHEREUM_BANCOR_PROTECTION_THREAD_ID'),
            },
          },
        },
        mapEthereumTokens: {
          '0xfc60fc0145d7330e5abcfc52af7b043a1ce18e7d': '0xfc60fc0145d7330e5abcfc52af7b043a1ce18e7d', // governer self mapping
          // '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48' : '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48', // usdc self mapping
          // '0xdAC17F958D2ee523a2206206994597C13D831ec7' : '0xdAC17F958D2ee523a2206206994597C13D831ec7', // usdt self mapping

        },
        pricingIgnoreList: [
          '0x44d13160094b45f39b712843d887939027513129',
          '0x251ee69eB945B79fb991B268690f1A43eD2A859d',
          '0x3cda61B56278842876e7fDD56123d83DBAFAe16C',
          '0xe2ec2b77a30743ebd746e4e18c5bfe665cd70222',
        ],
      }
    ];
  }

  getDeployments(): Deployment[] {
    return this.deployments;
  }

  getDeploymentByExchangeId(exchangeId: ExchangeId): Deployment {
    const deployment = this.deployments.find((d) => d.exchangeId === exchangeId);
    if (!deployment) {
      throw new Error(`Deployment for exchangeId ${exchangeId} not found`);
    }
    return deployment;
  }

  getDeploymentByBlockchainType(blockchainType: BlockchainType): Deployment {
    const deployment = this.deployments.find((d) => d.blockchainType === blockchainType);
    if (!deployment) {
      throw new Error(`Deployment not found for blockchain type: ${blockchainType}`);
    }
    return deployment;
  }

  getLowercaseTokenMap(deployment: Deployment): LowercaseTokenMap {
    if (!deployment.mapEthereumTokens) {
      return {};
    }

    return Object.entries(deployment.mapEthereumTokens).reduce((acc, [key, value]) => {
      acc[key.toLowerCase()] = value.toLowerCase();
      return acc;
    }, {});
  }

  /**
   * Checks if a token address should be ignored from pricing operations
   * @param deployment - The deployment configuration
   * @param tokenAddress - The token address to check (case-insensitive)
   * @returns true if the token should be ignored from pricing
   */
  isTokenIgnoredFromPricing(deployment: Deployment, tokenAddress: string): boolean {
    if (!deployment.pricingIgnoreList || deployment.pricingIgnoreList.length === 0) {
      return false;
    }

    const lowercaseAddress = tokenAddress.toLowerCase();
    return deployment.pricingIgnoreList.some((ignoredAddress) => ignoredAddress.toLowerCase() === lowercaseAddress);
  }

  /**
   * Checks if a token address should be ignored from token creation operations
   * @param deployment - The deployment configuration
   * @param tokenAddress - The token address to check (case-insensitive)
   * @returns true if the token should be ignored from creation
   */
  isTokenIgnoredFromCreation(deployment: Deployment, tokenAddress: string): boolean {
    if (!deployment.tokenIgnoreList || deployment.tokenIgnoreList.length === 0) {
      return false;
    }

    const lowercaseAddress = tokenAddress.toLowerCase();
    return deployment.tokenIgnoreList.some((ignoredAddress) => ignoredAddress.toLowerCase() === lowercaseAddress);
  }
}
