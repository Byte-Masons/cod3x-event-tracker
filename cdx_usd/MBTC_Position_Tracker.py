from web3 import Web3
from web3.middleware import geth_poa_middleware 
import pandas as pd
import time
import sys
import sqlite3
# sys.path.append("..")  # Add the root directory to the search path
from lending_pool import transaction_finder as tf
from lending_pool import balance_and_points as bp
from lending_pool import lending_pool_helper as lph
from sql_interfacer import sql
from cloud_storage import cloud_storage as cs

DATA_PROVIDER_V2_ABI = [{"inputs":[{"internalType":"contract IChainlinkAggregator","name":"_networkBaseTokenPriceInUsdProxyAggregator","type":"address"},{"internalType":"contract IChainlinkAggregator","name":"_marketReferenceCurrencyPriceInUsdProxyAggregator","type":"address"}],"stateMutability":"nonpayable","type":"constructor"},{"inputs":[],"name":"ETH_CURRENCY_UNIT","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"MKRAddress","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"bytes32","name":"_bytes32","type":"bytes32"}],"name":"bytes32ToString","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"pure","type":"function"},{"inputs":[{"internalType":"contract ILendingPoolAddressesProvider","name":"provider","type":"address"}],"name":"getReservesData","outputs":[{"components":[{"internalType":"address","name":"underlyingAsset","type":"address"},{"internalType":"string","name":"name","type":"string"},{"internalType":"string","name":"symbol","type":"string"},{"internalType":"uint256","name":"decimals","type":"uint256"},{"internalType":"uint256","name":"baseLTVasCollateral","type":"uint256"},{"internalType":"uint256","name":"reserveLiquidationThreshold","type":"uint256"},{"internalType":"uint256","name":"reserveLiquidationBonus","type":"uint256"},{"internalType":"uint256","name":"reserveFactor","type":"uint256"},{"internalType":"bool","name":"usageAsCollateralEnabled","type":"bool"},{"internalType":"bool","name":"borrowingEnabled","type":"bool"},{"internalType":"bool","name":"stableBorrowRateEnabled","type":"bool"},{"internalType":"bool","name":"isActive","type":"bool"},{"internalType":"bool","name":"isFrozen","type":"bool"},{"internalType":"uint128","name":"liquidityIndex","type":"uint128"},{"internalType":"uint128","name":"variableBorrowIndex","type":"uint128"},{"internalType":"uint128","name":"liquidityRate","type":"uint128"},{"internalType":"uint128","name":"variableBorrowRate","type":"uint128"},{"internalType":"uint128","name":"stableBorrowRate","type":"uint128"},{"internalType":"uint40","name":"lastUpdateTimestamp","type":"uint40"},{"internalType":"address","name":"aTokenAddress","type":"address"},{"internalType":"address","name":"stableDebtTokenAddress","type":"address"},{"internalType":"address","name":"variableDebtTokenAddress","type":"address"},{"internalType":"address","name":"interestRateStrategyAddress","type":"address"},{"internalType":"uint256","name":"availableLiquidity","type":"uint256"},{"internalType":"uint256","name":"totalPrincipalStableDebt","type":"uint256"},{"internalType":"uint256","name":"averageStableRate","type":"uint256"},{"internalType":"uint256","name":"stableDebtLastUpdateTimestamp","type":"uint256"},{"internalType":"uint256","name":"totalScaledVariableDebt","type":"uint256"},{"internalType":"uint256","name":"priceInMarketReferenceCurrency","type":"uint256"},{"internalType":"uint256","name":"variableRateSlope1","type":"uint256"},{"internalType":"uint256","name":"variableRateSlope2","type":"uint256"},{"internalType":"uint256","name":"stableRateSlope1","type":"uint256"},{"internalType":"uint256","name":"stableRateSlope2","type":"uint256"},{"internalType":"uint256","name":"optimalUtilizationRate","type":"uint256"}],"internalType":"struct IUiPoolDataProviderV2.AggregatedReserveData[]","name":"","type":"tuple[]"},{"components":[{"internalType":"uint256","name":"marketReferenceCurrencyUnit","type":"uint256"},{"internalType":"int256","name":"marketReferenceCurrencyPriceInUsd","type":"int256"},{"internalType":"int256","name":"networkBaseTokenPriceInUsd","type":"int256"},{"internalType":"uint8","name":"networkBaseTokenPriceDecimals","type":"uint8"}],"internalType":"struct IUiPoolDataProviderV2.BaseCurrencyInfo","name":"","type":"tuple"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"contract ILendingPoolAddressesProvider","name":"provider","type":"address"}],"name":"getReservesList","outputs":[{"internalType":"address[]","name":"","type":"address[]"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"contract ILendingPoolAddressesProvider","name":"provider","type":"address"},{"internalType":"address","name":"user","type":"address"}],"name":"getUserReservesData","outputs":[{"components":[{"internalType":"address","name":"underlyingAsset","type":"address"},{"internalType":"uint256","name":"scaledATokenBalance","type":"uint256"},{"internalType":"bool","name":"usageAsCollateralEnabledOnUser","type":"bool"},{"internalType":"uint256","name":"stableBorrowRate","type":"uint256"},{"internalType":"uint256","name":"scaledVariableDebt","type":"uint256"},{"internalType":"uint256","name":"principalStableDebt","type":"uint256"},{"internalType":"uint256","name":"stableBorrowLastUpdateTimestamp","type":"uint256"}],"internalType":"struct IUiPoolDataProviderV2.UserReserveData[]","name":"","type":"tuple[]"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"marketReferenceCurrencyPriceInUsdProxyAggregator","outputs":[{"internalType":"contract IChainlinkAggregator","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"networkBaseTokenPriceInUsdProxyAggregator","outputs":[{"internalType":"contract IChainlinkAggregator","name":"","type":"address"}],"stateMutability":"view","type":"function"}]

DATA_PROVIDER_V2_ADDRESS = '0xb70a71Cb4184B95D3B77Fdc572F6008b10709C68'

LENDING_POOL_ADDRESS_PROVIDER_ADDRESS = '0xEDc83309549e36f3c7FD8c2C5C54B4c8e5FA00FC'

RPC_URL = 'https://mainnet.mode.network'

WAIT_TIME = 0.3

# # makes a list of unique users from reading from the cloud
def get_icl_transactors():

    file_list = ['ironclad_lend_events.zip', 'ironclad_2_lend_events.zip']

    df_list = []

    for file in file_list:
        df = cs.read_zip_csv_from_cloud_storage(file, 'cooldowns2')

        df_list.append(df)

        time.sleep(5)
    
    df = pd.concat(df_list)
    unique_user_list = df['to_address'].unique()

    return unique_user_list

# # makes a dataframe of the unique addresses and writes to icl_users.csv
def make_unique_address_df(unique_user_list):
    print(unique_user_list)

    df = pd.DataFrame()

    df['address'] = unique_user_list

    print(df)

    df.to_csv('icl_users.csv', index=False)

    return df

def get_user_reserves_data(contract, user_address):
    try:
        output = contract.functions.getUserReservesData(LENDING_POOL_ADDRESS_PROVIDER_ADDRESS, user_address).call()
        
        # Create a list to store each reserve's data
        reserves_data = []
        
        for reserve in output:
            data = {
                'underlyingAsset': reserve[0],
                'scaledATokenBalance': reserve[1],
                'usageAsCollateralEnabled': reserve[2],
                'stableBorrowRate': reserve[3],
                'scaledVariableDebt': reserve[4],
                'principalStableDebt': reserve[5],
                'stableBorrowLastUpdateTimestamp': reserve[6]
            }
            reserves_data.append(data)
        
        # Convert to DataFrame
        df = pd.DataFrame(reserves_data)
        
        # Optional: Convert Wei values to Ether for better readability
        # wei_columns = ['scaledATokenBalance', 'scaledVariableDebt', 'principalStableDebt']
        # for col in wei_columns:
        #     df[f'{col}_ETH'] = df[col] / 1e18
            
        return df
        
    except Exception as e:
        print(f"Error: {e}")
        return pd.DataFrame()  # Return empty DataFrame on error


def run_all():
    # unique_user_list = get_icl_transactors()

    # df = make_unique_address_df(unique_user_list)

    df = pd.read_csv('icl_users.csv')
    unique_user_address_list = df['address'].unique()

    web3 = lph.get_web_3(RPC_URL)

    contract = lph.get_contract(DATA_PROVIDER_V2_ADDRESS, DATA_PROVIDER_V2_ABI, web3)

    df_list = []

    i = 1

    # # finds data for each historical user
    for unique_user_address in unique_user_address_list:
        df = get_user_reserves_data(contract, unique_user_address)

        if len(df) > 0:
            df['user_address'] = unique_user_address

            df = df[['user_address', 'underlyingAsset', 'scaledATokenBalance', 'scaledVariableDebt']]

            df_list.append(df)
            print(df)

        else:
            print('No Data Found: ', unique_user_address)

        print('Remaining: ', i, '/', len(unique_user_address_list), 'or ', str( ( len(unique_user_address_list)-i ) *WAIT_TIME), ' seconds left' )

        i += 1

        time.sleep(WAIT_TIME)

    df = pd.concat(df_list)

    df.to_csv('icl_user_balance_data.csv', index=False)

    return df