import pandas as pd
from web3 import Web3
from web3.middleware import geth_poa_middleware 
from cloud_storage import cloud_storage as cs
import time
from sql_interfacer import sql

def get_lp_config_df():
    csv_file_path = "./config/lp_config.csv"
    lp_config_df = pd.read_csv(csv_file_path)

    return lp_config_df

# # reads from our static config csv
def get_token_config_df():
    token_config_df = pd.read_csv('./config/token_config.csv')

    return token_config_df

# # returns our treasury_config_df
def get_treasury_config_df():
    treasury_config_df = pd.read_csv('./config/treasury_config.csv')

    return treasury_config_df

# # returns our cdp_config_df
def get_cdp_config_df():
    cdp_config_df = pd.read_csv('./config/cdp_config.csv')

    return cdp_config_df

# # returns our relevant cdp configuration values
def get_cdp_config_value(column_name, index):
    df = get_cdp_config_df()

    config_list = df[column_name].tolist()

    try:
        config_value = config_list[index]
    except:
        config_value = config_list[0]

    return config_value

# # returns the relevant value from our config csv
def get_lp_config_value(column_name, index):
    df = get_lp_config_df()
    df = df.loc[df['index'] == index]

    config_list = df[column_name].tolist()

    try:
        config_value = config_list[index]
    except:
        config_value = config_list[0]

    return config_value

# # returns our token_config value
def get_token_config_value(column_name, token_address, index):
    df = get_token_config_df()

    df = df.loc[df['chain_index'] == index]

    temp_df = df.loc[df['token_address'] == token_address]

    if len(temp_df) < 1:
        temp_df = df.loc[df['underlying_address'] == token_address]

    config_list = temp_df[column_name].tolist()

    config_value = config_list[0]

    return config_value

# # returns the relevant value from our treasury_config csv
def get_treasury_config_value(column_name, index):
    df = get_treasury_config_df()

    config_list = df[column_name].tolist()

    try:
        config_value = config_list[index]
    except:
        config_value = config_list[0]

    return config_value

def get_lending_pool_abi():
    abi = [{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"reserve","type":"address"},{"indexed":False,"internalType":"address","name":"user","type":"address"},{"indexed":True,"internalType":"address","name":"onBehalfOf","type":"address"},{"indexed":False,"internalType":"uint256","name":"amount","type":"uint256"},{"indexed":False,"internalType":"uint256","name":"borrowRateMode","type":"uint256"},{"indexed":False,"internalType":"uint256","name":"borrowRate","type":"uint256"},{"indexed":True,"internalType":"uint16","name":"referral","type":"uint16"}],"name":"Borrow","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"reserve","type":"address"},{"indexed":False,"internalType":"address","name":"user","type":"address"},{"indexed":True,"internalType":"address","name":"onBehalfOf","type":"address"},{"indexed":False,"internalType":"uint256","name":"amount","type":"uint256"},{"indexed":True,"internalType":"uint16","name":"referral","type":"uint16"}],"name":"Deposit","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"target","type":"address"},{"indexed":True,"internalType":"address","name":"initiator","type":"address"},{"indexed":True,"internalType":"address","name":"asset","type":"address"},{"indexed":False,"internalType":"uint256","name":"amount","type":"uint256"},{"indexed":False,"internalType":"uint256","name":"premium","type":"uint256"},{"indexed":False,"internalType":"uint16","name":"referralCode","type":"uint16"}],"name":"FlashLoan","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"collateralAsset","type":"address"},{"indexed":True,"internalType":"address","name":"debtAsset","type":"address"},{"indexed":True,"internalType":"address","name":"user","type":"address"},{"indexed":False,"internalType":"uint256","name":"debtToCover","type":"uint256"},{"indexed":False,"internalType":"uint256","name":"liquidatedCollateralAmount","type":"uint256"},{"indexed":False,"internalType":"address","name":"liquidator","type":"address"},{"indexed":False,"internalType":"bool","name":"receiveAToken","type":"bool"}],"name":"LiquidationCall","type":"event"},{"anonymous":False,"inputs":[],"name":"Paused","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"reserve","type":"address"},{"indexed":True,"internalType":"address","name":"user","type":"address"}],"name":"RebalanceStableBorrowRate","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"reserve","type":"address"},{"indexed":True,"internalType":"address","name":"user","type":"address"},{"indexed":True,"internalType":"address","name":"repayer","type":"address"},{"indexed":False,"internalType":"uint256","name":"amount","type":"uint256"}],"name":"Repay","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"reserve","type":"address"},{"indexed":False,"internalType":"uint256","name":"liquidityRate","type":"uint256"},{"indexed":False,"internalType":"uint256","name":"stableBorrowRate","type":"uint256"},{"indexed":False,"internalType":"uint256","name":"variableBorrowRate","type":"uint256"},{"indexed":False,"internalType":"uint256","name":"liquidityIndex","type":"uint256"},{"indexed":False,"internalType":"uint256","name":"variableBorrowIndex","type":"uint256"}],"name":"ReserveDataUpdated","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"reserve","type":"address"},{"indexed":True,"internalType":"address","name":"user","type":"address"}],"name":"ReserveUsedAsCollateralDisabled","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"reserve","type":"address"},{"indexed":True,"internalType":"address","name":"user","type":"address"}],"name":"ReserveUsedAsCollateralEnabled","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"reserve","type":"address"},{"indexed":True,"internalType":"address","name":"user","type":"address"},{"indexed":False,"internalType":"uint256","name":"rateMode","type":"uint256"}],"name":"Swap","type":"event"},{"anonymous":False,"inputs":[],"name":"Unpaused","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"reserve","type":"address"},{"indexed":True,"internalType":"address","name":"user","type":"address"},{"indexed":True,"internalType":"address","name":"to","type":"address"},{"indexed":False,"internalType":"uint256","name":"amount","type":"uint256"}],"name":"Withdraw","type":"event"},{"inputs":[],"name":"FLASHLOAN_PREMIUM_TOTAL","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"LENDINGPOOL_REVISION","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"MAX_NUMBER_RESERVES","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"MAX_STABLE_RATE_BORROW_SIZE_PERCENT","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"asset","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"},{"internalType":"uint256","name":"interestRateMode","type":"uint256"},{"internalType":"uint16","name":"referralCode","type":"uint16"},{"internalType":"address","name":"onBehalfOf","type":"address"}],"name":"borrow","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"asset","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"},{"internalType":"address","name":"onBehalfOf","type":"address"},{"internalType":"uint16","name":"referralCode","type":"uint16"}],"name":"deposit","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"asset","type":"address"},{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"},{"internalType":"uint256","name":"balanceFromBefore","type":"uint256"},{"internalType":"uint256","name":"balanceToBefore","type":"uint256"}],"name":"finalizeTransfer","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"receiverAddress","type":"address"},{"internalType":"address[]","name":"assets","type":"address[]"},{"internalType":"uint256[]","name":"amounts","type":"uint256[]"},{"internalType":"uint256[]","name":"modes","type":"uint256[]"},{"internalType":"address","name":"onBehalfOf","type":"address"},{"internalType":"bytes","name":"params","type":"bytes"},{"internalType":"uint16","name":"referralCode","type":"uint16"}],"name":"flashLoan","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"getAddressesProvider","outputs":[{"internalType":"contract ILendingPoolAddressesProvider","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"asset","type":"address"}],"name":"getConfiguration","outputs":[{"components":[{"internalType":"uint256","name":"data","type":"uint256"}],"internalType":"struct DataTypes.ReserveConfigurationMap","name":"","type":"tuple"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"asset","type":"address"}],"name":"getReserveData","outputs":[{"components":[{"components":[{"internalType":"uint256","name":"data","type":"uint256"}],"internalType":"struct DataTypes.ReserveConfigurationMap","name":"configuration","type":"tuple"},{"internalType":"uint128","name":"liquidityIndex","type":"uint128"},{"internalType":"uint128","name":"variableBorrowIndex","type":"uint128"},{"internalType":"uint128","name":"currentLiquidityRate","type":"uint128"},{"internalType":"uint128","name":"currentVariableBorrowRate","type":"uint128"},{"internalType":"uint128","name":"currentStableBorrowRate","type":"uint128"},{"internalType":"uint40","name":"lastUpdateTimestamp","type":"uint40"},{"internalType":"address","name":"aTokenAddress","type":"address"},{"internalType":"address","name":"stableDebtTokenAddress","type":"address"},{"internalType":"address","name":"variableDebtTokenAddress","type":"address"},{"internalType":"address","name":"interestRateStrategyAddress","type":"address"},{"internalType":"uint8","name":"id","type":"uint8"}],"internalType":"struct DataTypes.ReserveData","name":"","type":"tuple"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"asset","type":"address"}],"name":"getReserveNormalizedIncome","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"asset","type":"address"}],"name":"getReserveNormalizedVariableDebt","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"getReservesList","outputs":[{"internalType":"address[]","name":"","type":"address[]"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"user","type":"address"}],"name":"getUserAccountData","outputs":[{"internalType":"uint256","name":"totalCollateralETH","type":"uint256"},{"internalType":"uint256","name":"totalDebtETH","type":"uint256"},{"internalType":"uint256","name":"availableBorrowsETH","type":"uint256"},{"internalType":"uint256","name":"currentLiquidationThreshold","type":"uint256"},{"internalType":"uint256","name":"ltv","type":"uint256"},{"internalType":"uint256","name":"healthFactor","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"user","type":"address"}],"name":"getUserConfiguration","outputs":[{"components":[{"internalType":"uint256","name":"data","type":"uint256"}],"internalType":"struct DataTypes.UserConfigurationMap","name":"","type":"tuple"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"asset","type":"address"},{"internalType":"address","name":"aTokenAddress","type":"address"},{"internalType":"address","name":"stableDebtAddress","type":"address"},{"internalType":"address","name":"variableDebtAddress","type":"address"},{"internalType":"address","name":"interestRateStrategyAddress","type":"address"}],"name":"initReserve","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"contract ILendingPoolAddressesProvider","name":"provider","type":"address"}],"name":"initialize","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"collateralAsset","type":"address"},{"internalType":"address","name":"debtAsset","type":"address"},{"internalType":"address","name":"user","type":"address"},{"internalType":"uint256","name":"debtToCover","type":"uint256"},{"internalType":"bool","name":"receiveAToken","type":"bool"}],"name":"liquidationCall","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"paused","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"asset","type":"address"},{"internalType":"address","name":"user","type":"address"}],"name":"rebalanceStableBorrowRate","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"asset","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"},{"internalType":"uint256","name":"rateMode","type":"uint256"},{"internalType":"address","name":"onBehalfOf","type":"address"}],"name":"repay","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"asset","type":"address"},{"internalType":"uint256","name":"configuration","type":"uint256"}],"name":"setConfiguration","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"bool","name":"val","type":"bool"}],"name":"setPause","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"asset","type":"address"},{"internalType":"address","name":"rateStrategyAddress","type":"address"}],"name":"setReserveInterestRateStrategyAddress","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"asset","type":"address"},{"internalType":"bool","name":"useAsCollateral","type":"bool"}],"name":"setUserUseReserveAsCollateral","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"asset","type":"address"},{"internalType":"uint256","name":"rateMode","type":"uint256"}],"name":"swapBorrowRateMode","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"asset","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"},{"internalType":"address","name":"to","type":"address"}],"name":"withdraw","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"nonpayable","type":"function"}]

    return abi

def get_aave_oracle_abi():
    abi = [{"inputs":[{"internalType":"address[]","name":"assets","type":"address[]"},{"internalType":"address[]","name":"sources","type":"address[]"},{"internalType":"address","name":"fallbackOracle","type":"address"},{"internalType":"address","name":"baseCurrency","type":"address"},{"internalType":"uint256","name":"baseCurrencyUnit","type":"uint256"}],"stateMutability":"nonpayable","type":"constructor"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"asset","type":"address"},{"indexed":True,"internalType":"address","name":"source","type":"address"}],"name":"AssetSourceUpdated","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"baseCurrency","type":"address"},{"indexed":False,"internalType":"uint256","name":"baseCurrencyUnit","type":"uint256"}],"name":"BaseCurrencySet","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"fallbackOracle","type":"address"}],"name":"FallbackOracleUpdated","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"previousOwner","type":"address"},{"indexed":True,"internalType":"address","name":"newOwner","type":"address"}],"name":"OwnershipTransferred","type":"event"},{"inputs":[],"name":"BASE_CURRENCY","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"BASE_CURRENCY_UNIT","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"asset","type":"address"}],"name":"getAssetPrice","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address[]","name":"assets","type":"address[]"}],"name":"getAssetsPrices","outputs":[{"internalType":"uint256[]","name":"","type":"uint256[]"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"getFallbackOracle","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"asset","type":"address"}],"name":"getSourceOfAsset","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"owner","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"renounceOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address[]","name":"assets","type":"address[]"},{"internalType":"address[]","name":"sources","type":"address[]"}],"name":"setAssetSources","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"fallbackOracle","type":"address"}],"name":"setFallbackOracle","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"newOwner","type":"address"}],"name":"transferOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"}]

    return abi

def get_a_token_abi():
    abi = [{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"owner","type":"address"},{"indexed":True,"internalType":"address","name":"spender","type":"address"},{"indexed":False,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Approval","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"from","type":"address"},{"indexed":True,"internalType":"address","name":"to","type":"address"},{"indexed":False,"internalType":"uint256","name":"value","type":"uint256"},{"indexed":False,"internalType":"uint256","name":"index","type":"uint256"}],"name":"BalanceTransfer","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"from","type":"address"},{"indexed":True,"internalType":"address","name":"target","type":"address"},{"indexed":False,"internalType":"uint256","name":"value","type":"uint256"},{"indexed":False,"internalType":"uint256","name":"index","type":"uint256"}],"name":"Burn","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"underlyingAsset","type":"address"},{"indexed":True,"internalType":"address","name":"pool","type":"address"},{"indexed":False,"internalType":"address","name":"treasury","type":"address"},{"indexed":False,"internalType":"address","name":"incentivesController","type":"address"},{"indexed":False,"internalType":"uint8","name":"aTokenDecimals","type":"uint8"},{"indexed":False,"internalType":"string","name":"aTokenName","type":"string"},{"indexed":False,"internalType":"string","name":"aTokenSymbol","type":"string"},{"indexed":False,"internalType":"bytes","name":"params","type":"bytes"}],"name":"Initialized","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"from","type":"address"},{"indexed":False,"internalType":"uint256","name":"value","type":"uint256"},{"indexed":False,"internalType":"uint256","name":"index","type":"uint256"}],"name":"Mint","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"from","type":"address"},{"indexed":True,"internalType":"address","name":"to","type":"address"},{"indexed":False,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Transfer","type":"event"},{"inputs":[],"name":"ATOKEN_REVISION","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"DOMAIN_SEPARATOR","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"EIP712_REVISION","outputs":[{"internalType":"bytes","name":"","type":"bytes"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"PERMIT_TYPEHASH","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"POOL","outputs":[{"internalType":"contract ILendingPool","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"RESERVE_TREASURY_ADDRESS","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"UNDERLYING_ASSET_ADDRESS","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"_nonces","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"owner","type":"address"},{"internalType":"address","name":"spender","type":"address"}],"name":"allowance","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"}],"name":"approve","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"user","type":"address"}],"name":"balanceOf","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"user","type":"address"},{"internalType":"address","name":"receiverOfUnderlying","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"},{"internalType":"uint256","name":"index","type":"uint256"}],"name":"burn","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"decimals","outputs":[{"internalType":"uint8","name":"","type":"uint8"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"subtractedValue","type":"uint256"}],"name":"decreaseAllowance","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"getIncentivesController","outputs":[{"internalType":"contract IAaveIncentivesController","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"user","type":"address"}],"name":"getScaledUserBalanceAndSupply","outputs":[{"internalType":"uint256","name":"","type":"uint256"},{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"user","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"}],"name":"handleRepayment","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"addedValue","type":"uint256"}],"name":"increaseAllowance","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"contract ILendingPool","name":"pool","type":"address"},{"internalType":"address","name":"treasury","type":"address"},{"internalType":"address","name":"underlyingAsset","type":"address"},{"internalType":"contract IAaveIncentivesController","name":"incentivesController","type":"address"},{"internalType":"uint8","name":"aTokenDecimals","type":"uint8"},{"internalType":"string","name":"aTokenName","type":"string"},{"internalType":"string","name":"aTokenSymbol","type":"string"},{"internalType":"bytes","name":"params","type":"bytes"}],"name":"initialize","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"user","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"},{"internalType":"uint256","name":"index","type":"uint256"}],"name":"mint","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"amount","type":"uint256"},{"internalType":"uint256","name":"index","type":"uint256"}],"name":"mintToTreasury","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"name","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"owner","type":"address"},{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"},{"internalType":"uint256","name":"deadline","type":"uint256"},{"internalType":"uint8","name":"v","type":"uint8"},{"internalType":"bytes32","name":"r","type":"bytes32"},{"internalType":"bytes32","name":"s","type":"bytes32"}],"name":"permit","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"user","type":"address"}],"name":"scaledBalanceOf","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"scaledTotalSupply","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"symbol","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"totalSupply","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"}],"name":"transfer","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"sender","type":"address"},{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"}],"name":"transferFrom","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"}],"name":"transferOnLiquidation","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"target","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"}],"name":"transferUnderlyingTo","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"nonpayable","type":"function"}]    
    return abi

def get_v_token_abi():
    abi = [{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"owner","type":"address"},{"indexed":True,"internalType":"address","name":"spender","type":"address"},{"indexed":False,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Approval","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"from","type":"address"},{"indexed":True,"internalType":"address","name":"to","type":"address"},{"indexed":False,"internalType":"uint256","name":"value","type":"uint256"},{"indexed":False,"internalType":"uint256","name":"index","type":"uint256"}],"name":"BalanceTransfer","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"from","type":"address"},{"indexed":True,"internalType":"address","name":"target","type":"address"},{"indexed":False,"internalType":"uint256","name":"value","type":"uint256"},{"indexed":False,"internalType":"uint256","name":"index","type":"uint256"}],"name":"Burn","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"underlyingAsset","type":"address"},{"indexed":True,"internalType":"address","name":"pool","type":"address"},{"indexed":False,"internalType":"address","name":"treasury","type":"address"},{"indexed":False,"internalType":"address","name":"incentivesController","type":"address"},{"indexed":False,"internalType":"uint8","name":"aTokenDecimals","type":"uint8"},{"indexed":False,"internalType":"string","name":"aTokenName","type":"string"},{"indexed":False,"internalType":"string","name":"aTokenSymbol","type":"string"},{"indexed":False,"internalType":"bytes","name":"params","type":"bytes"}],"name":"Initialized","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"from","type":"address"},{"indexed":False,"internalType":"uint256","name":"value","type":"uint256"},{"indexed":False,"internalType":"uint256","name":"index","type":"uint256"}],"name":"Mint","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"from","type":"address"},{"indexed":True,"internalType":"address","name":"to","type":"address"},{"indexed":False,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Transfer","type":"event"},{"inputs":[],"name":"ATOKEN_REVISION","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"DOMAIN_SEPARATOR","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"EIP712_REVISION","outputs":[{"internalType":"bytes","name":"","type":"bytes"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"PERMIT_TYPEHASH","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"POOL","outputs":[{"internalType":"contract ILendingPool","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"RESERVE_TREASURY_ADDRESS","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"UNDERLYING_ASSET_ADDRESS","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"_nonces","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"owner","type":"address"},{"internalType":"address","name":"spender","type":"address"}],"name":"allowance","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"}],"name":"approve","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"user","type":"address"}],"name":"balanceOf","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"user","type":"address"},{"internalType":"address","name":"receiverOfUnderlying","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"},{"internalType":"uint256","name":"index","type":"uint256"}],"name":"burn","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"decimals","outputs":[{"internalType":"uint8","name":"","type":"uint8"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"subtractedValue","type":"uint256"}],"name":"decreaseAllowance","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"getIncentivesController","outputs":[{"internalType":"contract IAaveIncentivesController","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"user","type":"address"}],"name":"getScaledUserBalanceAndSupply","outputs":[{"internalType":"uint256","name":"","type":"uint256"},{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"user","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"}],"name":"handleRepayment","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"addedValue","type":"uint256"}],"name":"increaseAllowance","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"contract ILendingPool","name":"pool","type":"address"},{"internalType":"address","name":"treasury","type":"address"},{"internalType":"address","name":"underlyingAsset","type":"address"},{"internalType":"contract IAaveIncentivesController","name":"incentivesController","type":"address"},{"internalType":"uint8","name":"aTokenDecimals","type":"uint8"},{"internalType":"string","name":"aTokenName","type":"string"},{"internalType":"string","name":"aTokenSymbol","type":"string"},{"internalType":"bytes","name":"params","type":"bytes"}],"name":"initialize","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"user","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"},{"internalType":"uint256","name":"index","type":"uint256"}],"name":"mint","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"amount","type":"uint256"},{"internalType":"uint256","name":"index","type":"uint256"}],"name":"mintToTreasury","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"name","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"owner","type":"address"},{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"},{"internalType":"uint256","name":"deadline","type":"uint256"},{"internalType":"uint8","name":"v","type":"uint8"},{"internalType":"bytes32","name":"r","type":"bytes32"},{"internalType":"bytes32","name":"s","type":"bytes32"}],"name":"permit","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"user","type":"address"}],"name":"scaledBalanceOf","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"scaledTotalSupply","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"symbol","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"totalSupply","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"}],"name":"transfer","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"sender","type":"address"},{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"}],"name":"transferFrom","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"}],"name":"transferOnLiquidation","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"target","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"}],"name":"transferUnderlyingTo","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"nonpayable","type":"function"}]    
    return abi

# # cdp contract
def get_borrower_operations_abi():

    abi = [{"anonymous":False,"inputs":[{"indexed":False,"internalType":"address","name":"_activePoolAddress","type":"address"}],"name":"ActivePoolAddressChanged","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"address","name":"_collSurplusPoolAddress","type":"address"}],"name":"CollSurplusPoolAddressChanged","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"address","name":"_newCollateralConfigAddress","type":"address"}],"name":"CollateralConfigAddressChanged","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"address","name":"_defaultPoolAddress","type":"address"}],"name":"DefaultPoolAddressChanged","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"address","name":"_gasPoolAddress","type":"address"}],"name":"GasPoolAddressChanged","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"address","name":"_lqtyStakingAddress","type":"address"}],"name":"LQTYStakingAddressChanged","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"_borrower","type":"address"},{"indexed":False,"internalType":"address","name":"_collateral","type":"address"},{"indexed":False,"internalType":"uint256","name":"_LUSDFee","type":"uint256"}],"name":"LUSDBorrowingFeePaid","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"address","name":"_lusdTokenAddress","type":"address"}],"name":"LUSDTokenAddressChanged","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"previousOwner","type":"address"},{"indexed":True,"internalType":"address","name":"newOwner","type":"address"}],"name":"OwnershipTransferred","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"address","name":"_newPriceFeedAddress","type":"address"}],"name":"PriceFeedAddressChanged","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"address","name":"_sortedTrovesAddress","type":"address"}],"name":"SortedTrovesAddressChanged","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"address","name":"_stabilityPoolAddress","type":"address"}],"name":"StabilityPoolAddressChanged","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"_borrower","type":"address"},{"indexed":False,"internalType":"address","name":"_collateral","type":"address"},{"indexed":False,"internalType":"uint256","name":"arrayIndex","type":"uint256"}],"name":"TroveCreated","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"address","name":"_newTroveManagerAddress","type":"address"}],"name":"TroveManagerAddressChanged","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"_borrower","type":"address"},{"indexed":False,"internalType":"address","name":"_collateral","type":"address"},{"indexed":False,"internalType":"uint256","name":"_debt","type":"uint256"},{"indexed":False,"internalType":"uint256","name":"_coll","type":"uint256"},{"indexed":False,"internalType":"uint256","name":"stake","type":"uint256"},{"indexed":False,"internalType":"enum BorrowerOperations.BorrowerOperation","name":"operation","type":"uint8"}],"name":"TroveUpdated","type":"event"},{"inputs":[],"name":"BORROWING_FEE_FLOOR","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"DECIMAL_PRECISION","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"LUSD_GAS_COMPENSATION","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"MIN_NET_DEBT","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"NAME","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"PERCENT_DIVISOR","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"_100pct","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"activePool","outputs":[{"internalType":"contract IActivePool","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_collateral","type":"address"},{"internalType":"uint256","name":"_collAmount","type":"uint256"},{"internalType":"address","name":"_upperHint","type":"address"},{"internalType":"address","name":"_lowerHint","type":"address"}],"name":"addColl","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_collateral","type":"address"},{"internalType":"uint256","name":"_maxFeePercentage","type":"uint256"},{"internalType":"uint256","name":"_collTopUp","type":"uint256"},{"internalType":"uint256","name":"_collWithdrawal","type":"uint256"},{"internalType":"uint256","name":"_LUSDChange","type":"uint256"},{"internalType":"bool","name":"_isDebtIncrease","type":"bool"},{"internalType":"address","name":"_upperHint","type":"address"},{"internalType":"address","name":"_lowerHint","type":"address"}],"name":"adjustTrove","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_collateral","type":"address"}],"name":"claimCollateral","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_collateral","type":"address"}],"name":"closeTrove","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"collateralConfig","outputs":[{"internalType":"contract ICollateralConfig","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"defaultPool","outputs":[{"internalType":"contract IDefaultPool","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"_debt","type":"uint256"}],"name":"getCompositeDebt","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"pure","type":"function"},{"inputs":[{"internalType":"address","name":"_collateral","type":"address"}],"name":"getEntireSystemColl","outputs":[{"internalType":"uint256","name":"entireSystemColl","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_collateral","type":"address"}],"name":"getEntireSystemDebt","outputs":[{"internalType":"uint256","name":"entireSystemDebt","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"lqtyStaking","outputs":[{"internalType":"contract ILQTYStaking","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"lqtyStakingAddress","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"lusdToken","outputs":[{"internalType":"contract ILUSDToken","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_borrower","type":"address"},{"internalType":"address","name":"_collateral","type":"address"},{"internalType":"uint256","name":"_collAmount","type":"uint256"},{"internalType":"address","name":"_upperHint","type":"address"},{"internalType":"address","name":"_lowerHint","type":"address"}],"name":"moveCollateralGainToTrove","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_collateral","type":"address"},{"internalType":"uint256","name":"_collAmount","type":"uint256"},{"internalType":"uint256","name":"_maxFeePercentage","type":"uint256"},{"internalType":"uint256","name":"_LUSDAmount","type":"uint256"},{"internalType":"address","name":"_upperHint","type":"address"},{"internalType":"address","name":"_lowerHint","type":"address"}],"name":"openTrove","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"owner","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"priceFeed","outputs":[{"internalType":"contract IPriceFeed","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"renounceOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_collateral","type":"address"},{"internalType":"uint256","name":"_LUSDAmount","type":"uint256"},{"internalType":"address","name":"_upperHint","type":"address"},{"internalType":"address","name":"_lowerHint","type":"address"}],"name":"repayLUSD","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_collateralConfigAddress","type":"address"},{"internalType":"address","name":"_troveManagerAddress","type":"address"},{"internalType":"address","name":"_activePoolAddress","type":"address"},{"internalType":"address","name":"_defaultPoolAddress","type":"address"},{"internalType":"address","name":"_stabilityPoolAddress","type":"address"},{"internalType":"address","name":"_gasPoolAddress","type":"address"},{"internalType":"address","name":"_collSurplusPoolAddress","type":"address"},{"internalType":"address","name":"_priceFeedAddress","type":"address"},{"internalType":"address","name":"_sortedTrovesAddress","type":"address"},{"internalType":"address","name":"_lusdTokenAddress","type":"address"},{"internalType":"address","name":"_lqtyStakingAddress","type":"address"}],"name":"setAddresses","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"sortedTroves","outputs":[{"internalType":"contract ISortedTroves","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"newOwner","type":"address"}],"name":"transferOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"troveManager","outputs":[{"internalType":"contract ITroveManager","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_collateral","type":"address"},{"internalType":"uint256","name":"_collWithdrawal","type":"uint256"},{"internalType":"address","name":"_upperHint","type":"address"},{"internalType":"address","name":"_lowerHint","type":"address"}],"name":"withdrawColl","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_collateral","type":"address"},{"internalType":"uint256","name":"_maxFeePercentage","type":"uint256"},{"internalType":"uint256","name":"_LUSDAmount","type":"uint256"},{"internalType":"address","name":"_upperHint","type":"address"},{"internalType":"address","name":"_lowerHint","type":"address"}],"name":"withdrawLUSD","outputs":[],"stateMutability":"nonpayable","type":"function"}]
    return abi

# # gets our web3 contract object
def get_contract(contract_address, contract_abi, web3):

    contract = web3.eth.contract(address=contract_address, abi=contract_abi)
    
    return contract

# gets the last block number we have gotten data from and returns this block number
def get_last_block_tracked():
    df = pd.read_csv('all_users.csv')
    
    last_block_monitored = df['last_block_number'].max()

    last_block_monitored = int(last_block_monitored)

    return last_block_monitored

def make_checksum_values(df):

    lowered_tokenAddress_list = df['tokenAddress'].to_list()
    lowered_wallet_address_list = df['wallet_address'].to_list()

    # check_sum_tx_hash_list = [Web3.to_checksum_address(x) for x in lowered_tx_hash_list]

    check_sum_wallet_address_list = [Web3.to_checksum_address(x) for x in lowered_wallet_address_list]
    # print(check_sum_wallet_address_list)

    check_sum_tokenAddress_list = [Web3.to_checksum_address(x) for x in lowered_tokenAddress_list]
    # print(check_sum_tokenAddress_list)

    df['wallet_address'] = check_sum_wallet_address_list
    df['tokenAddress'] = check_sum_tokenAddress_list

    return df

#makes a dataframe and stores it in a csv file
def make_user_data_csv(df, index):

    event_csv = get_lp_config_value('event_csv_name', index)
    old_df = pd.read_csv(event_csv, dtype={'from_address': str, 'to_address': str, 'tx_hash': str, 'timestamp': str, 'token_address': str, 'reserve_address': str, 'token_volume': str, 'asset_price': str, 'usd_token_amount': str, 'log_index': int, 'transaction_index': int, 'block_number': int})
    old_df = old_df.drop_duplicates(subset=['tx_hash','log_index', 'transaction_index'], keep='last')

    combined_df_list = [df, old_df]
    combined_df = pd.concat(combined_df_list)
    combined_df = combined_df.drop_duplicates(subset=['tx_hash', 'log_index', 'transaction_index'], keep='last')
    
    if len(combined_df) >= len(old_df):
        event_csv = get_lp_config_value('event_csv_name', index)
        combined_df.to_csv(event_csv, index=False)
        print()
        print('Event CSV Updated. Old Length: ', len(old_df), ' New Length: ', len(combined_df), ' Events Added: ', len(combined_df) - len(old_df))
        print()
    return

# # takes in an a_token address and returns it's contract object
def get_a_token_contract(web3, contract_address):
    # contract_address = "0xEB329420Fae03176EC5877c34E2c38580D85E069"
    contract_abi = [{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"owner","type":"address"},{"indexed":True,"internalType":"address","name":"spender","type":"address"},{"indexed":False,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Approval","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"from","type":"address"},{"indexed":True,"internalType":"address","name":"to","type":"address"},{"indexed":False,"internalType":"uint256","name":"value","type":"uint256"},{"indexed":False,"internalType":"uint256","name":"index","type":"uint256"}],"name":"BalanceTransfer","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"from","type":"address"},{"indexed":True,"internalType":"address","name":"target","type":"address"},{"indexed":False,"internalType":"uint256","name":"value","type":"uint256"},{"indexed":False,"internalType":"uint256","name":"index","type":"uint256"}],"name":"Burn","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"underlyingAsset","type":"address"},{"indexed":True,"internalType":"address","name":"pool","type":"address"},{"indexed":False,"internalType":"address","name":"treasury","type":"address"},{"indexed":False,"internalType":"address","name":"incentivesController","type":"address"},{"indexed":False,"internalType":"uint8","name":"aTokenDecimals","type":"uint8"},{"indexed":False,"internalType":"string","name":"aTokenName","type":"string"},{"indexed":False,"internalType":"string","name":"aTokenSymbol","type":"string"},{"indexed":False,"internalType":"bytes","name":"params","type":"bytes"}],"name":"Initialized","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"from","type":"address"},{"indexed":False,"internalType":"uint256","name":"value","type":"uint256"},{"indexed":False,"internalType":"uint256","name":"index","type":"uint256"}],"name":"Mint","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"from","type":"address"},{"indexed":True,"internalType":"address","name":"to","type":"address"},{"indexed":False,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Transfer","type":"event"},{"inputs":[],"name":"ATOKEN_REVISION","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"DOMAIN_SEPARATOR","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"EIP712_REVISION","outputs":[{"internalType":"bytes","name":"","type":"bytes"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"PERMIT_TYPEHASH","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"POOL","outputs":[{"internalType":"contract ILendingPool","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"RESERVE_TREASURY_ADDRESS","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"UNDERLYING_ASSET_ADDRESS","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"_nonces","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"owner","type":"address"},{"internalType":"address","name":"spender","type":"address"}],"name":"allowance","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"}],"name":"approve","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"user","type":"address"}],"name":"balanceOf","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"user","type":"address"},{"internalType":"address","name":"receiverOfUnderlying","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"},{"internalType":"uint256","name":"index","type":"uint256"}],"name":"burn","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"decimals","outputs":[{"internalType":"uint8","name":"","type":"uint8"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"subtractedValue","type":"uint256"}],"name":"decreaseAllowance","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"getIncentivesController","outputs":[{"internalType":"contract IAaveIncentivesController","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"user","type":"address"}],"name":"getScaledUserBalanceAndSupply","outputs":[{"internalType":"uint256","name":"","type":"uint256"},{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"user","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"}],"name":"handleRepayment","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"addedValue","type":"uint256"}],"name":"increaseAllowance","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"contract ILendingPool","name":"pool","type":"address"},{"internalType":"address","name":"treasury","type":"address"},{"internalType":"address","name":"underlyingAsset","type":"address"},{"internalType":"contract IAaveIncentivesController","name":"incentivesController","type":"address"},{"internalType":"uint8","name":"aTokenDecimals","type":"uint8"},{"internalType":"string","name":"aTokenName","type":"string"},{"internalType":"string","name":"aTokenSymbol","type":"string"},{"internalType":"bytes","name":"params","type":"bytes"}],"name":"initialize","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"user","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"},{"internalType":"uint256","name":"index","type":"uint256"}],"name":"mint","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"amount","type":"uint256"},{"internalType":"uint256","name":"index","type":"uint256"}],"name":"mintToTreasury","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"name","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"owner","type":"address"},{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"},{"internalType":"uint256","name":"deadline","type":"uint256"},{"internalType":"uint8","name":"v","type":"uint8"},{"internalType":"bytes32","name":"r","type":"bytes32"},{"internalType":"bytes32","name":"s","type":"bytes32"}],"name":"permit","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"user","type":"address"}],"name":"scaledBalanceOf","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"scaledTotalSupply","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"symbol","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"totalSupply","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"}],"name":"transfer","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"sender","type":"address"},{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"}],"name":"transferFrom","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"}],"name":"transferOnLiquidation","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"target","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"}],"name":"transferUnderlyingTo","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"nonpayable","type":"function"}]    
    contract = web3.eth.contract(address=contract_address, abi=contract_abi)

    return contract

# # takes in an v_token address and returns it's contract object
def get_v_token_contract(web3, contract_address):
    # contract_address = "0xBE8afE7E442fFfFE576B979D490c5ADb7823C3c6"
    contract_abi = [{"type":"event","name":"Approval","inputs":[{"type":"address","name":"owner","internalType":"address","indexed":True},{"type":"address","name":"spender","internalType":"address","indexed":True},{"type":"uint256","name":"value","internalType":"uint256","indexed":False}],"anonymous":False},{"type":"event","name":"BorrowAllowanceDelegated","inputs":[{"type":"address","name":"fromUser","internalType":"address","indexed":True},{"type":"address","name":"toUser","internalType":"address","indexed":True},{"type":"address","name":"asset","internalType":"address","indexed":False},{"type":"uint256","name":"amount","internalType":"uint256","indexed":False}],"anonymous":False},{"type":"event","name":"Burn","inputs":[{"type":"address","name":"user","internalType":"address","indexed":True},{"type":"uint256","name":"amount","internalType":"uint256","indexed":False},{"type":"uint256","name":"currentBalance","internalType":"uint256","indexed":False},{"type":"uint256","name":"balanceIncrease","internalType":"uint256","indexed":False},{"type":"uint256","name":"avgStableRate","internalType":"uint256","indexed":False},{"type":"uint256","name":"newTotalSupply","internalType":"uint256","indexed":False}],"anonymous":False},{"type":"event","name":"Initialized","inputs":[{"type":"address","name":"underlyingAsset","internalType":"address","indexed":True},{"type":"address","name":"pool","internalType":"address","indexed":True},{"type":"address","name":"incentivesController","internalType":"address","indexed":False},{"type":"uint8","name":"debtTokenDecimals","internalType":"uint8","indexed":False},{"type":"string","name":"debtTokenName","internalType":"string","indexed":False},{"type":"string","name":"debtTokenSymbol","internalType":"string","indexed":False},{"type":"bytes","name":"params","internalType":"bytes","indexed":False}],"anonymous":False},{"type":"event","name":"Mint","inputs":[{"type":"address","name":"user","internalType":"address","indexed":True},{"type":"address","name":"onBehalfOf","internalType":"address","indexed":True},{"type":"uint256","name":"amount","internalType":"uint256","indexed":False},{"type":"uint256","name":"currentBalance","internalType":"uint256","indexed":False},{"type":"uint256","name":"balanceIncrease","internalType":"uint256","indexed":False},{"type":"uint256","name":"newRate","internalType":"uint256","indexed":False},{"type":"uint256","name":"avgStableRate","internalType":"uint256","indexed":False},{"type":"uint256","name":"newTotalSupply","internalType":"uint256","indexed":False}],"anonymous":False},{"type":"event","name":"Transfer","inputs":[{"type":"address","name":"from","internalType":"address","indexed":True},{"type":"address","name":"to","internalType":"address","indexed":True},{"type":"uint256","name":"value","internalType":"uint256","indexed":False}],"anonymous":False},{"type":"function","stateMutability":"view","outputs":[{"type":"uint256","name":"","internalType":"uint256"}],"name":"DEBT_TOKEN_REVISION","inputs":[]},{"type":"function","stateMutability":"view","outputs":[{"type":"address","name":"","internalType":"contract ILendingPool"}],"name":"POOL","inputs":[]},{"type":"function","stateMutability":"view","outputs":[{"type":"address","name":"","internalType":"address"}],"name":"UNDERLYING_ASSET_ADDRESS","inputs":[]},{"type":"function","stateMutability":"view","outputs":[{"type":"uint256","name":"","internalType":"uint256"}],"name":"allowance","inputs":[{"type":"address","name":"owner","internalType":"address"},{"type":"address","name":"spender","internalType":"address"}]},{"type":"function","stateMutability":"nonpayable","outputs":[{"type":"bool","name":"","internalType":"bool"}],"name":"approve","inputs":[{"type":"address","name":"spender","internalType":"address"},{"type":"uint256","name":"amount","internalType":"uint256"}]},{"type":"function","stateMutability":"nonpayable","outputs":[],"name":"approveDelegation","inputs":[{"type":"address","name":"delegatee","internalType":"address"},{"type":"uint256","name":"amount","internalType":"uint256"}]},{"type":"function","stateMutability":"view","outputs":[{"type":"uint256","name":"","internalType":"uint256"}],"name":"balanceOf","inputs":[{"type":"address","name":"account","internalType":"address"}]},{"type":"function","stateMutability":"view","outputs":[{"type":"uint256","name":"","internalType":"uint256"}],"name":"borrowAllowance","inputs":[{"type":"address","name":"fromUser","internalType":"address"},{"type":"address","name":"toUser","internalType":"address"}]},{"type":"function","stateMutability":"nonpayable","outputs":[],"name":"burn","inputs":[{"type":"address","name":"user","internalType":"address"},{"type":"uint256","name":"amount","internalType":"uint256"}]},{"type":"function","stateMutability":"view","outputs":[{"type":"uint8","name":"","internalType":"uint8"}],"name":"decimals","inputs":[]},{"type":"function","stateMutability":"nonpayable","outputs":[{"type":"bool","name":"","internalType":"bool"}],"name":"decreaseAllowance","inputs":[{"type":"address","name":"spender","internalType":"address"},{"type":"uint256","name":"subtractedValue","internalType":"uint256"}]},{"type":"function","stateMutability":"view","outputs":[{"type":"uint256","name":"","internalType":"uint256"}],"name":"getAverageStableRate","inputs":[]},{"type":"function","stateMutability":"view","outputs":[{"type":"address","name":"","internalType":"contract IRewarder"}],"name":"getIncentivesController","inputs":[]},{"type":"function","stateMutability":"view","outputs":[{"type":"uint256","name":"","internalType":"uint256"},{"type":"uint256","name":"","internalType":"uint256"},{"type":"uint256","name":"","internalType":"uint256"},{"type":"uint40","name":"","internalType":"uint40"}],"name":"getSupplyData","inputs":[]},{"type":"function","stateMutability":"view","outputs":[{"type":"uint256","name":"","internalType":"uint256"},{"type":"uint256","name":"","internalType":"uint256"}],"name":"getTotalSupplyAndAvgRate","inputs":[]},{"type":"function","stateMutability":"view","outputs":[{"type":"uint40","name":"","internalType":"uint40"}],"name":"getTotalSupplyLastUpdated","inputs":[]},{"type":"function","stateMutability":"view","outputs":[{"type":"uint40","name":"","internalType":"uint40"}],"name":"getUserLastUpdated","inputs":[{"type":"address","name":"user","internalType":"address"}]},{"type":"function","stateMutability":"view","outputs":[{"type":"uint256","name":"","internalType":"uint256"}],"name":"getUserStableRate","inputs":[{"type":"address","name":"user","internalType":"address"}]},{"type":"function","stateMutability":"nonpayable","outputs":[{"type":"bool","name":"","internalType":"bool"}],"name":"increaseAllowance","inputs":[{"type":"address","name":"spender","internalType":"address"},{"type":"uint256","name":"addedValue","internalType":"uint256"}]},{"type":"function","stateMutability":"nonpayable","outputs":[],"name":"initialize","inputs":[{"type":"address","name":"pool","internalType":"contract ILendingPool"},{"type":"address","name":"underlyingAsset","internalType":"address"},{"type":"address","name":"incentivesController","internalType":"contract IRewarder"},{"type":"uint8","name":"debtTokenDecimals","internalType":"uint8"},{"type":"string","name":"debtTokenName","internalType":"string"},{"type":"string","name":"debtTokenSymbol","internalType":"string"},{"type":"bytes","name":"params","internalType":"bytes"}]},{"type":"function","stateMutability":"nonpayable","outputs":[{"type":"bool","name":"","internalType":"bool"}],"name":"mint","inputs":[{"type":"address","name":"user","internalType":"address"},{"type":"address","name":"onBehalfOf","internalType":"address"},{"type":"uint256","name":"amount","internalType":"uint256"},{"type":"uint256","name":"rate","internalType":"uint256"}]},{"type":"function","stateMutability":"view","outputs":[{"type":"string","name":"","internalType":"string"}],"name":"name","inputs":[]},{"type":"function","stateMutability":"view","outputs":[{"type":"uint256","name":"","internalType":"uint256"}],"name":"principalBalanceOf","inputs":[{"type":"address","name":"user","internalType":"address"}]},{"type":"function","stateMutability":"view","outputs":[{"type":"string","name":"","internalType":"string"}],"name":"symbol","inputs":[]},{"type":"function","stateMutability":"view","outputs":[{"type":"uint256","name":"","internalType":"uint256"}],"name":"totalSupply","inputs":[]},{"type":"function","stateMutability":"nonpayable","outputs":[{"type":"bool","name":"","internalType":"bool"}],"name":"transfer","inputs":[{"type":"address","name":"recipient","internalType":"address"},{"type":"uint256","name":"amount","internalType":"uint256"}]},{"type":"function","stateMutability":"nonpayable","outputs":[{"type":"bool","name":"","internalType":"bool"}],"name":"transferFrom","inputs":[{"type":"address","name":"sender","internalType":"address"},{"type":"address","name":"recipient","internalType":"address"},{"type":"uint256","name":"amount","internalType":"uint256"}]}]    
    contract = web3.eth.contract(address=contract_address, abi=contract_abi)

    return contract

# # gets our borrower operations contract
def get_borrower_operations_contract(web3, contract_address):
    contract_abi = get_borrower_operations_abi()
    contract = web3.eth.contract(address=contract_address, abi=contract_abi)

    return contract

# # takes in a contract object and returns all associated deposit events
def get_deposit_events(contract, from_block, to_block):

    # events = contract.events.Transfer.get_logs(fromBlock=from_block, toBlock=latest_block)
    events = contract.events.Deposit.get_logs(fromBlock=from_block, toBlock=to_block)

    return events

# # gets our token transfer events
def get_transfer_events(contract, from_block, to_block):
    
    from_block = int(from_block)
    to_block = int(to_block)
    
    print('From Block: ', from_block, ' To Block: ', to_block)
    events = contract.events.Transfer.get_logs(fromBlock=from_block, toBlock=to_block)
    
    return events

# # takes in a contract object and returns all associated withdrawal events
def get_withdraw_events(contract, from_block, to_block):

    # events = contract.events.Transfer.get_logs(fromBlock=from_block, toBlock=latest_block)
    events = contract.events.Withdraw.get_logs(fromBlock=from_block, toBlock=to_block)

    return events

# # takes in a contract object and returns all associated borrow events
def get_borrow_events(contract, from_block, to_block):

    # events = contract.events.Transfer.get_logs(fromBlock=from_block, toBlock=latest_block)
    events = contract.events.Borrow.get_logs(fromBlock=from_block, toBlock=to_block)

    return events

# # takes in a contract object and returns all associated repay events
def get_repay_events(contract, from_block, to_block):
    events = contract.events.Repay.get_logs(fromBlock=from_block, toBlock=to_block)

    return events

# # gets our token transfer events
def get_approval_events(contract, from_block, to_block):
    
    from_block = int(from_block)
    to_block = int(to_block)
    
    events = contract.events.Approval.get_logs(fromBlock=from_block, toBlock=to_block)

    
    return events

# # will get the mint fee paid when someone mints our cdp stablecoin
def get_mint_fee_events(contract, from_block, to_block):
    
    from_block = int(from_block)
    to_block = int(to_block)
    
    events = contract.events.LUSDBorrowingFeePaid.get_logs(fromBlock=from_block, toBlock=to_block)

    return events

# # will get the mint fee paid when someone mints our cdp stablecoin
def get_trove_updated_events(contract, from_block, to_block):
    
    from_block = int(from_block)
    to_block = int(to_block)
    
    events = contract.events.TroveUpdated.get_logs(fromBlock=from_block, toBlock=to_block)

    return events

# # gets the current balances of a user given a contract address and a wallet address
def get_balance_of(contract, wallet_address):
    
    balance = contract.functions.balanceOf(wallet_address).call()

    return balance

#handles our weth_gateway events and returns the accurate user_address
def handle_weth_gateway(event, enum_name, index):

    gateway_address = get_lp_config_value('gateway_address', index)

    payload_address = event['args']['user']

    if payload_address.lower() == gateway_address.lower():
        print('Already part of the dataframe')
        # print(event)
        # if enum_name == 'LEND' or enum_name == 'BORROW':
        #     user = 'onBehalfOf'
        try:
            payload_address = event['args']['onBehalfOf']
        except:
            pass
    
    return payload_address

#returns a df if a tx_hash exists
def tx_hash_exists(df, tx_hash):

    new_df = pd.DataFrame()

    if ((df['tx_hash'] == tx_hash)).any():
        new_df = df.loc[df['tx_hash'] == tx_hash]
    
    return new_df

#returns whether a enum_name exists, and returns blank df if not
def lend_borrow_type_exists(df, lend_borrow_type):

    if ((df['lendBorrowType'] == lend_borrow_type)).any():
        df = df.loc[df['lendBorrowType'] == lend_borrow_type]

    else:
        df = pd.DataFrame()

    return df

#returns df if wallet_address exists
def wallet_address_exists(df, wallet_address):

    if ((df['wallet_address'] == wallet_address)).any():
        df = df.loc[df['wallet_address'] == wallet_address]

    else:
        df = pd.DataFrame()

    return df

# # generalized exists function that will help us reduce rpc calls
def value_exists(df, input_value, column_name):

    if (df[column_name] == input_value).any():
        df = df.loc[df[column_name] == input_value]
    
    else:
        df = pd.DataFrame()
    
    return df

# # will return a list of our token contract objects for our index
def get_token_contract_list(web3, index):
    df = get_token_config_df()

    df = df.loc[df['chain_index'] == index]

    contract_address_list = df['token_address'].tolist()

    contract_list = [get_a_token_contract(web3, contract_address) for contract_address in contract_address_list]
    
    return contract_list

# will tell us whether we need to find new data
# returns a list of [tx_hash, wallet_address]
def already_part_of_df(event, wait_time, from_block, to_block, index):

    all_exist = False
    tx_hash = ''
    log_index = -1
    tx_index = -1
    token_amount = -1
    wait_time = wait_time / 3

    csv = get_lp_config_value('event_csv_name', index)

    df = pd.read_csv(csv, usecols=['tx_hash', 'transaction_index', 'log_index', 'block_number'], dtype={'tx_hash': str, 'transaction_id': int, 'log_index': int, 'block_number': int})

    temp_from_block = from_block - 2500
    temp_to_block = to_block + 2500

    df = df[df['block_number'] > temp_from_block]
    df = df[df['block_number'] < temp_to_block]

    if len(df) > 0:

        tx_hash = event['transactionHash'].hex()
        tx_hash = tx_hash

        new_df = value_exists(df, tx_hash, 'tx_hash')
        time.sleep(wait_time)

        if len(new_df) > 0:
            tx_index = event['transactionIndex']
            time.sleep(wait_time)

            new_df = value_exists(new_df, tx_index, 'transaction_index')

            if len(new_df) > 0:

                log_index = event['logIndex']

                new_df = value_exists(new_df, log_index,'log_index')

                if len(new_df) > 0:
                    all_exist = True

    response_list = [tx_hash, log_index, tx_index, token_amount, all_exist]

    return response_list

#gets our reserve price
#@cache
def get_tx_usd_amount(reserve_address, token_amount, web3, index):

    asset_price_tx_usd_value_list = []

    contract_address = get_lp_config_value('aave_oracle_address', index)
    contract_abi = get_aave_oracle_abi()

    contract = get_contract(contract_address, contract_abi, web3)

    value_usd = contract.functions.getAssetPrice(reserve_address).call()
    time.sleep(0.1)
    decimals = get_token_config_value('decimals', reserve_address, index)
    usd_amount = (value_usd/1e8)*(token_amount/decimals)
    # print(usd_amount)
    asset_price_tx_usd_value_list.append(value_usd/1e8)
    asset_price_tx_usd_value_list.append(usd_amount)

    return asset_price_tx_usd_value_list

# # returns a df that has each batch of event's reserve addresses and prices in a single df
def get_batch_pricing_df(df, web3, index):

    pricing_df = df

    pricing_df = pricing_df.drop_duplicates(subset=['reserve_address'])

    reserve_list = pricing_df['reserve_address'].tolist()

    temp_reserve_list = []
    reserve_price_list = []

    temp_df_list = []

    for reserve in reserve_list:

        temp_df = pricing_df.loc[pricing_df['reserve_address'] == reserve]

        reserve_price = get_tx_usd_amount(reserve, 1, web3, index)

        reserve_price = reserve_price[0]

        temp_df['reserve_price'] = reserve_price

        temp_df_list.append(temp_df)
    
    pricing_df = pd.concat(temp_df_list)

    return pricing_df

# # users our batches pricing data to update our batches df asset_price and tx_usd_amount
# # saves contract calls massively
def set_batch_df_pricing(batch_df, pricing_df):

    df_list = []

    reserve_list = pricing_df['reserve_address'].tolist()

    for reserve in reserve_list:
        temp_batch_df = batch_df.loc[batch_df['reserve_address'] == reserve]
        temp_pricing_df = pricing_df.loc[pricing_df['reserve_address'] == reserve]

        asset_price = temp_pricing_df['reserve_price'].tolist()
        asset_price = asset_price[0]

        temp_batch_df['asset_price'] = asset_price
        temp_batch_df['usd_token_amount'] = temp_batch_df['token_volume'] * temp_batch_df['asset_price']

        df_list.append(temp_batch_df)

    df = pd.concat(df_list)

    return df

# # aggregate function that gets our asset pricing and returns updated batch_df
def update_batch_pricing(batch_df, web3, index):

    pricing_df = get_batch_pricing_df(batch_df, web3, index)
    batch_df = set_batch_df_pricing(batch_df, pricing_df)


    return batch_df

# # finds our contract launch_block
# # subtracts the interval from our from block to help account for the script quitting on one of the 4 deposit,withdraw,repay,borrow event sets before iterating to the next set of blocks
def get_from_block(index, interval):

    from_block = get_lp_config_value('from_block', index)

    last_block_checked = get_lp_config_value('last_block', index)

    if last_block_checked > from_block:
        from_block = last_block_checked
        from_block = from_block - interval

    from_block = int(from_block)

    return from_block

# # finds our contract launch_block
# # subtracts the interval from our from block to help account for the script quitting on one of the 4 deposit,withdraw,repay,borrow event sets before iterating to the next set of blocks
def get_from_block_2(value_function, index):

    interval = value_function('interval', index)

    from_block = value_function('from_block', index)


    last_block_checked = value_function('last_block', index)

    if last_block_checked > from_block:
        from_block = last_block_checked
        from_block = from_block - interval

    from_block = int(from_block)

    return from_block

# # gets the from_block for our reverse search
def get_reverse_from_block(index):

    from_block = get_lp_config_value('from_block', index)

    return from_block

# # finds our contract launch_block
# # subtracts the interval from our from block to help account for the script quitting on one of the 4 deposit,withdraw,repay,borrow event sets before iterating to the next set of blocks
def get_smart_latest_block(web3, index):

    latest_block = get_latest_block(web3)

    interval = get_lp_config_value('interval', index)

    config_latest_block = get_lp_config_value('last_block', index)


    if latest_block > config_latest_block:
        latest_block = config_latest_block
        latest_block = latest_block - interval

    latest_block = int(latest_block)

    return latest_block

# # returns the last from block of an asset
def get_last_asset_block(event_df, token_config_df, receipt_counter, index):

    token_config_df = token_config_df.loc[token_config_df['chain_index'] == index]

    receipt_list = token_config_df['token_address'].tolist()

    receipt_token = receipt_list[receipt_counter]

    temp_df = event_df.loc[event_df['token_address'] == receipt_token]

    last_asset_from_block = temp_df['block_number'].max()

    last_asset_from_block = int(last_asset_from_block)

    return last_asset_from_block

# # makes our web3 object and injects it's middleware
def get_web_3(rpc_url):

    if 'wss' in rpc_url:
        provider = Web3.WebsocketProvider(rpc_url)
        web3 = Web3(provider)
    else:
        web3 = Web3(Web3.HTTPProvider(rpc_url))
    time.sleep(1)
    web3.middleware_onion.inject(geth_poa_middleware, layer=0)
    time.sleep(1)
    
    return web3
# # finds the latest block on a given blockchain
def get_latest_block(web3):

    latest_block = web3.eth.get_block_number()

    return latest_block

# # will divide by our token decimals to get us a more human readable amount
def clean_up_df_decimals(df, amount_column_name, index):

    config_df = get_token_config_df()

    config_df = config_df.loc[config_df['chain_index'] == index]

    unique_token_list = config_df.token_address.unique()

    for unique_token_address in unique_token_list:
        decimals = get_token_config_value('decimals', unique_token_address, index)

        df.loc[df['token_address'] == unique_token_address, amount_column_name] /= decimals

        # df.loc[df['token_address'] == token_address, 'reserve_address'] = reserve_address

    
    return df

# # will add our reserve_address column to our dataframe
def add_df_reserve_address(df, index):
    
    config_df = get_token_config_df()

    config_df = config_df.loc[config_df['chain_index'] == index]

    unique_token_list = config_df.token_address.unique()

    # # placeholder initialization of the reserve_address column so we can assign it values
    df['reserve_address'] = 'N/A'

    for unique_token_address in unique_token_list:
        reserve_address = get_token_config_value('underlying_address', unique_token_address, index)

        df.loc[df['token_address'] == unique_token_address, 'reserve_address'] = reserve_address

    return df

# # will make a one line of a users tvl accross their tokens
def make_one_line_tvl(df):
    df['amount_cumulative'] = df['amount_cumulative'].astype(float)

    total_tvl = df.groupby('user_address')['amount_cumulative'].sum()
    
    df = df.join(total_tvl.to_frame(name='total_tvl'), on='user_address')

    df = df.drop_duplicates(subset=['user_address'])

    return df

# # will add our asset price column to our dataframe *
def add_df_asset_prices(df, web3, index):

    temp_df = df.drop_duplicates(subset=['reserve_address'])

    reserve_address_list = temp_df['reserve_address'].tolist()

    contract_address = get_lp_config_value('aave_oracle_address', index)
    contract_abi = get_aave_oracle_abi()
    contract = get_contract(contract_address, contract_abi, web3)

    wait_time = get_lp_config_value('wait_time', index)

    wait_time /= 3

    for reserve_address in reserve_address_list:
        value_usd = contract.functions.getAssetPrice(reserve_address).call()
        
        time.sleep(wait_time)

        value_usd = value_usd / 1e8

        df.loc[df['reserve_address'] == reserve_address, 'asset_price'] = value_usd

    return df

# # correctly updates our price at the end
def get_final_pricing(df, index):
    rpc_url = get_lp_config_value('rpc_url', index)

    web3 = get_web_3(rpc_url)

    config_df = get_token_config_df()

    reserve_address_list = config_df['underlying_address'].tolist()
    token_address_list = config_df['token_address'].tolist()

    contract_address = get_lp_config_value('aave_oracle_address', index)
    contract_abi = get_aave_oracle_abi()
    contract = get_contract(contract_address, contract_abi, web3)

    df['current_balance'] = df['current_balance'].astype(float)
    df['approval_amount'] = df['approval_amount'].astype(float)

    df_list = []

    i = 0
    while i < len(reserve_address_list):
        reserve_address = reserve_address_list[i]
        token_address = token_address_list[i]
        
        value_usd = contract.functions.getAssetPrice(reserve_address).call()
        time.sleep(0.1)

        value_usd = value_usd / 1e8

        decimals = get_token_config_value('decimals', reserve_address, index)

        temp_df = df.loc[df['token_address'] == token_address]

        if len(temp_df) > 0:

            temp_df['asset_price'] = value_usd
            temp_df['current_balance_raw'] = temp_df['current_balance']
            temp_df['approval_amount_raw'] = temp_df['approval_amount']
            temp_df['approval_amount'] /= decimals
            temp_df['current_balance'] = temp_df['current_balance'] / decimals
            temp_df['current_balance_usd'] = temp_df['current_balance'] * value_usd

            df_list.append(temp_df)
        
        i += 1
    
    df = pd.concat(df_list)

    return df

# # will return a list of only a_tokens
def get_a_token_list(index):

    token_config_df = get_token_config_df()

    token_config_df = token_config_df.loc[token_config_df['chain_index'] == index]

    token_list = token_config_df['token_name'].tolist()
    a_token_list = [x for x in token_list if x.lower()[0] != 'v']

    token_config_df = token_config_df.loc[token_config_df['token_name'].isin(a_token_list)]

    a_token_list = token_config_df['token_address'].tolist()

    return a_token_list

# # will return a list of only variable_debt tokens
def get_v_token_list(index):
    
    token_config_df = get_token_config_df()

    token_config_df = token_config_df.loc[token_config_df['chain_index'] == index]

    token_list = token_config_df['token_name'].tolist()
    v_token_list = [x for x in token_list if x.lower()[0] == 'v']

    token_config_df = token_config_df.loc[token_config_df['token_name'].isin(v_token_list)]

    v_token_list = token_config_df['token_address'].tolist()

    return v_token_list

# # runs all our looks
# # updates our csv
def find_all_transactions(event_function, data_function, column_list, index):

    config_df = get_lp_config_df()
    config_df = config_df.loc[config_df['index'] == index]

    rpc_url = get_lp_config_value('rpc_url', index)
    
    web3 = get_web_3(rpc_url)

    # contract = tf.get_contract(contract_address, contract_abi, web3)

    from_block = get_from_block(index)

    latest_block = get_latest_block(web3) 

    token_config_df = get_token_config_df()

    interval = get_lp_config_value('interval', index)

    wait_time = get_lp_config_value('wait_time', index)

    # # gets the database table name we will be using
    table_name = get_lp_config_value('table_name', index)

    to_block = from_block + interval

    token_config_df = get_token_config_df()

    token_config_df = token_config_df.loc[token_config_df['chain_index'] == index]

    receipt_token_list = token_config_df['token_address'].tolist()


    # # reads our last data from our treasury to ensure we don't lose info do to the vm reverting
    cloud_csv_name = get_lp_config_value('cloud_filename', index)
    cloud_bucket_name = get_lp_config_value('cloud_bucket_name', index)
    cloud_df = cs.read_zip_csv_from_cloud_storage(cloud_csv_name, cloud_bucket_name)
    # # drops any stray duplicates
    cloud_df.drop_duplicates(subset=['tx_hash', 'token_address', 'token_volume', 'from_address', 'to_address'])

    # # will create our table and only insert data into it from our cloud bucket if the table doesn't exist
    create_tx_data_table(table_name, cloud_df)

    while to_block < latest_block:

        print('Current Event Block vs Latest Event Block to Check: ', from_block, '/', latest_block, 'Blocks Remaining: ', latest_block - from_block)

        receipt_counter = 0

        while receipt_counter < len(receipt_token_list):
            receipt_token_address = receipt_token_list[receipt_counter]

            receipt_contract = get_a_token_contract(web3, receipt_token_address)


            print(receipt_token_address, ': Current Event Block vs Latest Event Block to Check: ', from_block, '/', latest_block, 'Blocks Remaining: ', latest_block - from_block)

            events = event_function(receipt_contract, from_block, to_block)

            receipt_counter += 1
            

            if len(events) > 0:
                contract_df = data_function(events, web3, from_block, to_block, wait_time, table_name, index)
                # # print(contract_df)
                if len(contract_df) > 0:
                    time.sleep(wait_time)
                    sql.write_to_db(contract_df, column_list, table_name)
                    # sql.drop_duplicates_from_database(cursor)
                    # make_user_data_csv(contract_df, index)
            else:
                time.sleep(wait_time)

        config_df.loc[config_df['index'] == index, 'last_block'] = from_block
        
        config_df.to_csv('./config/lp_config.csv', index=False)

        from_block += interval
        to_block += interval

        # print(deposit_events)


        if from_block >= latest_block:
            from_block = latest_block - 1
        
        if to_block >= latest_block:
            to_block = latest_block
        
    # bp.set_embers_database(index)

    return 

# # can use our final points breakdown from the app to update our cloud bucket
def finalize_points():
    df = pd.read_csv('ironclad_point_totals.csv')
    current_balance_df = cs.read_zip_csv_from_cloud_storage('snapshot_user_tvl_embers.csv', 'cooldowns2')

    current_balance_df = current_balance_df[['user_address', 'total_tvl']]

    df = df.loc[df['pointsBalance'] > 0]

    df['user_address'] = df['walletId']
    df['total_embers'] = df['pointsBalance']

    df = df[['user_address', 'total_embers']]

    merged_df = pd.merge(df, current_balance_df, how='left', on='user_address')

    merged_df['total_tvl'] = pd.to_numeric(merged_df['total_tvl'], errors='coerce')
    merged_df['total_embers'] = pd.to_numeric(merged_df['total_embers'], errors='coerce')

    # Replace NaN values with 0
    merged_df['total_tvl'] = merged_df['total_tvl'].fillna(0)
    merged_df['total_embers'] = merged_df['total_embers'].fillna(0)

    merged_df.loc[merged_df['total_tvl'] < 0, 'total_tvl'] = 0
    merged_df.loc[merged_df['total_embers'] < 0, 'total_embers'] = 0

    # cs.df_write_to_cloud_storage_as_zip(merged_df, 'snapshot_user_tvl_embers.csv', 'cooldowns2')
    
    return merged_df

def get_specified_users_transactions(user_list, events, web3, index):

    df = pd.DataFrame()

    to_address_list = []
    from_address_list = []
    tx_hash_list = []
    timestamp_list = []
    token_address_list = []
    reserve_address_list = []
    token_volume_list = []
    asset_price_list = []
    token_usd_amount_list = []
    log_index_list = []
    tx_index_list = []
    block_list = []

    user = ''

    treasury_address = get_lp_config_value('treasury_address', index)

    # # inputs to our sql function
    column_list = ['from_address','to_address','tx_hash','timestamp','token_address','reserve_address','token_volume','asset_price','usd_token_amount','log_index','transaction_index','block_number']
    data_type_list = ['TEXT' for x in column_list]
    table_name = get_lp_config_value('table_name', index)

    # reduces wait time by 50%
    wait_time = get_lp_config_value('wait_time', index)
    wait_time = wait_time/3

    start_time = time.time()
    i = 1
    for event in events:

        # print('Batch of Events Processed: ', i, '/', len(events))
        i+=1

        to_address = event['args']['to']
        time.sleep(wait_time)
        from_address = event['args']['from']

        # # will only add data to the dataframe if tokens are going to or from an address in our user_list
        if to_address in user_list or from_address in user_list:
            exists_list = sql.already_part_of_database(event, wait_time, column_list, data_type_list, table_name)

            tx_hash = exists_list[0]
            log_index = exists_list[1]
            tx_index = exists_list[2]
            token_address = exists_list[3]
            exists = exists_list[4]
            token_amount = -1
            
            if exists == False:
                try:
                    block = web3.eth.get_block(event['blockNumber'])
                    block_number = int(block['number'])
                except:
                    block_number = int(event['blockNumber'])

                time.sleep(wait_time)
                if log_index < 0:
                    log_index = event['logIndex']
                
                time.sleep(wait_time)
                if tx_index < 0:
                    tx_index = event['transactionIndex']

                time.sleep(wait_time)
                if token_amount < 0:
                    token_amount = event['args']['value']
                
                if len(token_address) < 1:
                    token_address = event['address']
                
                log_index = event['logIndex']
                time.sleep(wait_time)

                if token_amount > 0:
                    
                    block_list.append(block_number)
                    from_address_list.append(from_address)
                    to_address_list.append(to_address)
                    tx_hash_list.append(tx_hash)
                    timestamp_list.append(block['timestamp'])
                    time.sleep(wait_time)
                    # token_address = event['address']
                    token_address_list.append(token_address)
                    reserve_address = get_token_config_value('underlying_address', token_address, index)
                    reserve_address_list.append(reserve_address)
                    token_volume_list.append(token_amount)
                    log_index_list.append(log_index)
                    tx_index_list.append(tx_index)
                else:
                    # print('Already part of the dataframe')
                    # print(event)
                    time.sleep(wait_time)
                    pass

    if len(from_address_list) > 0:
        time.sleep(wait_time)


        df['from_address'] = from_address_list
        df['to_address'] = to_address_list
        df['tx_hash'] = tx_hash_list
        df['timestamp'] = timestamp_list
        df['token_address'] = token_address_list
        df['reserve_address'] = reserve_address_list
        df['token_volume'] = token_volume_list
        df = update_batch_pricing(df, web3, index)
        df['log_index'] = log_index_list
        df['transaction_index'] = tx_index_list
        df['block_number'] = block_list


    # print('User Data Event Looping done in: ', time.time() - start_time)
    return df        

# # will load our cloud df information into the sql database
def insert_bulk_data_into_table(df, table_name):
    # from_address,to_address,tx_hash,timestamp,token_address,reserve_address,token_volume,asset_price,usd_token_amount,log_index,transaction_index,block_number
    
    query = f"""
        CREATE TABLE IF NOT EXISTS {table_name}(
            from_address TEXT,
            to_address TEXT,
            tx_hash TEXT,
            timestamp TEXT,
            token_address TEXT,
            reserve_address TEXT,
            token_volume TEXT,
            asset_price TEXT,
            usd_token_amount TEXT,
            log_index TEXT,
            transaction_index TEXT,
            block_number TEXT
            )
        """
    
    # # will only insert data into the sql table if the table doesn't exist

    sql.create_custom_table(query)

    query = f"""
    INSERT INTO {table_name} (from_address,to_address,tx_hash,timestamp,token_address,reserve_address,token_volume,asset_price,usd_token_amount,log_index,transaction_index,block_number)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    sql.write_to_custom_table(query, df)

    return

# # creates our balance table if it doesn't already exist
def create_tx_data_table(table_name, df):

    query = f"""
            CREATE TABLE IF NOT EXISTS {table_name}(
                from_address TEXT,
                to_address TEXT,
                tx_hash TEXT,
                timestamp TEXT,
                token_address TEXT,
                reserve_address TEXT,
                token_volume TEXT,
                asset_price TEXT,
                usd_token_amount TEXT,
                log_index TEXT,
                transaction_index TEXT,
                block_number TEXT
                )
            """
    
    # # will only insert data into the sql table if the table doesn't exist

    sql.create_custom_table(query)

    table_length = sql.select_star_count(table_name)[0][0]
    
    # # we will drop our table and insert the data from the cloud of our local database has less entries than the cloud
    if table_length < len(df):
        sql.drop_table(table_name)
        sql.create_custom_table(query)
        insert_bulk_data_into_table(df, table_name)

    return

# # will find the balance of our tokens in human readable form by dividing them by their Decimals
def get_token_volume_decimals(df, index):

    df['token_volume'] = df['token_volume'].astype(float)

    temp_df = df.drop_duplicates(subset=['token_address'])

    token_list = temp_df['token_address'].tolist()

    for token_address in token_list:
        decimals = get_token_config_value('decimals', token_address, index)
        decimals = float(decimals)


        df.loc[df['token_address'] == token_address, 'decimals'] = decimals

    df['token_volume_decimals'] = df['token_volume'] / df['decimals']

    return df

# # will fix our reserve addresses
def fix_reserve_addresses(df, index):

    temp_df = df.drop_duplicates(subset=['token_address'])

    token_list = temp_df['token_address'].tolist()

    for token_address in token_list:
        reserve_address = get_token_config_value('underlying_address', token_address, index)

        df.loc[df['token_address'] == token_address, 'reserve_address'] = reserve_address

    return df

# # users our batches pricing data to update our batches df asset_price and tx_usd_amount
# # saves contract calls massively
def set_df_prices(df, web3, index):

    df[['usd_token_amount', 'token_volume', 'asset_price']] = df[['usd_token_amount', 'token_volume', 'asset_price']].astype(float)

    df = fix_reserve_addresses(df, index)

    df = add_df_asset_prices(df, web3, index)

    df = get_token_volume_decimals(df, index)

    df['usd_token_amount'] = df['token_volume_decimals'] * df['asset_price']

    df = df[['from_address','to_address','tx_hash','timestamp','token_address','reserve_address','token_volume','asset_price','usd_token_amount','log_index','transaction_index','block_number']]
    
    return df

# # gets the deposit token_config df
def get_deposit_token_df(index):
    token_df = get_token_config_df()

    token_df = token_df.loc[token_df['chain_index'] == index]

    token_list = token_df['token_name'].tolist()

    token_list = [token for token in token_list if token[0] != 'v']

    deposit_df = pd.DataFrame()

    deposit_df['token_name'] = token_list

    token_df = token_df.loc[token_df['token_name'].isin(token_list)]

    return token_df

# # gets the borrow token_config df
def get_borrow_token_df(index):
    token_df = get_token_config_df()

    token_df = token_df.loc[token_df['chain_index'] == index]

    token_list = token_df['token_name'].tolist()

    token_list = [token for token in token_list if token[0] == 'v']

    borrow_df = pd.DataFrame()

    borrow_df['token_name'] = token_list

    token_df = token_df.loc[token_df['token_name'].isin(token_list)]

    return token_df

# # finds if a transaction adds to or reduces a balance 
# # (deposit + borrow add to a balance and withdraw + repay reduce a balance)
def set_token_flows(index):
    # event_df = pd.read_csv(csv_name, usecols=['from_address','to_address','timestamp','token_address', 'token_volume','tx_hash'], dtype={'from_address': str,'to_address': str,'timestamp' : str,'token_address': str, 'token_volume': float,'tx_hash': str})
    
    # # tries to remove the null address to greatly reduce computation needs
    # event_df = event_df.loc[event_df['to_address'] != '0x0000000000000000000000000000000000000000']
    
    table_name = get_lp_config_value('table_name', index)
    
    event_df = sql.get_transaction_data_df(table_name)

    event_df = event_df.drop_duplicates(subset=['tx_hash', 'to_address', 'from_address', 'token_address', 'token_volume'])

    event_df[['usd_token_amount', 'token_volume']] = event_df[['usd_token_amount', 'token_volume']].astype(float)

    unique_user_df = sql.set_unique_users(table_name)

    unique_user_list = unique_user_df['to_address'].to_list()

    deposit_token_df = get_deposit_token_df(index)
    borrow_token_df = get_borrow_token_df(index)

    deposit_token_list = deposit_token_df['token_address'].tolist()
    borrow_token_list = borrow_token_df['token_address'].tolist()

    combo_df = pd.DataFrame()
    temp_df = pd.DataFrame()

    # # handles deposits and borrows
    temp_df = event_df.loc[event_df['to_address'].isin(unique_user_list)]
    deposit_df = temp_df.loc[temp_df['token_address'].isin(deposit_token_list)]
    borrow_df = temp_df.loc[temp_df['token_address'].isin(borrow_token_list)]

    # # handles withdrawals and repays
    temp_df = event_df.loc[event_df['from_address'].isin(unique_user_list)]
    withdraw_df = temp_df.loc[temp_df['token_address'].isin(deposit_token_list)]
    repay_df = temp_df.loc[temp_df['token_address'].isin(borrow_token_list)]

    withdraw_df['usd_token_amount'] *= -1
    repay_df['usd_token_amount'] *= -1

    deposit_df['user_address'] = deposit_df['to_address']
    borrow_df['user_address'] = borrow_df['to_address']
    
    withdraw_df['user_address'] = withdraw_df['from_address']
    repay_df['user_address'] = repay_df['from_address']

    combo_df = pd.concat([deposit_df, borrow_df, withdraw_df, repay_df])
    combo_df = combo_df[['user_address', 'tx_hash', 'token_address', 'token_volume', 'usd_token_amount', 'timestamp']]

    combo_df.drop_duplicates(subset=['user_address', 'tx_hash', 'token_address', 'token_volume'])
    # make_user_data_csv(combo_df, token_flow_csv)

    # # tries to remove the null address to greatly reduce computation needs
    # combo_df = combo_df.loc[combo_df['user_address'] != '0x0000000000000000000000000000000000000000']

    return combo_df

# # sets rolling balances for each of a users tokens
def set_rolling_balance(df):
    
    df['timestamp'] = df['timestamp'].astype(float)

    df = df.sort_values(by=['timestamp'], ascending=True)

    # Group the DataFrame by 'name' and calculate cumulative sum
    name_groups = df.groupby(['user_address','token_address'])['usd_token_amount'].transform(pd.Series.cumsum)

    # Print the DataFrame with the new 'amount_cumulative' column
    df = df.assign(usd_rolling_balance=name_groups)

    df = df.reset_index()

    df = df[['user_address', 'tx_hash', 'token_address', 'token_volume', 'usd_token_amount', 'timestamp', 'usd_rolling_balance']]
    
    df = df.sort_values(by=['timestamp'], ascending=True)
    
    return df

# # uses our timestamp column to make a day column
def make_day_from_timestamp(df):
    
    df['day'] = pd.to_datetime(df['timestamp'], unit='s').dt.strftime('%Y/%m/%d')

    return df

def set_token_and_day_diffs(df):

    df['usd_rolling_balance'] = df['usd_rolling_balance'].astype(float)

    daily_token_sum = df.groupby(['day', 'token_address', 'user_address'])['usd_rolling_balance'].sum().reset_index()

    daily_token_sum = daily_token_sum.sort_values(['day', 'token_address', 'user_address'])
    
    df = df.merge(daily_token_sum, on=['day', 'token_address', 'user_address'], suffixes=('', '_daily_token_sum'))

    daily_revenue_sum = df.groupby(['day', 'user_address'])['usd_rolling_balance'].sum().reset_index()

    df = df.merge(daily_revenue_sum, on=['day'], suffixes=('', '_daily_total_balance'))

    df.rename(columns = {'usd_rolling_balance_daily_token_sum':'token_day_rolling_balance', 'usd_rolling_balance_daily_total_balance':'total_rolling_balance'}, inplace = True)

    # # casts our day to a datetime
    df['day'] = pd.to_datetime(df['day'], format='%Y-%m-%d')
    # df['day'] = df['day'].astype(str)
    df[['total_rolling_balance_change', 'token_day_rolling_balance']] = df[['total_rolling_balance', 'token_day_rolling_balance']].astype(float)

    df = df.sort_values(by=['day', 'token_address', 'user_address'])

    df['token_day_rolling_balance'] = df.groupby(['token_address', 'user_address', 'day'])['token_day_rolling_balance'].fillna(method='ffill')
    df['total_rolling_balance'] = df.groupby(['token_address', 'user_address', 'day'])['total_rolling_balance'].fillna(method='ffill')

    # Calculate the difference between consecutive non-NaN values (optional)
    df['token_day_diff'] = df['token_day_rolling_balance'].diff().fillna(0)
    df['day_diff'] = df['total_rolling_balance'].diff().fillna(0)

    return df

# # will make our dataframe only have 1 row per day_diff
def set_total_day_diff_1_line(df):

    daily_total_balance_diff_df = df.groupby(['day', 'user_address', 'total_rolling_balance'])['day_diff'].sum().reset_index()
    
    return daily_total_balance_diff_df

# # will make our dataframe only have 1 row per day_diff
def set_token_wallet_address_diff_1_line(df):

    daily_token_wallet_address_balance_diff = df.groupby(['day', 'user_address', 'token_address'])['token_day_rolling_balance'].sum().reset_index()
    
    return daily_token_wallet_address_balance_diff

# # takes in our df from our database, our dataframe from our cloud
# # concats them together and drops duplicates based off of the column_list provided
# # returns a sanitized df
def sanitize_database_and_cloud_df(db_df, cloud_df, column_list):

    df_list = [db_df, cloud_df]

    combined_df = pd.concat(df_list)

    sanitized_df = combined_df.drop_duplicates(subset=column_list)

    return sanitized_df

# # will return a boolean on whether or not a number is in our string
def number_in_string(string):
    number_list = [0,1,2,3,4,5,6,7,8,9]
    contains_number = False

    for number in number_list:
        if str(number) in string:
            contains_number = True
    
    return contains_number

# # sets a moving average for our dataframe
def set_n_days_avg_revenue(df, new_column_name, lookback_days):

    df = df.sort_values(by=['day'], ascending=True)

    day_revenue_df = df.groupby(['day'])['daily_revenue'].max().reset_index()

    # Calculate the rolling average
    day_revenue_df[new_column_name] = day_revenue_df['daily_revenue'].rolling(window=lookback_days, min_periods=1).mean()

    # Merge the rolling average back to the original dataframe
    df = df.merge(day_revenue_df[['day', new_column_name]], on='day', how='left')

    return df