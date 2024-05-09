from web3 import Web3
from web3.middleware import geth_poa_middleware 
import pandas as pd
import time
import lending_pool.transaction_finder as tf

# # returns our config csv as a dataframe
def get_config_df(csv_name):
    df = pd.read_csv(csv_name)

    return df

# # returns the relevant value from our config csv
def get_config_value(csv_name, column_name, index):
    
    df = get_config_df(csv_name)

    config_list = df[column_name].tolist()

    config_value = config_list[index]

    return config_value

# # gets our lending_pool abi
def get_lending_pool_abi():

    abi = [ { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "address", "name": "reserve", "type": "address" }, { "indexed": False, "internalType": "address", "name": "user", "type": "address" }, { "indexed": True, "internalType": "address", "name": "onBehalfOf", "type": "address" }, { "indexed": False, "internalType": "uint256", "name": "amount", "type": "uint256" }, { "indexed": False, "internalType": "uint256", "name": "borrowRateMode", "type": "uint256" }, { "indexed": False, "internalType": "uint256", "name": "borrowRate", "type": "uint256" }, { "indexed": True, "internalType": "uint16", "name": "referral", "type": "uint16" } ], "name": "Borrow", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "address", "name": "reserve", "type": "address" }, { "indexed": False, "internalType": "address", "name": "user", "type": "address" }, { "indexed": True, "internalType": "address", "name": "onBehalfOf", "type": "address" }, { "indexed": False, "internalType": "uint256", "name": "amount", "type": "uint256" }, { "indexed": True, "internalType": "uint16", "name": "referral", "type": "uint16" } ], "name": "Deposit", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "address", "name": "target", "type": "address" }, { "indexed": True, "internalType": "address", "name": "initiator", "type": "address" }, { "indexed": True, "internalType": "address", "name": "asset", "type": "address" }, { "indexed": False, "internalType": "uint256", "name": "amount", "type": "uint256" }, { "indexed": False, "internalType": "uint256", "name": "premium", "type": "uint256" }, { "indexed": False, "internalType": "uint16", "name": "referralCode", "type": "uint16" } ], "name": "FlashLoan", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "address", "name": "collateralAsset", "type": "address" }, { "indexed": True, "internalType": "address", "name": "debtAsset", "type": "address" }, { "indexed": True, "internalType": "address", "name": "user", "type": "address" }, { "indexed": False, "internalType": "uint256", "name": "debtToCover", "type": "uint256" }, { "indexed": False, "internalType": "uint256", "name": "liquidatedCollateralAmount", "type": "uint256" }, { "indexed": False, "internalType": "address", "name": "liquidator", "type": "address" }, { "indexed": False, "internalType": "bool", "name": "receiveAToken", "type": "bool" } ], "name": "LiquidationCall", "type": "event" }, { "anonymous": False, "inputs": [], "name": "Paused", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "address", "name": "reserve", "type": "address" }, { "indexed": True, "internalType": "address", "name": "user", "type": "address" } ], "name": "RebalanceStableBorrowRate", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "address", "name": "reserve", "type": "address" }, { "indexed": True, "internalType": "address", "name": "user", "type": "address" }, { "indexed": True, "internalType": "address", "name": "repayer", "type": "address" }, { "indexed": False, "internalType": "uint256", "name": "amount", "type": "uint256" } ], "name": "Repay", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "address", "name": "reserve", "type": "address" }, { "indexed": False, "internalType": "uint256", "name": "liquidityRate", "type": "uint256" }, { "indexed": False, "internalType": "uint256", "name": "stableBorrowRate", "type": "uint256" }, { "indexed": False, "internalType": "uint256", "name": "variableBorrowRate", "type": "uint256" }, { "indexed": False, "internalType": "uint256", "name": "liquidityIndex", "type": "uint256" }, { "indexed": False, "internalType": "uint256", "name": "variableBorrowIndex", "type": "uint256" } ], "name": "ReserveDataUpdated", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "address", "name": "reserve", "type": "address" }, { "indexed": True, "internalType": "address", "name": "user", "type": "address" } ], "name": "ReserveUsedAsCollateralDisabled", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "address", "name": "reserve", "type": "address" }, { "indexed": True, "internalType": "address", "name": "user", "type": "address" } ], "name": "ReserveUsedAsCollateralEnabled", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "address", "name": "reserve", "type": "address" }, { "indexed": True, "internalType": "address", "name": "user", "type": "address" }, { "indexed": False, "internalType": "uint256", "name": "rateMode", "type": "uint256" } ], "name": "Swap", "type": "event" }, { "anonymous": False, "inputs": [], "name": "Unpaused", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "address", "name": "reserve", "type": "address" }, { "indexed": True, "internalType": "address", "name": "user", "type": "address" }, { "indexed": True, "internalType": "address", "name": "to", "type": "address" }, { "indexed": False, "internalType": "uint256", "name": "amount", "type": "uint256" } ], "name": "Withdraw", "type": "event" }, { "inputs": [], "name": "FLASHLOAN_PREMIUM_TOTAL", "outputs": [ { "internalType": "uint256", "name": "", "type": "uint256" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "LENDINGPOOL_REVISION", "outputs": [ { "internalType": "uint256", "name": "", "type": "uint256" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "MAX_NUMBER_RESERVES", "outputs": [ { "internalType": "uint256", "name": "", "type": "uint256" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "MAX_STABLE_RATE_BORROW_SIZE_PERCENT", "outputs": [ { "internalType": "uint256", "name": "", "type": "uint256" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "asset", "type": "address" }, { "internalType": "uint256", "name": "amount", "type": "uint256" }, { "internalType": "uint256", "name": "interestRateMode", "type": "uint256" }, { "internalType": "uint16", "name": "referralCode", "type": "uint16" }, { "internalType": "address", "name": "onBehalfOf", "type": "address" } ], "name": "borrow", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "asset", "type": "address" }, { "internalType": "uint256", "name": "amount", "type": "uint256" }, { "internalType": "address", "name": "onBehalfOf", "type": "address" }, { "internalType": "uint16", "name": "referralCode", "type": "uint16" } ], "name": "deposit", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "asset", "type": "address" }, { "internalType": "address", "name": "from", "type": "address" }, { "internalType": "address", "name": "to", "type": "address" }, { "internalType": "uint256", "name": "amount", "type": "uint256" }, { "internalType": "uint256", "name": "balanceFromBefore", "type": "uint256" }, { "internalType": "uint256", "name": "balanceToBefore", "type": "uint256" } ], "name": "finalizeTransfer", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "receiverAddress", "type": "address" }, { "internalType": "address[]", "name": "assets", "type": "address[]" }, { "internalType": "uint256[]", "name": "amounts", "type": "uint256[]" }, { "internalType": "uint256[]", "name": "modes", "type": "uint256[]" }, { "internalType": "address", "name": "onBehalfOf", "type": "address" }, { "internalType": "bytes", "name": "params", "type": "bytes" }, { "internalType": "uint16", "name": "referralCode", "type": "uint16" } ], "name": "flashLoan", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [], "name": "getAddressesProvider", "outputs": [ { "internalType": "contract ILendingPoolAddressesProvider", "name": "", "type": "address" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "asset", "type": "address" } ], "name": "getConfiguration", "outputs": [ { "components": [ { "internalType": "uint256", "name": "data", "type": "uint256" } ], "internalType": "struct DataTypes.ReserveConfigurationMap", "name": "", "type": "tuple" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "asset", "type": "address" } ], "name": "getReserveData", "outputs": [ { "components": [ { "components": [ { "internalType": "uint256", "name": "data", "type": "uint256" } ], "internalType": "struct DataTypes.ReserveConfigurationMap", "name": "configuration", "type": "tuple" }, { "internalType": "uint128", "name": "liquidityIndex", "type": "uint128" }, { "internalType": "uint128", "name": "variableBorrowIndex", "type": "uint128" }, { "internalType": "uint128", "name": "currentLiquidityRate", "type": "uint128" }, { "internalType": "uint128", "name": "currentVariableBorrowRate", "type": "uint128" }, { "internalType": "uint128", "name": "currentStableBorrowRate", "type": "uint128" }, { "internalType": "uint40", "name": "lastUpdateTimestamp", "type": "uint40" }, { "internalType": "address", "name": "aTokenAddress", "type": "address" }, { "internalType": "address", "name": "stableDebtTokenAddress", "type": "address" }, { "internalType": "address", "name": "variableDebtTokenAddress", "type": "address" }, { "internalType": "address", "name": "interestRateStrategyAddress", "type": "address" }, { "internalType": "uint8", "name": "id", "type": "uint8" } ], "internalType": "struct DataTypes.ReserveData", "name": "", "type": "tuple" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "asset", "type": "address" } ], "name": "getReserveNormalizedIncome", "outputs": [ { "internalType": "uint256", "name": "", "type": "uint256" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "asset", "type": "address" } ], "name": "getReserveNormalizedVariableDebt", "outputs": [ { "internalType": "uint256", "name": "", "type": "uint256" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "getReservesList", "outputs": [ { "internalType": "address[]", "name": "", "type": "address[]" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "getSignature", "outputs": [ { "internalType": "string", "name": "", "type": "string" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "user", "type": "address" } ], "name": "getUserAccountData", "outputs": [ { "internalType": "uint256", "name": "totalCollateralETH", "type": "uint256" }, { "internalType": "uint256", "name": "totalDebtETH", "type": "uint256" }, { "internalType": "uint256", "name": "availableBorrowsETH", "type": "uint256" }, { "internalType": "uint256", "name": "currentLiquidationThreshold", "type": "uint256" }, { "internalType": "uint256", "name": "ltv", "type": "uint256" }, { "internalType": "uint256", "name": "healthFactor", "type": "uint256" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "user", "type": "address" } ], "name": "getUserConfiguration", "outputs": [ { "components": [ { "internalType": "uint256", "name": "data", "type": "uint256" } ], "internalType": "struct DataTypes.UserConfigurationMap", "name": "", "type": "tuple" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "asset", "type": "address" }, { "internalType": "address", "name": "aTokenAddress", "type": "address" }, { "internalType": "address", "name": "stableDebtAddress", "type": "address" }, { "internalType": "address", "name": "variableDebtAddress", "type": "address" }, { "internalType": "address", "name": "interestRateStrategyAddress", "type": "address" } ], "name": "initReserve", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "contract ILendingPoolAddressesProvider", "name": "provider", "type": "address" } ], "name": "initialize", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "collateralAsset", "type": "address" }, { "internalType": "address", "name": "debtAsset", "type": "address" }, { "internalType": "address", "name": "user", "type": "address" }, { "internalType": "uint256", "name": "debtToCover", "type": "uint256" }, { "internalType": "bool", "name": "receiveAToken", "type": "bool" } ], "name": "liquidationCall", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [], "name": "paused", "outputs": [ { "internalType": "bool", "name": "", "type": "bool" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "asset", "type": "address" }, { "internalType": "address", "name": "user", "type": "address" } ], "name": "rebalanceStableBorrowRate", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "asset", "type": "address" }, { "internalType": "uint256", "name": "amount", "type": "uint256" }, { "internalType": "uint256", "name": "rateMode", "type": "uint256" }, { "internalType": "address", "name": "onBehalfOf", "type": "address" } ], "name": "repay", "outputs": [ { "internalType": "uint256", "name": "", "type": "uint256" } ], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "asset", "type": "address" }, { "internalType": "uint256", "name": "configuration", "type": "uint256" } ], "name": "setConfiguration", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "bool", "name": "val", "type": "bool" } ], "name": "setPause", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "asset", "type": "address" }, { "internalType": "address", "name": "rateStrategyAddress", "type": "address" } ], "name": "setReserveInterestRateStrategyAddress", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "string", "name": "signature", "type": "string" } ], "name": "setSignature", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "asset", "type": "address" }, { "internalType": "bool", "name": "useAsCollateral", "type": "bool" } ], "name": "setUserUseReserveAsCollateral", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "asset", "type": "address" }, { "internalType": "uint256", "name": "rateMode", "type": "uint256" } ], "name": "swapBorrowRateMode", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "asset", "type": "address" }, { "internalType": "uint256", "name": "amount", "type": "uint256" }, { "internalType": "address", "name": "to", "type": "address" } ], "name": "withdraw", "outputs": [ { "internalType": "uint256", "name": "", "type": "uint256" } ], "stateMutability": "nonpayable", "type": "function" } ]
    
    return abi

# # finds our contract launch_block
# # subtracts the interval from our from block to help account for the script quitting on one of the 4 deposit,withdraw,repay,borrow event sets before iterating to the next set of blocks
def get_from_block(csv_name, index):

    interval = get_config_value(csv_name, 'interval', index)

    from_block = get_config_value(csv_name, 'from_block', index)


    last_block_checked = get_config_value(csv_name, 'last_block', index)

    if last_block_checked > from_block:
        from_block = last_block_checked
        from_block = from_block - interval

    from_block = int(from_block)

    return from_block

# # takes in an a_token address and returns it's contract object
def get_a_token_contract(web3, contract_address):
    # contract_address = "0xEB329420Fae03176EC5877c34E2c38580D85E069"
    contract_abi = [{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"owner","type":"address"},{"indexed":True,"internalType":"address","name":"spender","type":"address"},{"indexed":False,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Approval","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"from","type":"address"},{"indexed":True,"internalType":"address","name":"to","type":"address"},{"indexed":False,"internalType":"uint256","name":"value","type":"uint256"},{"indexed":False,"internalType":"uint256","name":"index","type":"uint256"}],"name":"BalanceTransfer","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"from","type":"address"},{"indexed":True,"internalType":"address","name":"target","type":"address"},{"indexed":False,"internalType":"uint256","name":"value","type":"uint256"},{"indexed":False,"internalType":"uint256","name":"index","type":"uint256"}],"name":"Burn","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"underlyingAsset","type":"address"},{"indexed":True,"internalType":"address","name":"pool","type":"address"},{"indexed":False,"internalType":"address","name":"treasury","type":"address"},{"indexed":False,"internalType":"address","name":"incentivesController","type":"address"},{"indexed":False,"internalType":"uint8","name":"aTokenDecimals","type":"uint8"},{"indexed":False,"internalType":"string","name":"aTokenName","type":"string"},{"indexed":False,"internalType":"string","name":"aTokenSymbol","type":"string"},{"indexed":False,"internalType":"bytes","name":"params","type":"bytes"}],"name":"Initialized","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"from","type":"address"},{"indexed":False,"internalType":"uint256","name":"value","type":"uint256"},{"indexed":False,"internalType":"uint256","name":"index","type":"uint256"}],"name":"Mint","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"from","type":"address"},{"indexed":True,"internalType":"address","name":"to","type":"address"},{"indexed":False,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Transfer","type":"event"},{"inputs":[],"name":"ATOKEN_REVISION","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"DOMAIN_SEPARATOR","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"EIP712_REVISION","outputs":[{"internalType":"bytes","name":"","type":"bytes"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"PERMIT_TYPEHASH","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"POOL","outputs":[{"internalType":"contract ILendingPool","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"RESERVE_TREASURY_ADDRESS","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"UNDERLYING_ASSET_ADDRESS","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"_nonces","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"owner","type":"address"},{"internalType":"address","name":"spender","type":"address"}],"name":"allowance","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"}],"name":"approve","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"user","type":"address"}],"name":"balanceOf","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"user","type":"address"},{"internalType":"address","name":"receiverOfUnderlying","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"},{"internalType":"uint256","name":"index","type":"uint256"}],"name":"burn","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"decimals","outputs":[{"internalType":"uint8","name":"","type":"uint8"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"subtractedValue","type":"uint256"}],"name":"decreaseAllowance","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"getIncentivesController","outputs":[{"internalType":"contract IAaveIncentivesController","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"user","type":"address"}],"name":"getScaledUserBalanceAndSupply","outputs":[{"internalType":"uint256","name":"","type":"uint256"},{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"user","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"}],"name":"handleRepayment","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"addedValue","type":"uint256"}],"name":"increaseAllowance","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"contract ILendingPool","name":"pool","type":"address"},{"internalType":"address","name":"treasury","type":"address"},{"internalType":"address","name":"underlyingAsset","type":"address"},{"internalType":"contract IAaveIncentivesController","name":"incentivesController","type":"address"},{"internalType":"uint8","name":"aTokenDecimals","type":"uint8"},{"internalType":"string","name":"aTokenName","type":"string"},{"internalType":"string","name":"aTokenSymbol","type":"string"},{"internalType":"bytes","name":"params","type":"bytes"}],"name":"initialize","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"user","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"},{"internalType":"uint256","name":"index","type":"uint256"}],"name":"mint","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"amount","type":"uint256"},{"internalType":"uint256","name":"index","type":"uint256"}],"name":"mintToTreasury","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"name","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"owner","type":"address"},{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"},{"internalType":"uint256","name":"deadline","type":"uint256"},{"internalType":"uint8","name":"v","type":"uint8"},{"internalType":"bytes32","name":"r","type":"bytes32"},{"internalType":"bytes32","name":"s","type":"bytes32"}],"name":"permit","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"user","type":"address"}],"name":"scaledBalanceOf","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"scaledTotalSupply","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"symbol","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"totalSupply","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"}],"name":"transfer","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"sender","type":"address"},{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"}],"name":"transferFrom","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"}],"name":"transferOnLiquidation","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"target","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"}],"name":"transferUnderlyingTo","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"nonpayable","type":"function"}]    
    contract = web3.eth.contract(address=contract_address, abi=contract_abi)

    return contract

# # will return a list of our token contract objects for our index
def get_token_contract_list(web3, index):
    df = get_config_df('token_config.csv')

    df = df.loc[df['chain_index'] == index]

    contract_address_list = df['token_address'].tolist()

    contract_list = [get_a_token_contract(web3, contract_address) for contract_address in contract_address_list]
    
    return contract_list

def get_liquidation_events(contract, from_block, to_block):
    
    from_block = int(from_block)
    to_block = int(to_block)
    
    events = contract.events.LiquidationCall.get_logs(fromBlock=from_block, toBlock=to_block)
    
    return events

def get_liquidation_df(events, web3, csv_name, index):
    
    df = pd.DataFrame()

    user_address_list = []
    liquidator_address_list = []

    collateral_asset_list = []
    liquidated_collateral_amount_list = []

    debt_asset_list = []
    debt_repaid_list = []

    tx_hash_list = []
    block_number_list = []
    timestamp_list = []

    wait_time = get_config_value(csv_name, 'wait_time', index)

    sub_wait_time = 0.25

    i = 1

    for event in events:
        time.sleep(wait_time)
        # print(event)
        # print()
        print('Batch of Events Processed: ', i, '/', len(events))
        # print()
        try:
            block = web3.eth.get_block(event['blockNumber'])
            block_number = int(block['number'])
        except:
            block_number = int(event['blockNumber'])

        user_address = event['args']['user']
        time.sleep(sub_wait_time)
        liquidator_address = event['args']['liquidator']
        time.sleep(sub_wait_time)

        collateral_asset = event['args']['collateralAsset']
        time.sleep(sub_wait_time)
        liquidated_amount = event['args']['liquidatedCollateralAmount']
        time.sleep(sub_wait_time)

        debt_asset = event['args']['debtAsset']
        time.sleep(sub_wait_time)
        debt_repaid = event['args']['debtToCover']
        time.sleep(sub_wait_time)

        tx_hash = event['transactionHash'].hex()
        time.sleep(sub_wait_time)
        timestamp = block['timestamp']

        user_address_list.append(user_address)
        liquidator_address_list.append(liquidator_address)
        collateral_asset_list.append(collateral_asset)
        liquidated_collateral_amount_list.append(liquidated_amount)
        debt_asset_list.append(debt_asset)
        debt_repaid_list.append(debt_repaid)
        tx_hash_list.append(tx_hash)
        timestamp_list.append(timestamp)
        block_number_list.append(block_number)

        # print(liquidated_amount)

        i += 1
    
    if len(user_address_list) > 0:
        df['user_address'] = user_address_list
        df['liquidator_address'] = liquidator_address_list
        df['collateral_asset'] = collateral_asset_list
        df['liquidated_amount'] = liquidated_collateral_amount_list
        df['debt_asset'] = debt_asset_list
        df['debt_repaid'] = debt_repaid_list
        df['tx_hash'] = tx_hash_list
        df['timestamp'] = timestamp_list
        df['block_number'] = block_number_list

    return df

#makes a dataframe and stores it in a csv file
def make_user_data_csv(df, config_csv_name, index):

    event_csv = get_config_value(config_csv_name, 'event_csv_name', index)
    old_df = pd.read_csv(event_csv)

    # old_df = old_df.drop_duplicates(subset=['user_address', 'liquidator_address', 'collateral_asset','liquidated_amount', 'tx_hash'], keep='last')

    old_df = old_df.drop_duplicates()
    
    combined_df_list = [df, old_df]
    combined_df = pd.concat(combined_df_list)
    # combined_df = combined_df.drop_duplicates(subset=['user_address', 'liquidator_address', 'collateral_asset','liquidated_amount', 'tx_hash'], keep='last')

    combined_df = combined_df.drop_duplicates()
    
    if len(combined_df) >= len(old_df):
        event_csv = get_config_value(config_csv_name, 'event_csv_name', index)
        combined_df.to_csv(event_csv, index=False)
        print('Event CSV Updated')
    return

def find_all_liquidations(index):

    config_csv_name = 'liquidation_config.csv'

    config_df = get_config_df(config_csv_name)

    from_block = get_from_block(config_csv_name, index)

    rpc_url = get_config_value(config_csv_name, 'rpc_url', index)
    contract_address = get_config_value(config_csv_name, 'contract_address', index)

    contract_abi = get_lending_pool_abi()

    web3 = tf.get_web_3(rpc_url)

    contract = tf.get_contract(contract_address, contract_abi, web3)

    latest_block = tf.get_latest_block(web3) 

    event_csv = get_config_value(config_csv_name, 'event_csv_name', index)

    event_df = pd.read_csv(event_csv)

    token_config_df = get_config_df('token_config.csv')

    interval = get_config_value(config_csv_name, 'interval', index)

    wait_time = get_config_value(config_csv_name, 'wait_time', index)

    to_block = from_block + interval

    while to_block < latest_block:

        print('Current Event Block vs Latest Event Block to Check: ', from_block, '/', latest_block, 'Blocks Remaining: ', latest_block - from_block)

        events = get_liquidation_events(contract, from_block, to_block)

        if len(events) > 0:
            contract_df = get_liquidation_df(events, web3, config_csv_name, index)

            if len(contract_df) > 0:
                make_user_data_csv(contract_df, config_csv_name, index)
        
        config_df['last_block'] = from_block
        config_df.to_csv(config_csv_name, index=False)

        from_block += interval
        to_block += interval

        time.sleep(wait_time)

    return

def loop_all_events(index_list):
    for index in index_list:
        try:
            find_all_liquidations(index)
        except:
            time.sleep(60)
            loop_all_events(index_list)

index_list = [0]

# loop_all_events(index_list)

find_all_liquidations(0)