from web3 import Web3
from web3.middleware import geth_poa_middleware 
import pandas as pd
import time
import datetime

# # makes our web3 object and injects it's middleware
def get_web_3(rpc_url):

    if 'wss' in rpc_url:
        provider = Web3.WebsocketProvider(rpc_url)
        web3 = Web3(provider)
    else:
        web3 = Web3(Web3.HTTPProvider(rpc_url))
    time.sleep(2.5)
    web3.middleware_onion.inject(geth_poa_middleware, layer=0)
    time.sleep(2.5)
    
    return web3

# # reads our config from our static csv
def get_config_df():
    config_df = pd.read_csv('cdp_config.csv')

    return config_df
# # gets our duplicate subset
def get_csv_subset(index):
    subset_list = [['liquidator_address', 'tx_hash', 'collateral_redeemed'], ['trove_owner', 'tx_hash', 'collateral_redeemed'], ['trove_owner', 'tx_hash', 'collateral_redeemed']]

    subset = subset_list[index]

    return subset

# # gets a dataframe with specific asset pricing values
def get_token_info_df():
    df = pd.DataFrame()

    token_address_list = ['0x09Bc4E0D864854c6aFB6eB9A9cdF58aC190D0dF9', '0x201EBa5CC46D216Ce6DC03F6a759e8E766e956aE', '0xCAbAE6f6Ea1ecaB08Ad02fE02ce9A44F09aebfA2', '0xdEAddEaDdeadDEadDEADDEAddEADDEAddead1111', '0x78c1b0C915c4FAA5FffA6CAbf0219DA63d7f4cb8', '0xcda86a272531e8640cd7f1a92c01839911b90bb0', '0x5bE26527e817998A7206475496fDE1E68957c5A6']
    token_symbol_list = ['USDC', 'USDT', 'WBTC', 'WETH', 'MNT', 'mETH', 'USDY']
    division_list = [1e6, 1e6, 1e8, 1e18, 1e18, 1e18, 1e18]

    df['token_address'] = token_address_list
    df['token_symbol'] = token_symbol_list
    df['division'] = division_list

    return df

# # will return data from our config_df depending on the column name and index
def get_config_df_value(column_name, index):
    
    df = get_config_df()

    config_list = df[column_name].tolist()

    config_value = config_list[index]

    return config_value

# # finds the latest block on a given blockchain
def get_latest_block(web3):

    latest_block = web3.eth.get_block_number()

    return latest_block

# # finds our contract launch_block
def get_from_block(index):
    from_block_list = [51922528, 51922528, 51922639]

    from_block = from_block_list[index]

    csv = get_config_df_value('csv', index)
    block_column_name = get_config_df_value('block_column_name', index)

    last_block_checked = get_last_block_tracked(csv, block_column_name, index)

    if last_block_checked > from_block:
        from_block = last_block_checked

    return from_block

# # gets our trove_manager address
def get_trove_manager_address(index):
    trove_manager_list = ['0x295c6074F090f85819cbC911266522e43A8e0f4A', '0x295c6074F090f85819cbC911266522e43A8e0f4A']

    trove_manager_address = trove_manager_list[index]

    return trove_manager_address

# # gets our trove_manager address
def get_borrower_operations_address(index):
    borrower_operations_list = ['0x4Cd23F2C694F991029B85af5575D0B5E70e4A3F1']

    borrower_operations_address = borrower_operations_list[index]

    return borrower_operations_address

# # gets the ABI for our redemption contract
def get_trove_manager_abi():
    contract_abi = [{"inputs":[],"stateMutability":"nonpayable","type":"constructor"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"address","name":"_activePoolAddress","type":"address"}],"name":"ActivePoolAddressChanged","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"uint256","name":"_baseRate","type":"uint256"}],"name":"BaseRateUpdated","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"address","name":"_newBorrowerOperationsAddress","type":"address"}],"name":"BorrowerOperationsAddressChanged","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"address","name":"_collSurplusPoolAddress","type":"address"}],"name":"CollSurplusPoolAddressChanged","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"address","name":"_newCollateralConfigAddress","type":"address"}],"name":"CollateralConfigAddressChanged","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"address","name":"_defaultPoolAddress","type":"address"}],"name":"DefaultPoolAddressChanged","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"address","name":"_gasPoolAddress","type":"address"}],"name":"GasPoolAddressChanged","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"address","name":"_lqtyStakingAddress","type":"address"}],"name":"LQTYStakingAddressChanged","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"address","name":"_lqtyTokenAddress","type":"address"}],"name":"LQTYTokenAddressChanged","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"address","name":"_collateral","type":"address"},{"indexed":False,"internalType":"uint256","name":"_L_Collateral","type":"uint256"},{"indexed":False,"internalType":"uint256","name":"_L_LUSDDebt","type":"uint256"}],"name":"LTermsUpdated","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"address","name":"_newLUSDTokenAddress","type":"address"}],"name":"LUSDTokenAddressChanged","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"uint256","name":"_lastFeeOpTime","type":"uint256"}],"name":"LastFeeOpTimeUpdated","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"address","name":"_collateral","type":"address"},{"indexed":False,"internalType":"uint256","name":"_liquidatedDebt","type":"uint256"},{"indexed":False,"internalType":"uint256","name":"_liquidatedColl","type":"uint256"},{"indexed":False,"internalType":"uint256","name":"_collGasCompensation","type":"uint256"},{"indexed":False,"internalType":"uint256","name":"_LUSDGasCompensation","type":"uint256"}],"name":"Liquidation","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"address","name":"_liquidationHelperAddress","type":"address"}],"name":"LiquidationHelperAddressChanged","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"address","name":"_newPriceFeedAddress","type":"address"}],"name":"PriceFeedAddressChanged","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"address","name":"_collateral","type":"address"},{"indexed":False,"internalType":"uint256","name":"_attemptedLUSDAmount","type":"uint256"},{"indexed":False,"internalType":"uint256","name":"_actualLUSDAmount","type":"uint256"},{"indexed":False,"internalType":"uint256","name":"_collSent","type":"uint256"},{"indexed":False,"internalType":"uint256","name":"_collFee","type":"uint256"}],"name":"Redemption","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"address","name":"_redemptionHelperAddress","type":"address"}],"name":"RedemptionHelperAddressChanged","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"address","name":"_sortedTrovesAddress","type":"address"}],"name":"SortedTrovesAddressChanged","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"address","name":"_stabilityPoolAddress","type":"address"}],"name":"StabilityPoolAddressChanged","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"address","name":"_collateral","type":"address"},{"indexed":False,"internalType":"uint256","name":"_totalStakesSnapshot","type":"uint256"},{"indexed":False,"internalType":"uint256","name":"_totalCollateralSnapshot","type":"uint256"}],"name":"SystemSnapshotsUpdated","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"address","name":"_collateral","type":"address"},{"indexed":False,"internalType":"uint256","name":"_newTotalStakes","type":"uint256"}],"name":"TotalStakesUpdated","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"address","name":"_borrower","type":"address"},{"indexed":False,"internalType":"address","name":"_collateral","type":"address"},{"indexed":False,"internalType":"uint256","name":"_newIndex","type":"uint256"}],"name":"TroveIndexUpdated","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"_borrower","type":"address"},{"indexed":False,"internalType":"address","name":"_collateral","type":"address"},{"indexed":False,"internalType":"uint256","name":"_debt","type":"uint256"},{"indexed":False,"internalType":"uint256","name":"_coll","type":"uint256"},{"indexed":False,"internalType":"enum TroveManager.TroveManagerOperation","name":"_operation","type":"uint8"}],"name":"TroveLiquidated","type":"event"},{"anonymous":False,"inputs":[{"indexed":False,"internalType":"address","name":"_collateral","type":"address"},{"indexed":False,"internalType":"uint256","name":"_L_Collateral","type":"uint256"},{"indexed":False,"internalType":"uint256","name":"_L_LUSDDebt","type":"uint256"}],"name":"TroveSnapshotsUpdated","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"_borrower","type":"address"},{"indexed":False,"internalType":"address","name":"_collateral","type":"address"},{"indexed":False,"internalType":"uint256","name":"_debt","type":"uint256"},{"indexed":False,"internalType":"uint256","name":"_coll","type":"uint256"},{"indexed":False,"internalType":"uint256","name":"_stake","type":"uint256"},{"indexed":False,"internalType":"enum TroveManager.TroveManagerOperation","name":"_operation","type":"uint8"}],"name":"TroveUpdated","type":"event"},{"inputs":[],"name":"BETA","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"BORROWING_FEE_FLOOR","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"DECIMAL_PRECISION","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"LUSD_GAS_COMPENSATION","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"L_Collateral","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"L_LUSDDebt","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"MAX_BORROWING_FEE","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"MINUTE_DECAY_FACTOR","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"MIN_NET_DEBT","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"PERCENT_DIVISOR","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"REDEMPTION_FEE_FLOOR","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"SECONDS_IN_ONE_MINUTE","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"},{"internalType":"uint256","name":"","type":"uint256"}],"name":"TroveOwners","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"},{"internalType":"address","name":"","type":"address"}],"name":"Troves","outputs":[{"internalType":"uint256","name":"debt","type":"uint256"},{"internalType":"uint256","name":"coll","type":"uint256"},{"internalType":"uint256","name":"stake","type":"uint256"},{"internalType":"enum TroveStatus","name":"status","type":"uint8"},{"internalType":"uint128","name":"arrayIndex","type":"uint128"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"_100pct","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"activePool","outputs":[{"internalType":"contract IActivePool","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_borrower","type":"address"},{"internalType":"address","name":"_collateral","type":"address"}],"name":"addTroveOwnerToArray","outputs":[{"internalType":"uint256","name":"index","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_borrower","type":"address"},{"internalType":"address","name":"_collateral","type":"address"}],"name":"applyPendingRewards","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"baseRate","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_collateral","type":"address"},{"internalType":"address[]","name":"_troveArray","type":"address[]"}],"name":"batchLiquidateTroves","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"borrowerOperationsAddress","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_redeemer","type":"address"},{"internalType":"address","name":"_collateral","type":"address"},{"internalType":"uint256","name":"_attemptedLUSDAmount","type":"uint256"},{"internalType":"uint256","name":"_actualLUSDAmount","type":"uint256"},{"internalType":"uint256","name":"_collSent","type":"uint256"},{"internalType":"uint256","name":"_collFee","type":"uint256"}],"name":"burnLUSDAndEmitRedemptionEvent","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_collateral","type":"address"},{"internalType":"uint256","name":"_price","type":"uint256"}],"name":"checkRecoveryMode","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_borrower","type":"address"},{"internalType":"address","name":"_collateral","type":"address"},{"internalType":"uint256","name":"_closedStatusNum","type":"uint256"}],"name":"closeTrove","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"collateralConfig","outputs":[{"internalType":"contract ICollateralConfig","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"decayBaseRateFromBorrowing","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_borrower","type":"address"},{"internalType":"address","name":"_collateral","type":"address"},{"internalType":"uint256","name":"_collDecrease","type":"uint256"}],"name":"decreaseTroveColl","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_borrower","type":"address"},{"internalType":"address","name":"_collateral","type":"address"},{"internalType":"uint256","name":"_debtDecrease","type":"uint256"}],"name":"decreaseTroveDebt","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"defaultPool","outputs":[{"internalType":"contract IDefaultPool","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_collateral","type":"address"},{"internalType":"uint256","name":"_liquidatedDebt","type":"uint256"},{"internalType":"uint256","name":"_liquidatedColl","type":"uint256"},{"internalType":"uint256","name":"_collGasCompensation","type":"uint256"},{"internalType":"uint256","name":"_LUSDGasCompensation","type":"uint256"}],"name":"emitLiquidationEvent","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_borrower","type":"address"},{"internalType":"address","name":"_collateral","type":"address"},{"internalType":"uint256","name":"_debt","type":"uint256"},{"internalType":"uint256","name":"_coll","type":"uint256"},{"internalType":"bool","name":"_isRecoveryMode","type":"bool"}],"name":"emitTroveLiquidatedAndTroveUpdated","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"_LUSDDebt","type":"uint256"}],"name":"getBorrowingFee","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"_LUSDDebt","type":"uint256"}],"name":"getBorrowingFeeWithDecay","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"getBorrowingRate","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"getBorrowingRateWithDecay","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_borrower","type":"address"},{"internalType":"address","name":"_collateral","type":"address"},{"internalType":"uint256","name":"_price","type":"uint256"}],"name":"getCurrentICR","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_borrower","type":"address"},{"internalType":"address","name":"_collateral","type":"address"}],"name":"getEntireDebtAndColl","outputs":[{"internalType":"uint256","name":"debt","type":"uint256"},{"internalType":"uint256","name":"coll","type":"uint256"},{"internalType":"uint256","name":"pendingLUSDDebtReward","type":"uint256"},{"internalType":"uint256","name":"pendingCollateralReward","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_collateral","type":"address"}],"name":"getEntireSystemColl","outputs":[{"internalType":"uint256","name":"entireSystemColl","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_collateral","type":"address"}],"name":"getEntireSystemDebt","outputs":[{"internalType":"uint256","name":"entireSystemDebt","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_borrower","type":"address"},{"internalType":"address","name":"_collateral","type":"address"}],"name":"getNominalICR","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_borrower","type":"address"},{"internalType":"address","name":"_collateral","type":"address"}],"name":"getPendingCollateralReward","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_borrower","type":"address"},{"internalType":"address","name":"_collateral","type":"address"}],"name":"getPendingLUSDDebtReward","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"_collateralDrawn","type":"uint256"}],"name":"getRedemptionFee","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"_collateralDrawn","type":"uint256"}],"name":"getRedemptionFeeWithDecay","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"getRedemptionRate","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"getRedemptionRateWithDecay","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_collateral","type":"address"},{"internalType":"uint256","name":"_price","type":"uint256"}],"name":"getTCR","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_borrower","type":"address"},{"internalType":"address","name":"_collateral","type":"address"}],"name":"getTroveColl","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_borrower","type":"address"},{"internalType":"address","name":"_collateral","type":"address"}],"name":"getTroveDebt","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_collateral","type":"address"},{"internalType":"uint256","name":"_index","type":"uint256"}],"name":"getTroveFromTroveOwnersArray","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_collateral","type":"address"}],"name":"getTroveOwnersCount","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_borrower","type":"address"},{"internalType":"address","name":"_collateral","type":"address"}],"name":"getTroveStake","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_borrower","type":"address"},{"internalType":"address","name":"_collateral","type":"address"}],"name":"getTroveStatus","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_borrower","type":"address"},{"internalType":"address","name":"_collateral","type":"address"}],"name":"hasPendingRewards","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_borrower","type":"address"},{"internalType":"address","name":"_collateral","type":"address"},{"internalType":"uint256","name":"_collIncrease","type":"uint256"}],"name":"increaseTroveColl","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_borrower","type":"address"},{"internalType":"address","name":"_collateral","type":"address"},{"internalType":"uint256","name":"_debtIncrease","type":"uint256"}],"name":"increaseTroveDebt","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"lastCollateralError_Redistribution","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"lastFeeOperationTime","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"lastLUSDDebtError_Redistribution","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_borrower","type":"address"},{"internalType":"address","name":"_collateral","type":"address"}],"name":"liquidate","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_collateral","type":"address"},{"internalType":"uint256","name":"_n","type":"uint256"}],"name":"liquidateTroves","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"liquidationHelper","outputs":[{"internalType":"contract ILiquidationHelper","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"lqtyStaking","outputs":[{"internalType":"contract ILQTYStaking","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"lqtyToken","outputs":[{"internalType":"contract IERC20","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"lusdToken","outputs":[{"internalType":"contract ILUSDToken","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"contract IActivePool","name":"_activePool","type":"address"},{"internalType":"contract IDefaultPool","name":"_defaultPool","type":"address"},{"internalType":"address","name":"_collateral","type":"address"},{"internalType":"uint256","name":"_LUSD","type":"uint256"},{"internalType":"uint256","name":"_collAmount","type":"uint256"}],"name":"movePendingTroveRewardsToActivePool","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"owner","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"priceFeed","outputs":[{"internalType":"contract IPriceFeed","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_id","type":"address"},{"internalType":"address","name":"_collateral","type":"address"},{"internalType":"uint256","name":"_newNICR","type":"uint256"},{"internalType":"address","name":"_prevId","type":"address"},{"internalType":"address","name":"_nextId","type":"address"}],"name":"reInsert","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_borrower","type":"address"},{"internalType":"address","name":"_collateral","type":"address"},{"internalType":"uint256","name":"_LUSD","type":"uint256"},{"internalType":"uint256","name":"_collAmount","type":"uint256"}],"name":"redeemCloseTrove","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_collateral","type":"address"},{"internalType":"uint256","name":"_LUSDamount","type":"uint256"},{"internalType":"address","name":"_firstRedemptionHint","type":"address"},{"internalType":"address","name":"_upperPartialRedemptionHint","type":"address"},{"internalType":"address","name":"_lowerPartialRedemptionHint","type":"address"},{"internalType":"uint256","name":"_partialRedemptionHintNICR","type":"uint256"},{"internalType":"uint256","name":"_maxIterations","type":"uint256"},{"internalType":"uint256","name":"_maxFeePercentage","type":"uint256"}],"name":"redeemCollateral","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"redemptionHelper","outputs":[{"internalType":"contract IRedemptionHelper","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"contract IActivePool","name":"_activePool","type":"address"},{"internalType":"contract IDefaultPool","name":"_defaultPool","type":"address"},{"internalType":"address","name":"_collateral","type":"address"},{"internalType":"uint256","name":"_debt","type":"uint256"},{"internalType":"uint256","name":"_coll","type":"uint256"},{"internalType":"uint256","name":"_collDecimals","type":"uint256"}],"name":"redistributeDebtAndColl","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_borrower","type":"address"},{"internalType":"address","name":"_collateral","type":"address"}],"name":"removeStake","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"},{"internalType":"address","name":"","type":"address"}],"name":"rewardSnapshots","outputs":[{"internalType":"uint256","name":"collAmount","type":"uint256"},{"internalType":"uint256","name":"LUSDDebt","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"contract IActivePool","name":"_activePool","type":"address"},{"internalType":"address","name":"_collateral","type":"address"},{"internalType":"address","name":"_liquidator","type":"address"},{"internalType":"uint256","name":"_LUSD","type":"uint256"},{"internalType":"uint256","name":"_collAmount","type":"uint256"}],"name":"sendGasCompensation","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_borrowerOperationsAddress","type":"address"},{"internalType":"address","name":"_collateralConfigAddress","type":"address"},{"internalType":"address","name":"_activePoolAddress","type":"address"},{"internalType":"address","name":"_defaultPoolAddress","type":"address"},{"internalType":"address","name":"_gasPoolAddress","type":"address"},{"internalType":"address","name":"_collSurplusPoolAddress","type":"address"},{"internalType":"address","name":"_priceFeedAddress","type":"address"},{"internalType":"address","name":"_lusdTokenAddress","type":"address"},{"internalType":"address","name":"_sortedTrovesAddress","type":"address"},{"internalType":"address","name":"_lqtyTokenAddress","type":"address"},{"internalType":"address","name":"_lqtyStakingAddress","type":"address"},{"internalType":"address","name":"_redemptionHelperAddress","type":"address"},{"internalType":"address","name":"_liquidationHelperAddress","type":"address"}],"name":"setAddresses","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_borrower","type":"address"},{"internalType":"address","name":"_collateral","type":"address"},{"internalType":"uint256","name":"_num","type":"uint256"}],"name":"setTroveStatus","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"sortedTroves","outputs":[{"internalType":"contract ISortedTroves","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"totalCollateralSnapshot","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"totalStakes","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"totalStakesSnapshot","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"_collateralDrawn","type":"uint256"},{"internalType":"uint256","name":"_price","type":"uint256"},{"internalType":"uint256","name":"_collDecimals","type":"uint256"},{"internalType":"uint256","name":"_collDebt","type":"uint256"}],"name":"updateBaseRateFromRedemption","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_borrower","type":"address"},{"internalType":"address","name":"_collateral","type":"address"},{"internalType":"uint256","name":"_newDebt","type":"uint256"},{"internalType":"uint256","name":"_newColl","type":"uint256"}],"name":"updateDebtAndCollAndStakesPostRedemption","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_borrower","type":"address"},{"internalType":"address","name":"_collateral","type":"address"}],"name":"updateStakeAndTotalStakes","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"contract IActivePool","name":"_activePool","type":"address"},{"internalType":"address","name":"_collateral","type":"address"},{"internalType":"uint256","name":"_collRemainder","type":"uint256"}],"name":"updateSystemSnapshots_excludeCollRemainder","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"_borrower","type":"address"},{"internalType":"address","name":"_collateral","type":"address"}],"name":"updateTroveRewardSnapshots","outputs":[],"stateMutability":"nonpayable","type":"function"}]
    return contract_abi

# # gets the ABI for our borrower operations contract
def get_borrower_operations_abi():
    contract_abi = [ { "anonymous": False, "inputs": [ { "indexed": False, "internalType": "address", "name": "_activePoolAddress", "type": "address" } ], "name": "ActivePoolAddressChanged", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": False, "internalType": "address", "name": "_collSurplusPoolAddress", "type": "address" } ], "name": "CollSurplusPoolAddressChanged", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": False, "internalType": "address", "name": "_newCollateralConfigAddress", "type": "address" } ], "name": "CollateralConfigAddressChanged", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": False, "internalType": "address", "name": "_defaultPoolAddress", "type": "address" } ], "name": "DefaultPoolAddressChanged", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": False, "internalType": "address", "name": "_gasPoolAddress", "type": "address" } ], "name": "GasPoolAddressChanged", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": False, "internalType": "address", "name": "_lqtyStakingAddress", "type": "address" } ], "name": "LQTYStakingAddressChanged", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "address", "name": "_borrower", "type": "address" }, { "indexed": False, "internalType": "address", "name": "_collateral", "type": "address" }, { "indexed": False, "internalType": "uint256", "name": "_LUSDFee", "type": "uint256" } ], "name": "LUSDBorrowingFeePaid", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": False, "internalType": "address", "name": "_lusdTokenAddress", "type": "address" } ], "name": "LUSDTokenAddressChanged", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": False, "internalType": "address", "name": "_leverager", "type": "address" } ], "name": "LeveragerAddressChanged", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "address", "name": "previousOwner", "type": "address" }, { "indexed": True, "internalType": "address", "name": "newOwner", "type": "address" } ], "name": "OwnershipTransferred", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": False, "internalType": "address", "name": "_newPriceFeedAddress", "type": "address" } ], "name": "PriceFeedAddressChanged", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": False, "internalType": "address", "name": "_borrower", "type": "address" }, { "indexed": False, "internalType": "bool", "name": "_isExempt", "type": "bool" } ], "name": "SetFeeExemption", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": False, "internalType": "address", "name": "_sortedTrovesAddress", "type": "address" } ], "name": "SortedTrovesAddressChanged", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "address", "name": "_borrower", "type": "address" }, { "indexed": False, "internalType": "address", "name": "_collateral", "type": "address" }, { "indexed": False, "internalType": "uint256", "name": "arrayIndex", "type": "uint256" } ], "name": "TroveCreated", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": False, "internalType": "address", "name": "_newTroveManagerAddress", "type": "address" } ], "name": "TroveManagerAddressChanged", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "address", "name": "_borrower", "type": "address" }, { "indexed": False, "internalType": "address", "name": "_collateral", "type": "address" }, { "indexed": False, "internalType": "uint256", "name": "_debt", "type": "uint256" }, { "indexed": False, "internalType": "uint256", "name": "_coll", "type": "uint256" }, { "indexed": False, "internalType": "uint256", "name": "stake", "type": "uint256" }, { "indexed": False, "internalType": "enum BorrowerOperations.BorrowerOperation", "name": "operation", "type": "uint8" } ], "name": "TroveUpdated", "type": "event" }, { "inputs": [], "name": "BORROWING_FEE_FLOOR", "outputs": [ { "internalType": "uint256", "name": "", "type": "uint256" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "DECIMAL_PRECISION", "outputs": [ { "internalType": "uint256", "name": "", "type": "uint256" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "LUSD_GAS_COMPENSATION", "outputs": [ { "internalType": "uint256", "name": "", "type": "uint256" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "MIN_NET_DEBT", "outputs": [ { "internalType": "uint256", "name": "", "type": "uint256" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "NAME", "outputs": [ { "internalType": "string", "name": "", "type": "string" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "PERCENT_DIVISOR", "outputs": [ { "internalType": "uint256", "name": "", "type": "uint256" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "_100pct", "outputs": [ { "internalType": "uint256", "name": "", "type": "uint256" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "activePool", "outputs": [ { "internalType": "contract IActivePool", "name": "", "type": "address" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "_collateral", "type": "address" }, { "internalType": "uint256", "name": "_collAmount", "type": "uint256" }, { "internalType": "address", "name": "_upperHint", "type": "address" }, { "internalType": "address", "name": "_lowerHint", "type": "address" } ], "name": "addColl", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "_collateral", "type": "address" }, { "internalType": "uint256", "name": "_maxFeePercentage", "type": "uint256" }, { "internalType": "uint256", "name": "_collTopUp", "type": "uint256" }, { "internalType": "uint256", "name": "_collWithdrawal", "type": "uint256" }, { "internalType": "uint256", "name": "_LUSDChange", "type": "uint256" }, { "internalType": "bool", "name": "_isDebtIncrease", "type": "bool" }, { "internalType": "address", "name": "_upperHint", "type": "address" }, { "internalType": "address", "name": "_lowerHint", "type": "address" } ], "name": "adjustTrove", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "components": [ { "internalType": "address", "name": "_borrower", "type": "address" }, { "internalType": "address", "name": "_collateral", "type": "address" }, { "internalType": "uint256", "name": "_maxFeePercentage", "type": "uint256" }, { "internalType": "uint256", "name": "_collTopUp", "type": "uint256" }, { "internalType": "uint256", "name": "_collWithdrawal", "type": "uint256" }, { "internalType": "uint256", "name": "_LUSDChange", "type": "uint256" }, { "internalType": "bool", "name": "_isDebtIncrease", "type": "bool" }, { "internalType": "address", "name": "_upperHint", "type": "address" }, { "internalType": "address", "name": "_lowerHint", "type": "address" } ], "internalType": "struct IBorrowerOperations.Params_adjustTroveFor", "name": "params", "type": "tuple" } ], "name": "adjustTroveFor", "outputs": [ { "internalType": "address", "name": "", "type": "address" }, { "internalType": "address", "name": "", "type": "address" } ], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "_collateral", "type": "address" } ], "name": "claimCollateral", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "_collateral", "type": "address" } ], "name": "closeTrove", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "_borrower", "type": "address" }, { "internalType": "address", "name": "_collateral", "type": "address" } ], "name": "closeTroveFor", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [], "name": "collateralConfig", "outputs": [ { "internalType": "contract ICollateralConfig", "name": "", "type": "address" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "defaultPool", "outputs": [ { "internalType": "contract IDefaultPool", "name": "", "type": "address" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "", "type": "address" } ], "name": "exemptFromFee", "outputs": [ { "internalType": "bool", "name": "", "type": "bool" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "uint256", "name": "_debt", "type": "uint256" } ], "name": "getCompositeDebt", "outputs": [ { "internalType": "uint256", "name": "", "type": "uint256" } ], "stateMutability": "pure", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "_collateral", "type": "address" } ], "name": "getEntireSystemColl", "outputs": [ { "internalType": "uint256", "name": "entireSystemColl", "type": "uint256" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "_collateral", "type": "address" } ], "name": "getEntireSystemDebt", "outputs": [ { "internalType": "uint256", "name": "entireSystemDebt", "type": "uint256" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "initialized", "outputs": [ { "internalType": "bool", "name": "", "type": "bool" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "leveragerAddress", "outputs": [ { "internalType": "address", "name": "", "type": "address" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "lqtyStakingAddress", "outputs": [ { "internalType": "address", "name": "", "type": "address" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "lusdToken", "outputs": [ { "internalType": "contract ILUSDToken", "name": "", "type": "address" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "_collateral", "type": "address" }, { "internalType": "uint256", "name": "_collAmount", "type": "uint256" }, { "internalType": "uint256", "name": "_maxFeePercentage", "type": "uint256" }, { "internalType": "uint256", "name": "_LUSDAmount", "type": "uint256" }, { "internalType": "address", "name": "_upperHint", "type": "address" }, { "internalType": "address", "name": "_lowerHint", "type": "address" } ], "name": "openTrove", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "_borrower", "type": "address" }, { "internalType": "address", "name": "_collateral", "type": "address" }, { "internalType": "uint256", "name": "_collAmount", "type": "uint256" }, { "internalType": "uint256", "name": "_maxFeePercentage", "type": "uint256" }, { "internalType": "uint256", "name": "_LUSDAmount", "type": "uint256" }, { "internalType": "address", "name": "_upperHint", "type": "address" }, { "internalType": "address", "name": "_lowerHint", "type": "address" } ], "name": "openTroveFor", "outputs": [ { "internalType": "address", "name": "", "type": "address" }, { "internalType": "address", "name": "", "type": "address" } ], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [], "name": "owner", "outputs": [ { "internalType": "address", "name": "", "type": "address" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "priceFeed", "outputs": [ { "internalType": "contract IPriceFeed", "name": "", "type": "address" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "renounceOwnership", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "_collateral", "type": "address" }, { "internalType": "uint256", "name": "_LUSDAmount", "type": "uint256" }, { "internalType": "address", "name": "_upperHint", "type": "address" }, { "internalType": "address", "name": "_lowerHint", "type": "address" } ], "name": "repayLUSD", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "_collateralConfigAddress", "type": "address" }, { "internalType": "address", "name": "_troveManagerAddress", "type": "address" }, { "internalType": "address", "name": "_activePoolAddress", "type": "address" }, { "internalType": "address", "name": "_defaultPoolAddress", "type": "address" }, { "internalType": "address", "name": "_gasPoolAddress", "type": "address" }, { "internalType": "address", "name": "_collSurplusPoolAddress", "type": "address" }, { "internalType": "address", "name": "_priceFeedAddress", "type": "address" }, { "internalType": "address", "name": "_sortedTrovesAddress", "type": "address" }, { "internalType": "address", "name": "_lusdTokenAddress", "type": "address" }, { "internalType": "address", "name": "_lqtyStakingAddress", "type": "address" }, { "internalType": "address", "name": "_leveragerAddress", "type": "address" } ], "name": "setAddresses", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "_borrower", "type": "address" }, { "internalType": "bool", "name": "_isExempt", "type": "bool" } ], "name": "setExemptFromFee", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "_leveragerAddress", "type": "address" } ], "name": "setLeveragerAddress", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [], "name": "sortedTroves", "outputs": [ { "internalType": "contract ISortedTroves", "name": "", "type": "address" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "newOwner", "type": "address" } ], "name": "transferOwnership", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [], "name": "troveManager", "outputs": [ { "internalType": "contract ITroveManager", "name": "", "type": "address" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "_collateral", "type": "address" }, { "internalType": "uint256", "name": "_collWithdrawal", "type": "uint256" }, { "internalType": "address", "name": "_upperHint", "type": "address" }, { "internalType": "address", "name": "_lowerHint", "type": "address" } ], "name": "withdrawColl", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "_collateral", "type": "address" }, { "internalType": "uint256", "name": "_maxFeePercentage", "type": "uint256" }, { "internalType": "uint256", "name": "_LUSDAmount", "type": "uint256" }, { "internalType": "address", "name": "_upperHint", "type": "address" }, { "internalType": "address", "name": "_lowerHint", "type": "address" } ], "name": "withdrawLUSD", "outputs": [], "stateMutability": "nonpayable", "type": "function" } ]
    return contract_abi

# # returns the appropriate index
def get_abi(index):

    trove_manager_abi = get_trove_manager_abi()
    borrower_operations_abi = get_borrower_operations_abi()

    abi_list = [trove_manager_abi, borrower_operations_abi, trove_manager_abi]

    abi = abi_list[index]

    return abi

# # gets our web3 contract object
def get_contract(contract_address, contract_abi, web3):

    contract = web3.eth.contract(address=contract_address, abi=contract_abi)
    
    return contract

# # gets our redemption events
def get_redemption_events(contract, from_block, to_block):
    
    events = contract.events.Redemption.get_logs(fromBlock=from_block, toBlock=to_block)

    return events

# # gets our troveUpdated events
def get_trove_updated_events(contract, from_block, to_block):
    
    events = contract.events.TroveUpdated.get_logs(fromBlock=from_block, toBlock=to_block)

    return events

# # gets the last block number we have gotten data from and returns this block number
def get_last_block_tracked(csv_name, last_block_column_name, index):

    redemption_index_list = get_redemption_index_list()

    df = pd.read_csv(csv_name)
    
    last_block_monitored = 0

    if len(df) > 1:
        if index not in redemption_index_list:
            trove_updated_contract_type = get_trove_type_value(index)
            
            df = df.loc[df['contract_type'] == trove_updated_contract_type]

        last_block_monitored = df[last_block_column_name].max()

        try:
            last_block_monitored = int(last_block_monitored)
        except:
            pass

    return last_block_monitored

# # makes all the addresses in a given df[column] to their checksum version
def get_checksum_values(df, column_name):

    address_list = df[column_name].to_list()

    checksum_address_list = [Web3.to_checksum_address(x) for x in address_list]

    df[column_name] = checksum_address_list

    return df

# # takes in an a_token address and returns it's contract object
def get_a_token_contract(contract_address, web3):
    # contract_address = "0xEB329420Fae03176EC5877c34E2c38580D85E069"
    contract_abi = [{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"owner","type":"address"},{"indexed":True,"internalType":"address","name":"spender","type":"address"},{"indexed":False,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Approval","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"from","type":"address"},{"indexed":True,"internalType":"address","name":"to","type":"address"},{"indexed":False,"internalType":"uint256","name":"value","type":"uint256"},{"indexed":False,"internalType":"uint256","name":"index","type":"uint256"}],"name":"BalanceTransfer","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"from","type":"address"},{"indexed":True,"internalType":"address","name":"target","type":"address"},{"indexed":False,"internalType":"uint256","name":"value","type":"uint256"},{"indexed":False,"internalType":"uint256","name":"index","type":"uint256"}],"name":"Burn","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"underlyingAsset","type":"address"},{"indexed":True,"internalType":"address","name":"pool","type":"address"},{"indexed":False,"internalType":"address","name":"treasury","type":"address"},{"indexed":False,"internalType":"address","name":"incentivesController","type":"address"},{"indexed":False,"internalType":"uint8","name":"aTokenDecimals","type":"uint8"},{"indexed":False,"internalType":"string","name":"aTokenName","type":"string"},{"indexed":False,"internalType":"string","name":"aTokenSymbol","type":"string"},{"indexed":False,"internalType":"bytes","name":"params","type":"bytes"}],"name":"Initialized","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"from","type":"address"},{"indexed":False,"internalType":"uint256","name":"value","type":"uint256"},{"indexed":False,"internalType":"uint256","name":"index","type":"uint256"}],"name":"Mint","type":"event"},{"anonymous":False,"inputs":[{"indexed":True,"internalType":"address","name":"from","type":"address"},{"indexed":True,"internalType":"address","name":"to","type":"address"},{"indexed":False,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Transfer","type":"event"},{"inputs":[],"name":"ATOKEN_REVISION","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"DOMAIN_SEPARATOR","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"EIP712_REVISION","outputs":[{"internalType":"bytes","name":"","type":"bytes"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"PERMIT_TYPEHASH","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"POOL","outputs":[{"internalType":"contract ILendingPool","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"RESERVE_TREASURY_ADDRESS","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"UNDERLYING_ASSET_ADDRESS","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"_nonces","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"owner","type":"address"},{"internalType":"address","name":"spender","type":"address"}],"name":"allowance","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"}],"name":"approve","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"user","type":"address"}],"name":"balanceOf","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"user","type":"address"},{"internalType":"address","name":"receiverOfUnderlying","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"},{"internalType":"uint256","name":"index","type":"uint256"}],"name":"burn","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"decimals","outputs":[{"internalType":"uint8","name":"","type":"uint8"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"subtractedValue","type":"uint256"}],"name":"decreaseAllowance","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"getIncentivesController","outputs":[{"internalType":"contract IAaveIncentivesController","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"user","type":"address"}],"name":"getScaledUserBalanceAndSupply","outputs":[{"internalType":"uint256","name":"","type":"uint256"},{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"user","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"}],"name":"handleRepayment","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"addedValue","type":"uint256"}],"name":"increaseAllowance","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"contract ILendingPool","name":"pool","type":"address"},{"internalType":"address","name":"treasury","type":"address"},{"internalType":"address","name":"underlyingAsset","type":"address"},{"internalType":"contract IAaveIncentivesController","name":"incentivesController","type":"address"},{"internalType":"uint8","name":"aTokenDecimals","type":"uint8"},{"internalType":"string","name":"aTokenName","type":"string"},{"internalType":"string","name":"aTokenSymbol","type":"string"},{"internalType":"bytes","name":"params","type":"bytes"}],"name":"initialize","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"user","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"},{"internalType":"uint256","name":"index","type":"uint256"}],"name":"mint","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"amount","type":"uint256"},{"internalType":"uint256","name":"index","type":"uint256"}],"name":"mintToTreasury","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"name","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"owner","type":"address"},{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"},{"internalType":"uint256","name":"deadline","type":"uint256"},{"internalType":"uint8","name":"v","type":"uint8"},{"internalType":"bytes32","name":"r","type":"bytes32"},{"internalType":"bytes32","name":"s","type":"bytes32"}],"name":"permit","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"user","type":"address"}],"name":"scaledBalanceOf","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"scaledTotalSupply","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"symbol","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"totalSupply","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"}],"name":"transfer","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"sender","type":"address"},{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"}],"name":"transferFrom","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"value","type":"uint256"}],"name":"transferOnLiquidation","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"target","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"}],"name":"transferUnderlyingTo","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"nonpayable","type":"function"}]    
    contract = web3.eth.contract(address=contract_address, abi=contract_abi)

    return contract

# # takes in an v_token address and returns it's contract object
def get_v_token_contract(contract_address, web3):
    # contract_address = "0xBE8afE7E442fFfFE576B979D490c5ADb7823C3c6"
    contract_abi = [{"type":"event","name":"Approval","inputs":[{"type":"address","name":"owner","internalType":"address","indexed":True},{"type":"address","name":"spender","internalType":"address","indexed":True},{"type":"uint256","name":"value","internalType":"uint256","indexed":False}],"anonymous":False},{"type":"event","name":"BorrowAllowanceDelegated","inputs":[{"type":"address","name":"fromUser","internalType":"address","indexed":True},{"type":"address","name":"toUser","internalType":"address","indexed":True},{"type":"address","name":"asset","internalType":"address","indexed":False},{"type":"uint256","name":"amount","internalType":"uint256","indexed":False}],"anonymous":False},{"type":"event","name":"Burn","inputs":[{"type":"address","name":"user","internalType":"address","indexed":True},{"type":"uint256","name":"amount","internalType":"uint256","indexed":False},{"type":"uint256","name":"currentBalance","internalType":"uint256","indexed":False},{"type":"uint256","name":"balanceIncrease","internalType":"uint256","indexed":False},{"type":"uint256","name":"avgStableRate","internalType":"uint256","indexed":False},{"type":"uint256","name":"newTotalSupply","internalType":"uint256","indexed":False}],"anonymous":False},{"type":"event","name":"Initialized","inputs":[{"type":"address","name":"underlyingAsset","internalType":"address","indexed":True},{"type":"address","name":"pool","internalType":"address","indexed":True},{"type":"address","name":"incentivesController","internalType":"address","indexed":False},{"type":"uint8","name":"debtTokenDecimals","internalType":"uint8","indexed":False},{"type":"string","name":"debtTokenName","internalType":"string","indexed":False},{"type":"string","name":"debtTokenSymbol","internalType":"string","indexed":False},{"type":"bytes","name":"params","internalType":"bytes","indexed":False}],"anonymous":False},{"type":"event","name":"Mint","inputs":[{"type":"address","name":"user","internalType":"address","indexed":True},{"type":"address","name":"onBehalfOf","internalType":"address","indexed":True},{"type":"uint256","name":"amount","internalType":"uint256","indexed":False},{"type":"uint256","name":"currentBalance","internalType":"uint256","indexed":False},{"type":"uint256","name":"balanceIncrease","internalType":"uint256","indexed":False},{"type":"uint256","name":"newRate","internalType":"uint256","indexed":False},{"type":"uint256","name":"avgStableRate","internalType":"uint256","indexed":False},{"type":"uint256","name":"newTotalSupply","internalType":"uint256","indexed":False}],"anonymous":False},{"type":"event","name":"Transfer","inputs":[{"type":"address","name":"from","internalType":"address","indexed":True},{"type":"address","name":"to","internalType":"address","indexed":True},{"type":"uint256","name":"value","internalType":"uint256","indexed":False}],"anonymous":False},{"type":"function","stateMutability":"view","outputs":[{"type":"uint256","name":"","internalType":"uint256"}],"name":"DEBT_TOKEN_REVISION","inputs":[]},{"type":"function","stateMutability":"view","outputs":[{"type":"address","name":"","internalType":"contract ILendingPool"}],"name":"POOL","inputs":[]},{"type":"function","stateMutability":"view","outputs":[{"type":"address","name":"","internalType":"address"}],"name":"UNDERLYING_ASSET_ADDRESS","inputs":[]},{"type":"function","stateMutability":"view","outputs":[{"type":"uint256","name":"","internalType":"uint256"}],"name":"allowance","inputs":[{"type":"address","name":"owner","internalType":"address"},{"type":"address","name":"spender","internalType":"address"}]},{"type":"function","stateMutability":"nonpayable","outputs":[{"type":"bool","name":"","internalType":"bool"}],"name":"approve","inputs":[{"type":"address","name":"spender","internalType":"address"},{"type":"uint256","name":"amount","internalType":"uint256"}]},{"type":"function","stateMutability":"nonpayable","outputs":[],"name":"approveDelegation","inputs":[{"type":"address","name":"delegatee","internalType":"address"},{"type":"uint256","name":"amount","internalType":"uint256"}]},{"type":"function","stateMutability":"view","outputs":[{"type":"uint256","name":"","internalType":"uint256"}],"name":"balanceOf","inputs":[{"type":"address","name":"account","internalType":"address"}]},{"type":"function","stateMutability":"view","outputs":[{"type":"uint256","name":"","internalType":"uint256"}],"name":"borrowAllowance","inputs":[{"type":"address","name":"fromUser","internalType":"address"},{"type":"address","name":"toUser","internalType":"address"}]},{"type":"function","stateMutability":"nonpayable","outputs":[],"name":"burn","inputs":[{"type":"address","name":"user","internalType":"address"},{"type":"uint256","name":"amount","internalType":"uint256"}]},{"type":"function","stateMutability":"view","outputs":[{"type":"uint8","name":"","internalType":"uint8"}],"name":"decimals","inputs":[]},{"type":"function","stateMutability":"nonpayable","outputs":[{"type":"bool","name":"","internalType":"bool"}],"name":"decreaseAllowance","inputs":[{"type":"address","name":"spender","internalType":"address"},{"type":"uint256","name":"subtractedValue","internalType":"uint256"}]},{"type":"function","stateMutability":"view","outputs":[{"type":"uint256","name":"","internalType":"uint256"}],"name":"getAverageStableRate","inputs":[]},{"type":"function","stateMutability":"view","outputs":[{"type":"address","name":"","internalType":"contract IRewarder"}],"name":"getIncentivesController","inputs":[]},{"type":"function","stateMutability":"view","outputs":[{"type":"uint256","name":"","internalType":"uint256"},{"type":"uint256","name":"","internalType":"uint256"},{"type":"uint256","name":"","internalType":"uint256"},{"type":"uint40","name":"","internalType":"uint40"}],"name":"getSupplyData","inputs":[]},{"type":"function","stateMutability":"view","outputs":[{"type":"uint256","name":"","internalType":"uint256"},{"type":"uint256","name":"","internalType":"uint256"}],"name":"getTotalSupplyAndAvgRate","inputs":[]},{"type":"function","stateMutability":"view","outputs":[{"type":"uint40","name":"","internalType":"uint40"}],"name":"getTotalSupplyLastUpdated","inputs":[]},{"type":"function","stateMutability":"view","outputs":[{"type":"uint40","name":"","internalType":"uint40"}],"name":"getUserLastUpdated","inputs":[{"type":"address","name":"user","internalType":"address"}]},{"type":"function","stateMutability":"view","outputs":[{"type":"uint256","name":"","internalType":"uint256"}],"name":"getUserStableRate","inputs":[{"type":"address","name":"user","internalType":"address"}]},{"type":"function","stateMutability":"nonpayable","outputs":[{"type":"bool","name":"","internalType":"bool"}],"name":"increaseAllowance","inputs":[{"type":"address","name":"spender","internalType":"address"},{"type":"uint256","name":"addedValue","internalType":"uint256"}]},{"type":"function","stateMutability":"nonpayable","outputs":[],"name":"initialize","inputs":[{"type":"address","name":"pool","internalType":"contract ILendingPool"},{"type":"address","name":"underlyingAsset","internalType":"address"},{"type":"address","name":"incentivesController","internalType":"contract IRewarder"},{"type":"uint8","name":"debtTokenDecimals","internalType":"uint8"},{"type":"string","name":"debtTokenName","internalType":"string"},{"type":"string","name":"debtTokenSymbol","internalType":"string"},{"type":"bytes","name":"params","internalType":"bytes"}]},{"type":"function","stateMutability":"nonpayable","outputs":[{"type":"bool","name":"","internalType":"bool"}],"name":"mint","inputs":[{"type":"address","name":"user","internalType":"address"},{"type":"address","name":"onBehalfOf","internalType":"address"},{"type":"uint256","name":"amount","internalType":"uint256"},{"type":"uint256","name":"rate","internalType":"uint256"}]},{"type":"function","stateMutability":"view","outputs":[{"type":"string","name":"","internalType":"string"}],"name":"name","inputs":[]},{"type":"function","stateMutability":"view","outputs":[{"type":"uint256","name":"","internalType":"uint256"}],"name":"principalBalanceOf","inputs":[{"type":"address","name":"user","internalType":"address"}]},{"type":"function","stateMutability":"view","outputs":[{"type":"string","name":"","internalType":"string"}],"name":"symbol","inputs":[]},{"type":"function","stateMutability":"view","outputs":[{"type":"uint256","name":"","internalType":"uint256"}],"name":"totalSupply","inputs":[]},{"type":"function","stateMutability":"nonpayable","outputs":[{"type":"bool","name":"","internalType":"bool"}],"name":"transfer","inputs":[{"type":"address","name":"recipient","internalType":"address"},{"type":"uint256","name":"amount","internalType":"uint256"}]},{"type":"function","stateMutability":"nonpayable","outputs":[{"type":"bool","name":"","internalType":"bool"}],"name":"transferFrom","inputs":[{"type":"address","name":"sender","internalType":"address"},{"type":"address","name":"recipient","internalType":"address"},{"type":"uint256","name":"amount","internalType":"uint256"}]}]    
    contract = web3.eth.contract(address=contract_address, abi=contract_abi)

    return contract

# # takes in a contract object and returns all associated deposit events
def get_deposit_events(contract, from_block, to_block):

    # events = contract.events.Transfer.get_logs(fromBlock=from_block, toBlock=latest_block)
    events = contract.events.Deposit.get_logs(fromBlock=from_block, toBlock=to_block)

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

# # returns a list of relevant events
def get_events(contract, from_block, to_block, index):

    redemption_index_list = get_redemption_index_list()
    trove_updated_list = get_trove_updated_index_list()

    events = ''

    if index in redemption_index_list:
        events = get_redemption_events(contract, from_block, to_block)

    elif index in trove_updated_list:
        events = get_trove_updated_events(contract, from_block, to_block)

    return events

# # returns a list of redemption_indexes for our script
def get_redemption_index_list():
    redemption_index_list = [0]
    
    return redemption_index_list

# # returns a list of trove_updated_indexes for our script
def get_trove_updated_index_list():
    trove_updated_index_list = [1,2]
    
    return trove_updated_index_list

# # returns the index of of our borrower_operations
def get_borrower_operations_index_list():

    borrower_operations_index_list = [1]

    return borrower_operations_index_list

# # gets the the contract type we got our trove_update from
def get_trove_type_value(index):
    borrower_operations_index_list = get_borrower_operations_index_list()

    if index in borrower_operations_index_list:
        type_value = 'borrower_operations'
    
    else:
        type_value = 'trove_manager'

    return type_value

#makes a dataframe and stores it in a csv file
def make_user_data_csv(df, index):
    
    combined_df_list = []

    csv = get_config_df_value('csv', index)

    subset_list = get_csv_subset(index)

    if len(df) > 0:
        
        old_df = pd.read_csv(csv)
        old_df = old_df.drop_duplicates(subset=subset_list, keep='last')

        combined_df_list = [df, old_df]

        combined_df = pd.concat(combined_df_list)

        combined_df = combined_df.drop_duplicates(subset=subset_list, keep='last')
        
        if len(combined_df) >= len(old_df):
            combined_df.to_csv(csv, index=False)
            print('Event CSV Updated')

    return

#returns a df if a tx_hash exists
def tx_hash_exists(df, tx_hash):

    new_df = pd.DataFrame()

    if ((df['tx_hash'] == tx_hash)).any():
        new_df = df.loc[df['tx_hash'] == tx_hash]
    
    return new_df

# # sees if a redemption event's collateral_fee already exists
def collateral_fee_exists(df, collateral_fee):
    
    if ((df['collateral_fee'] == collateral_fee)).any():
        df = df.loc[df['collateral_fee'] == collateral_fee]

    else:
        df = pd.DataFrame()

    return df

# # sees if a borrower_operation event's number_of_collateral_tokens already exists
def number_of_collateral_tokens_exists(df, number_of_collateral_tokens):
    
    if ((df['number_of_collateral_tokens'] == number_of_collateral_tokens)).any():
        df = df.loc[df['number_of_collateral_tokens'] == number_of_collateral_tokens]

    else:
        df = pd.DataFrame()

    return df

#handles our weth_gateway events and returns the accurate user_address
def handle_weth_gateway(event, redemption_index_list, trove_updated_index_list, index):

    if index in redemption_index_list:
        payload_address = event['address']
    
    elif index in trove_updated_index_list:
        # print(event)
        payload_address = event['args']['_borrower']

    # elif payload_address == '0x9546f673ef71ff666ae66d01fd6e7c6dae5a9995':
    #     if contract_type == 2:
    #         user = 'onBehalfOf'
    #         payload_address = event['args'][user]
    
    return payload_address

# # returns a wallet address column
def get_wallet_address_column(index):
    wallet_address_column_list = ['liquidator_address', 'trove_owner', 'trove_owner']
    wallet_address_column = wallet_address_column_list[index]

    return wallet_address_column

#returns df if wallet_address exists
def wallet_address_exists(df, wallet_address, index):

    wallet_address_column_name = get_wallet_address_column(index)

    if ((df[wallet_address_column_name] == wallet_address)).any():
        df = df.loc[df[wallet_address_column_name] == wallet_address]

    else:
        df = pd.DataFrame()

    return df


# # returns the required dataframe depending on our contract_type and index
def get_index_df(event, tx_hash, wallet_address, web3, index):

    redemption_index_list = get_redemption_index_list()
    trove_updated_index_list = get_trove_updated_index_list()

    df = pd.DataFrame()

    if index in redemption_index_list:
        df = get_redemption_event_df(event, tx_hash, wallet_address, web3)

    elif index in trove_updated_index_list:
        df = get_trove_updated_event_df(event, tx_hash, wallet_address, web3, index)

    return df

# # turns our redemption event into a dataframe and returns it
def get_redemption_event_df(event, tx_hash, wallet_address, web3):

    df = pd.DataFrame()

    tx_hash_list = []
    liquidator_address_list = []
    collateral_redeemed_list = []
    number_of_collateral_redeemed_tokens_list = []
    ern_redeemed_list = []
    collateral_fee_list = []
    timestamp_list = []
    block_list = []

    #adds wallet_address if it doesn't exist
    if len(wallet_address) == 42:

        block = web3.eth.get_block(event['blockNumber'])
        block_number = int(block['number'])
        block_list.append(block_number)
        time.sleep(0.25)

        liquidator_address_list.append(wallet_address)
        tx_hash_list.append(tx_hash)
        timestamp_list.append(block['timestamp'])
        token_address = event['args']['_collateral']
        collateral_redeemed_list.append(token_address)
        time.sleep(0.25)
        token_amount = event['args']['_collSent']
        number_of_collateral_redeemed_tokens_list.append(token_amount)
        ern_redeemed = event['args']['_actualLUSDAmount']
        ern_redeemed_list.append(ern_redeemed)
        time.sleep(0.25)
        collateral_fee = event['args']['_collFee']
        collateral_fee_list.append(collateral_fee)

    df['tx_hash'] = tx_hash_list
    df['liquidator_address'] = liquidator_address_list
    df['collateral_redeemed'] = collateral_redeemed_list
    df['number_of_collateral_redeemed_tokens'] = number_of_collateral_redeemed_tokens_list
    df['ern_redeemed'] = ern_redeemed_list
    df['collateral_fee'] = collateral_fee_list
    df['timestamp'] = timestamp_list
    df['block_number'] = block_list

    return df

# # turns our troveUpdated event into a dataframe and returns it
def get_trove_updated_event_df(event, tx_hash, wallet_address, web3, index):

    trove_updated_contract_type = get_trove_type_value(index)

    df = pd.DataFrame()

    tx_hash_list = []
    trove_owner_list = []
    collateral_redeemed_list = []
    number_of_collateral_tokens_list = []
    debt_list = []
    timestamp_list = []
    operation_list = []
    block_list = []

    #adds wallet_address if it doesn't exist
    if len(wallet_address) == 42:

        block = web3.eth.get_block(event['blockNumber'])
        block_number = int(block['number'])
        block_list.append(block_number)

        trove_owner_list.append(wallet_address)
        tx_hash_list.append(tx_hash)
        timestamp_list.append(block['timestamp'])
        token_address = event['args']['_collateral']
        collateral_redeemed_list.append(token_address)
        token_amount = event['args']['_coll']
        number_of_collateral_tokens_list.append(token_amount)
        debt = event['args']['_debt']
        debt_list.append(debt)
        try:
            operation = int(event['args']['operation'])
        except:
            operation = int(event['args']['_operation'])

        operation_list.append(operation)

    df['tx_hash'] = tx_hash_list
    df['trove_owner'] = trove_owner_list
    df['collateral_redeemed'] = collateral_redeemed_list
    df['number_of_collateral_tokens'] = number_of_collateral_tokens_list
    df['debt'] = debt_list
    df['operation'] = operation_list
    df['timestamp'] = timestamp_list
    df['contract_type'] = trove_updated_contract_type
    df['block_number'] = block_list

    return df

# will tell us whether we need to find new data
# returns a list of [tx_hash, wallet_address]
def already_part_of_df(event, index):

    all_exist = False
    tx_hash = ''
    wallet_address = ''
    contract_type = -1

    csv = get_config_df_value('csv', index)

    df = pd.read_csv(csv)

    tx_hash = event['transactionHash'].hex()

    new_df = tx_hash_exists(df, tx_hash)
    
    redemption_index_list = get_redemption_index_list()
    trove_udpated_index_list = get_trove_updated_index_list()

    wallet_address = handle_weth_gateway(event, redemption_index_list, trove_udpated_index_list, index)

    if len(new_df) > 0:
        new_df = wallet_address_exists(df, wallet_address, index)

        if len(new_df) > 0:
            if index in redemption_index_list:
                collateral_fee = event['args']['_collFee']
                new_df = collateral_fee_exists(df, collateral_fee)
            
            elif index in trove_udpated_index_list:
                number_of_collateral_tokens = event['args']['_coll']
                new_df = number_of_collateral_tokens_exists(df, number_of_collateral_tokens)

            if len(new_df) > 0:
                all_exist = True

    response_list = [tx_hash, wallet_address, contract_type, all_exist]

    return response_list

#makes our dataframe
def get_event_df(events, wait_time, web3, index):
    
    df = pd.DataFrame()

    df_list = []

    # # tracks how many events we've gone through
    i = 1
    for event in events:
        time.sleep(wait_time)
        
        print(' Batch of Events Processed: ', i, '/', len(events), ' Events Remaining: ', len(events) - i)
            
        exists_list = already_part_of_df(event, index)

        tx_hash = exists_list[0]
        wallet_address = exists_list[1]
        exists = exists_list[3]


        if exists == False and len(wallet_address) == 42: 
            df = get_index_df(event, tx_hash, wallet_address, web3, index)
            if len(df) > 0:
                df_list.append(df)

        i+=1

    if len(df) < 1:
        df = get_index_df(event, '', '', web3, index)
        df_list.append(df)
    
    df = pd.concat(df_list)

    # print(df)
    # print('User Data Event Looping done in: ', time.time() - start_time)
    return df

# # runs all our looks
# # updates our csv
def find_all_transactions(index):

    interval = get_config_df_value('interval', index)

    csv = get_config_df_value('csv', index)

    from_block = get_from_block(index)

    # from_block = 57966560

    to_block = from_block + interval

    abi = get_abi(index)

    rpc_url = get_config_df_value('rpc_url', index)

    web3 = get_web_3(rpc_url)

    contract_address = get_config_df_value('contract_address', index)

    contract = get_contract(contract_address, abi, web3)

    latest_block = get_latest_block(web3)

    wait_time = get_config_df_value('wait_time', index)
    
    to_block = from_block + interval

    while to_block < latest_block:

        print('Current Event Block vs Latest Event Block to Check: ', from_block, '/', latest_block, 'Blocks Remaining: ', latest_block - from_block)

        events = get_events(contract, from_block, to_block, index)
        
        if len(events) > 0:
            # print(events)
            event_df = get_event_df(events, wait_time, web3, index)
            make_user_data_csv(event_df, index)

        from_block += interval
        to_block += interval

        # print(deposit_events)

        time.sleep(2.5)

        if from_block >= latest_block:
            from_block = latest_block - 1
        
        if to_block >= latest_block:
            to_block = latest_block
    
    return

# returns a list of borrowers who have been redeemed
def get_redeemed_trove_owner_address_list(redemption_df, trove_updated_df):

    unique_redemption_user_df = redemption_df.drop_duplicates(subset=['tx_hash'])

    redemption_tx_hash_list = unique_redemption_user_df['tx_hash'].tolist()

    trove_updated_redemption_tx_list = [trove_updated_df.loc[trove_updated_df['tx_hash'] == x] for x in redemption_tx_hash_list]

    trove_updated_redemption_df = pd.concat(trove_updated_redemption_tx_list)

    # print(trove_updated_redemption_tx_list)
    # print(redemption_tx_hash_list)

    trove_updated_redemption_df = trove_updated_redemption_df.drop_duplicates(subset=['tx_hash'])

    unique_redeemed_trove_owner_address_list = trove_updated_redemption_df['trove_owner'].tolist()

    return unique_redeemed_trove_owner_address_list

# # calculates a users rolling trove balance
def calculate_user_balance_history(trove_owner_df):

    trove_owner_df = trove_owner_df.sort_values(by=['block_number'], ascending=True)
    trove_owner_df = trove_owner_df.reset_index(drop=True)

    trove_owner_df['number_of_collateral_tokens'] = trove_owner_df['number_of_collateral_tokens'].astype(float)
    trove_owner_df['debt'] = trove_owner_df['debt'].astype(float)
    # print(trove_owner_df.dtypes)
    
    trove_owner_df['collateral_change'] = trove_owner_df['number_of_collateral_tokens'].diff()
    trove_owner_df['debt_change'] = trove_owner_df['debt'].diff()
    # print(trove_owner_df)

    trove_owner_df = trove_owner_df.sort_values(by=['block_number'], ascending=False)
    # trove_owner_df = trove_owner_df.reset_index(drop=True)

    return trove_owner_df

# # finds our rolling balance for each redeemed trove_owner
def get_redeemed_user_trove_history(redemption_df, trove_updated_df):

    user_collateral_df_list = []

    unique_redeemed_trove_owner_list = get_redeemed_trove_owner_address_list(redemption_df, trove_updated_df)
    
    for trove_owner in unique_redeemed_trove_owner_list:
        # print(trove_owner)
        trove_owner_df = trove_updated_df.loc[trove_updated_df['trove_owner'] == trove_owner]
        unique_collateral_list = trove_owner_df.collateral_redeemed.unique()
        

        for collateral in unique_collateral_list:
            user_collateral_df = trove_owner_df.loc[trove_owner_df['collateral_redeemed'] == collateral]
            user_collateral_df = calculate_user_balance_history(user_collateral_df)
            if len(user_collateral_df) > 0:
                user_collateral_df_list.append(user_collateral_df)
    
    user_collateral_df = pd.concat(user_collateral_df_list)
    user_collateral_df = user_collateral_df.reset_index(drop=True)

    return user_collateral_df

# # will match our blockchain's contract with our dune contract for the same token
# # adds the token symbol to our dataframe that can be used as a key to get pricing data
def get_collateral_symbol(balance_df):
    token_info_df = get_token_info_df()

    token_info_address_list = token_info_df['token_address'].tolist()
    token_info_symbol_list = token_info_df['token_symbol'].tolist()

    token_list = balance_df['collateral_redeemed'].tolist()

    unique_token_list = list(set(token_list))

    new_df_list = []

    for unique_token in unique_token_list:
        new_df = balance_df.loc[balance_df['collateral_redeemed'] == unique_token]

        i = 0
        while i < len(token_info_address_list):

            token_address = token_info_address_list[i]
            
            unique_token = unique_token.lower()
            token_address = token_address.lower()

            if token_address == unique_token:
                token_symbol = token_info_symbol_list[i]
                new_df['token_symbol'] = token_symbol
                break

            i += 1
        
        if len(new_df) > 0:
            new_df_list.append(new_df)

    new_df = pd.concat(new_df_list)

    return new_df

# # will convert our collateral tokens and debt tokens into a more human readible form that is also more compatible with the dune pricing data
def get_normalized_balance(balance_df):
    
    df_list = []

    token_info_df = get_token_info_df()

    token_list = balance_df['token_symbol'].tolist()

    unique_token_list = list(set(token_list))

    for token_symbol in unique_token_list:
        new_df = balance_df.loc[balance_df['token_symbol'] == token_symbol]
        new_token_info_df = token_info_df.loc[token_info_df['token_symbol'] == token_symbol]

        if len(new_df) > 0:
            division = new_token_info_df['division'].tolist()
            division = division[0]
            new_df['normalized_collateral'] = df['number_of_collateral_tokens'] / division

            df_list.append(new_df)

    if len(df_list) > 0:
        balance_df = pd.concat(df_list)
    
    return balance_df

def set_timestamp_to_unix(dt_str):

  datetime_obj = datetime.datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S.%f UTC")
  
  return datetime_obj.timestamp()


def get_closest_price(balance_df):


    return

# # gets our USD balances over time
def get_usd_balance_history(balance_df):
    

    return

def find_redeemed_trove_cr(redeemed_trove_history_df):

    return

index_list = [0, 1, 2]

for index in index_list:
    find_all_transactions(index)

# find_all_transactions(0)

# redemption_df = pd.read_csv('aurelius_redemption_events.csv')
# trove_updated_df = pd.read_csv('aurelius_trove_updated_events.csv')

# unique_user_list = get_redeemed_trove_owner_address_list(redemption_df, trove_updated_df)
# print(unique_user_list)

# trove_owner_df = calculate_user_balance_history(trove_updated_df)
# print(trove_owner_df)

# df = get_redeemed_user_trove_history(redemption_df, trove_updated_df)

# df = get_collateral_symbol(df)

# df = get_normalized_balance(df)

# timestamp = set_timestamp_to_unix(df)

# print(timestamp)

# print(df)

# token_df = get_token_info_df()


# print(token_df)