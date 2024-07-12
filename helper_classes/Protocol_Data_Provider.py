import sys
import os
import time
import pandas as pd
import numpy as np

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lending_pool import lending_pool_helper as lph

class Protocol_Data_Provider():

    def __init__(self, protocol_data_provider_address: str, rpc_url: str):
        self.protocol_data_provider_address = protocol_data_provider_address
        self.rpc_url = rpc_url
        self.web3 = lph.get_web_3(rpc_url)
        self.abi = self.get_protocol_data_provider_abi()
    
    # # protocol data provider abi
    def get_protocol_data_provider_abi(self) -> list:
        contract_abi = [ { "inputs": [ { "internalType": "contract ILendingPoolAddressesProvider", "name": "addressesProvider", "type": "address" } ], "stateMutability": "nonpayable", "type": "constructor" }, { "inputs": [], "name": "ADDRESSES_PROVIDER", "outputs": [ { "internalType": "contract ILendingPoolAddressesProvider", "name": "", "type": "address" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "getAllATokens", "outputs": [ { "components": [ { "internalType": "string", "name": "symbol", "type": "string" }, { "internalType": "address", "name": "tokenAddress", "type": "address" } ], "internalType": "struct ProtocolDataProvider.TokenData[]", "name": "", "type": "tuple[]" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "getAllReservesTokens", "outputs": [ { "components": [ { "internalType": "string", "name": "symbol", "type": "string" }, { "internalType": "address", "name": "tokenAddress", "type": "address" } ], "internalType": "struct ProtocolDataProvider.TokenData[]", "name": "", "type": "tuple[]" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "asset", "type": "address" } ], "name": "getReserveConfigurationData", "outputs": [ { "internalType": "uint256", "name": "decimals", "type": "uint256" }, { "internalType": "uint256", "name": "ltv", "type": "uint256" }, { "internalType": "uint256", "name": "liquidationThreshold", "type": "uint256" }, { "internalType": "uint256", "name": "liquidationBonus", "type": "uint256" }, { "internalType": "uint256", "name": "reserveFactor", "type": "uint256" }, { "internalType": "bool", "name": "usageAsCollateralEnabled", "type": "bool" }, { "internalType": "bool", "name": "borrowingEnabled", "type": "bool" }, { "internalType": "bool", "name": "stableBorrowRateEnabled", "type": "bool" }, { "internalType": "bool", "name": "isActive", "type": "bool" }, { "internalType": "bool", "name": "isFrozen", "type": "bool" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "asset", "type": "address" } ], "name": "getReserveData", "outputs": [ { "internalType": "uint256", "name": "availableLiquidity", "type": "uint256" }, { "internalType": "uint256", "name": "totalStableDebt", "type": "uint256" }, { "internalType": "uint256", "name": "totalVariableDebt", "type": "uint256" }, { "internalType": "uint256", "name": "liquidityRate", "type": "uint256" }, { "internalType": "uint256", "name": "variableBorrowRate", "type": "uint256" }, { "internalType": "uint256", "name": "stableBorrowRate", "type": "uint256" }, { "internalType": "uint256", "name": "averageStableBorrowRate", "type": "uint256" }, { "internalType": "uint256", "name": "liquidityIndex", "type": "uint256" }, { "internalType": "uint256", "name": "variableBorrowIndex", "type": "uint256" }, { "internalType": "uint40", "name": "lastUpdateTimestamp", "type": "uint40" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "asset", "type": "address" } ], "name": "getReserveTokensAddresses", "outputs": [ { "internalType": "address", "name": "aTokenAddress", "type": "address" }, { "internalType": "address", "name": "stableDebtTokenAddress", "type": "address" }, { "internalType": "address", "name": "variableDebtTokenAddress", "type": "address" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "asset", "type": "address" }, { "internalType": "address", "name": "user", "type": "address" } ], "name": "getUserReserveData", "outputs": [ { "internalType": "uint256", "name": "currentATokenBalance", "type": "uint256" }, { "internalType": "uint256", "name": "currentStableDebt", "type": "uint256" }, { "internalType": "uint256", "name": "currentVariableDebt", "type": "uint256" }, { "internalType": "uint256", "name": "principalStableDebt", "type": "uint256" }, { "internalType": "uint256", "name": "scaledVariableDebt", "type": "uint256" }, { "internalType": "uint256", "name": "stableBorrowRate", "type": "uint256" }, { "internalType": "uint256", "name": "liquidityRate", "type": "uint256" }, { "internalType": "uint40", "name": "stableRateLastUpdated", "type": "uint40" }, { "internalType": "bool", "name": "usageAsCollateralEnabled", "type": "bool" } ], "stateMutability": "view", "type": "function" } ]
        return contract_abi
    
    def get_address_provider_abi(self) -> list:
        contract_abi = [ { "inputs": [ { "internalType": "string", "name": "marketId", "type": "string" } ], "stateMutability": "nonpayable", "type": "constructor" }, { "anonymous": False, "inputs": [ { "indexed": False, "internalType": "bytes32", "name": "id", "type": "bytes32" }, { "indexed": True, "internalType": "address", "name": "newAddress", "type": "address" }, { "indexed": False, "internalType": "bool", "name": "hasProxy", "type": "bool" } ], "name": "AddressSet", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "address", "name": "newAddress", "type": "address" } ], "name": "ConfigurationAdminUpdated", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "address", "name": "newAddress", "type": "address" } ], "name": "EmergencyAdminUpdated", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "address", "name": "newAddress", "type": "address" } ], "name": "LendingPoolCollateralManagerUpdated", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "address", "name": "newAddress", "type": "address" } ], "name": "LendingPoolConfiguratorUpdated", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "address", "name": "newAddress", "type": "address" } ], "name": "LendingPoolUpdated", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "address", "name": "newAddress", "type": "address" } ], "name": "LendingRateOracleUpdated", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": False, "internalType": "string", "name": "newMarketId", "type": "string" } ], "name": "MarketIdSet", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "address", "name": "previousOwner", "type": "address" }, { "indexed": True, "internalType": "address", "name": "newOwner", "type": "address" } ], "name": "OwnershipTransferred", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "address", "name": "newAddress", "type": "address" } ], "name": "PriceOracleUpdated", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": False, "internalType": "bytes32", "name": "id", "type": "bytes32" }, { "indexed": True, "internalType": "address", "name": "newAddress", "type": "address" } ], "name": "ProxyCreated", "type": "event" }, { "inputs": [ { "internalType": "bytes32", "name": "id", "type": "bytes32" } ], "name": "getAddress", "outputs": [ { "internalType": "address", "name": "", "type": "address" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "getEmergencyAdmin", "outputs": [ { "internalType": "address", "name": "", "type": "address" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "getLendingPool", "outputs": [ { "internalType": "address", "name": "", "type": "address" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "getLendingPoolCollateralManager", "outputs": [ { "internalType": "address", "name": "", "type": "address" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "getLendingPoolConfigurator", "outputs": [ { "internalType": "address", "name": "", "type": "address" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "getLendingRateOracle", "outputs": [ { "internalType": "address", "name": "", "type": "address" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "getMarketId", "outputs": [ { "internalType": "string", "name": "", "type": "string" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "getPoolAdmin", "outputs": [ { "internalType": "address", "name": "", "type": "address" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "getPriceOracle", "outputs": [ { "internalType": "address", "name": "", "type": "address" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "getUiSigner", "outputs": [ { "internalType": "address", "name": "", "type": "address" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "owner", "outputs": [ { "internalType": "address", "name": "", "type": "address" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "renounceOwnership", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "bytes32", "name": "id", "type": "bytes32" }, { "internalType": "address", "name": "newAddress", "type": "address" } ], "name": "setAddress", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "bytes32", "name": "id", "type": "bytes32" }, { "internalType": "address", "name": "implementationAddress", "type": "address" } ], "name": "setAddressAsProxy", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "emergencyAdmin", "type": "address" } ], "name": "setEmergencyAdmin", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "manager", "type": "address" } ], "name": "setLendingPoolCollateralManager", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "configurator", "type": "address" } ], "name": "setLendingPoolConfiguratorImpl", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "pool", "type": "address" } ], "name": "setLendingPoolImpl", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "lendingRateOracle", "type": "address" } ], "name": "setLendingRateOracle", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "string", "name": "marketId", "type": "string" } ], "name": "setMarketId", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "admin", "type": "address" } ], "name": "setPoolAdmin", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "priceOracle", "type": "address" } ], "name": "setPriceOracle", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "signer", "type": "address" } ], "name": "setUiSigner", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "newOwner", "type": "address" } ], "name": "transferOwnership", "outputs": [], "stateMutability": "nonpayable", "type": "function" } ]
        return contract_abi
    
    def get_price_oracle_abi(self) -> list:
        contract_abi = [ { "inputs": [ { "internalType": "address[]", "name": "assets", "type": "address[]" }, { "internalType": "address[]", "name": "sources", "type": "address[]" }, { "internalType": "address", "name": "fallbackOracle", "type": "address" }, { "internalType": "address", "name": "baseCurrency", "type": "address" }, { "internalType": "uint256", "name": "baseCurrencyUnit", "type": "uint256" } ], "stateMutability": "nonpayable", "type": "constructor" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "address", "name": "asset", "type": "address" }, { "indexed": True, "internalType": "address", "name": "source", "type": "address" } ], "name": "AssetSourceUpdated", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "address", "name": "baseCurrency", "type": "address" }, { "indexed": False, "internalType": "uint256", "name": "baseCurrencyUnit", "type": "uint256" } ], "name": "BaseCurrencySet", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "address", "name": "fallbackOracle", "type": "address" } ], "name": "FallbackOracleUpdated", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "address", "name": "previousOwner", "type": "address" }, { "indexed": True, "internalType": "address", "name": "newOwner", "type": "address" } ], "name": "OwnershipTransferred", "type": "event" }, { "inputs": [], "name": "BASE_CURRENCY", "outputs": [ { "internalType": "address", "name": "", "type": "address" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "BASE_CURRENCY_UNIT", "outputs": [ { "internalType": "uint256", "name": "", "type": "uint256" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "asset", "type": "address" } ], "name": "getAssetPrice", "outputs": [ { "internalType": "uint256", "name": "", "type": "uint256" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "address[]", "name": "assets", "type": "address[]" } ], "name": "getAssetsPrices", "outputs": [ { "internalType": "uint256[]", "name": "", "type": "uint256[]" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "getFallbackOracle", "outputs": [ { "internalType": "address", "name": "", "type": "address" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "asset", "type": "address" } ], "name": "getSourceOfAsset", "outputs": [ { "internalType": "address", "name": "", "type": "address" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "owner", "outputs": [ { "internalType": "address", "name": "", "type": "address" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "renounceOwnership", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "address[]", "name": "assets", "type": "address[]" }, { "internalType": "address[]", "name": "sources", "type": "address[]" } ], "name": "setAssetSources", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "fallbackOracle", "type": "address" } ], "name": "setFallbackOracle", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "newOwner", "type": "address" } ], "name": "transferOwnership", "outputs": [], "stateMutability": "nonpayable", "type": "function" } ]
        return contract_abi
    
    # # will get all the reserve tokens for our deployment using the protocol data provider contract
    def get_reserve_address_list(self) -> list:

        protocol_data_provider_address = self.protocol_data_provider_address
        contract_abi = self.get_protocol_data_provider_abi()
        web3 = self.web3

        contract = lph.get_contract(protocol_data_provider_address, contract_abi, web3)
        time.sleep(0.4)
        
        reserve_list = contract.functions.getAllReservesTokens().call()

        reserve_list = [x[1] for x in reserve_list]

        self.pdp_contract = contract
        self.reserve_list = reserve_list

        return reserve_list
    
    # # using our reserve addresses, this will find our receipt token addresses
    def get_receipt_token_list(self):

        reserve_address_list = self.reserve_list

        receipt_token_list = []

        contract = self.pdp_contract

        for reserve in reserve_address_list:
            a_token = contract.functions.getReserveTokensAddresses(reserve).call()
            receipt_token_list.append(a_token)
            time.sleep(1)

        self.receipt_token_list = receipt_token_list

        return receipt_token_list
    
    def get_a_token_list(self) -> list:

        a_token_list = [x[0] for x in self.receipt_token_list]

        self.a_token_list = a_token_list

        return a_token_list
    
    def get_v_token_list(self) -> list:
        
        v_token_list = [x[2] for x in self.receipt_token_list]
        self.v_token_list = v_token_list

        return v_token_list

    # # gets all the deposit and variable debt receipt tokens that we actually need to track
    def get_non_stable_receipt_token_list(self) -> list:
        self.get_reserve_address_list()

        self.get_receipt_token_list()

        self.get_a_token_list()

        self.get_v_token_list()

        non_stable_receipt_token_list = []

        for a_token in self.a_token_list:
            non_stable_receipt_token_list.append(a_token)

        for v_token in self.v_token_list:
            non_stable_receipt_token_list.append(v_token)
        
        self.receipt_list = non_stable_receipt_token_list

        return non_stable_receipt_token_list
    
    # # gets our address provider contract address
    def get_address_provider_address(self) -> str:
        
        abi = self.get_protocol_data_provider_abi()
        contract = lph.get_contract(self.protocol_data_provider_address, abi, self.web3)

        address_provider_contract_address = contract.functions.ADDRESSES_PROVIDER().call()
        
        return address_provider_contract_address
    
    def get_address_provider_contract(self):

        address_provider_address = self.get_address_provider_address()
        abi = self.get_address_provider_abi()
        web3 = self.web3

        contract = lph.get_contract(address_provider_address, abi, web3)
        
        return contract
    
    def get_price_oracle_address(self) -> str:

        address_provider_contract = self.get_address_provider_contract()

        price_oracle_address = address_provider_contract.functions.getPriceOracle().call()

        return price_oracle_address
    
    # # gets the aave price oracle
    def get_price_oracle_contract(self):

        price_oracle_address = self.get_price_oracle_address()
        abi = self.get_price_oracle_abi()
        web3 = self.web3

        contract = lph.get_contract(price_oracle_address, abi, web3)

        return contract
    
    def get_oracle_price(self, reserve_list):

        price_oracle_contract = self.get_price_oracle_contract()

        if isinstance(reserve_list, np.ndarray):
            reserve_list = reserve_list.tolist()

        price_list = price_oracle_contract.functions.getAssetsPrices(reserve_list).call()

        price_list = [price / 1e8 for price in price_list]
        
        return price_list

# treasury_address = '0xd93E25A8B1D645b15f8c736E1419b4819Ff9e6EF'
# protocol_data_provider_address = '0x29563f73De731Ae555093deb795ba4D1E584e42E'
# rpc_url = 'wss://mode.drpc.org'
# index = 'mode'
# wait_time = 0.2

# ironclad_pdp = Protocol_Data_Provider(protocol_data_provider_address, rpc_url)

# ironclad_pdp.get_non_stable_receipt_token_list()

# print(ironclad_pdp.receipt_list)