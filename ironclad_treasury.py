import sys
import os
import time
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lending_pool import lending_pool_helper as lph
from interface.treasury_interface import Treasury_Interface as ti

class Ironclad_Treasury(ti):

    def __init__(self, treasury_address: str, protocol_data_provider_address: str, rpc_url: str):
        self.treasury_address = treasury_address
        self.protocol_data_provider_address = protocol_data_provider_address
        self.rpc_url = rpc_url
    
    def set_web_3(self):
        self.web3 = lph.get_web_3(self.rpc_url)

    def get_pdp_abi(self) -> list:
        contract_abi = [ { "inputs": [ { "internalType": "contract ILendingPoolAddressesProvider", "name": "addressesProvider", "type": "address" } ], "stateMutability": "nonpayable", "type": "constructor" }, { "inputs": [], "name": "ADDRESSES_PROVIDER", "outputs": [ { "internalType": "contract ILendingPoolAddressesProvider", "name": "", "type": "address" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "getAllATokens", "outputs": [ { "components": [ { "internalType": "string", "name": "symbol", "type": "string" }, { "internalType": "address", "name": "tokenAddress", "type": "address" } ], "internalType": "struct ProtocolDataProvider.TokenData[]", "name": "", "type": "tuple[]" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "getAllReservesTokens", "outputs": [ { "components": [ { "internalType": "string", "name": "symbol", "type": "string" }, { "internalType": "address", "name": "tokenAddress", "type": "address" } ], "internalType": "struct ProtocolDataProvider.TokenData[]", "name": "", "type": "tuple[]" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "asset", "type": "address" } ], "name": "getReserveConfigurationData", "outputs": [ { "internalType": "uint256", "name": "decimals", "type": "uint256" }, { "internalType": "uint256", "name": "ltv", "type": "uint256" }, { "internalType": "uint256", "name": "liquidationThreshold", "type": "uint256" }, { "internalType": "uint256", "name": "liquidationBonus", "type": "uint256" }, { "internalType": "uint256", "name": "reserveFactor", "type": "uint256" }, { "internalType": "bool", "name": "usageAsCollateralEnabled", "type": "bool" }, { "internalType": "bool", "name": "borrowingEnabled", "type": "bool" }, { "internalType": "bool", "name": "stableBorrowRateEnabled", "type": "bool" }, { "internalType": "bool", "name": "isActive", "type": "bool" }, { "internalType": "bool", "name": "isFrozen", "type": "bool" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "asset", "type": "address" } ], "name": "getReserveData", "outputs": [ { "internalType": "uint256", "name": "availableLiquidity", "type": "uint256" }, { "internalType": "uint256", "name": "totalStableDebt", "type": "uint256" }, { "internalType": "uint256", "name": "totalVariableDebt", "type": "uint256" }, { "internalType": "uint256", "name": "liquidityRate", "type": "uint256" }, { "internalType": "uint256", "name": "variableBorrowRate", "type": "uint256" }, { "internalType": "uint256", "name": "stableBorrowRate", "type": "uint256" }, { "internalType": "uint256", "name": "averageStableBorrowRate", "type": "uint256" }, { "internalType": "uint256", "name": "liquidityIndex", "type": "uint256" }, { "internalType": "uint256", "name": "variableBorrowIndex", "type": "uint256" }, { "internalType": "uint40", "name": "lastUpdateTimestamp", "type": "uint40" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "asset", "type": "address" } ], "name": "getReserveTokensAddresses", "outputs": [ { "internalType": "address", "name": "aTokenAddress", "type": "address" }, { "internalType": "address", "name": "stableDebtTokenAddress", "type": "address" }, { "internalType": "address", "name": "variableDebtTokenAddress", "type": "address" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "asset", "type": "address" }, { "internalType": "address", "name": "user", "type": "address" } ], "name": "getUserReserveData", "outputs": [ { "internalType": "uint256", "name": "currentATokenBalance", "type": "uint256" }, { "internalType": "uint256", "name": "currentStableDebt", "type": "uint256" }, { "internalType": "uint256", "name": "currentVariableDebt", "type": "uint256" }, { "internalType": "uint256", "name": "principalStableDebt", "type": "uint256" }, { "internalType": "uint256", "name": "scaledVariableDebt", "type": "uint256" }, { "internalType": "uint256", "name": "stableBorrowRate", "type": "uint256" }, { "internalType": "uint256", "name": "liquidityRate", "type": "uint256" }, { "internalType": "uint40", "name": "stableRateLastUpdated", "type": "uint40" }, { "internalType": "bool", "name": "usageAsCollateralEnabled", "type": "bool" } ], "stateMutability": "view", "type": "function" } ]
        return contract_abi
    
    def get_reserve_address_list(self) -> list:

        protocol_data_provider_address = self.protocol_data_provider_address
        contract_abi = self.get_pdp_abi()
        web3 = self.web3

        contract = lph.get_contract(protocol_data_provider_address, contract_abi, web3)
        
        reserve_list = contract.functions.getAllReservesTokens().call()

        reserve_list = [x[1] for x in reserve_list]

        self.pdp_contract = contract
        self.reserve_list = reserve_list

        return reserve_list
    
    def get_receipt_token_list(self):

        reserve_address_list = self.reserve_list

        receipt_token_list = []

        contract = self.pdp_contract

        for reserve in reserve_address_list:
            a_token = contract.functions.getReserveTokensAddresses(reserve).call()
            # print(a_token[0])
            receipt_token_list.append(a_token)
            time.sleep(2)

        self.receipt_token_list = receipt_token_list

        return receipt_token_list
    
    def get_a_token_list(self) -> list:

        a_token_list = [x[0] for x in self.receipt_token_list]

        return a_token_list
    
    def get_v_token_list(self) -> list:
        
        v_token_list = [x[2] for x in self.receipt_token_list]

        return v_token_list



treasury_address = '0xd93E25A8B1D645b15f8c736E1419b4819Ff9e6EF'
protocol_data_provider_address = '0x29563f73De731Ae555093deb795ba4D1E584e42E'
rpc_url = 'wss://mode.drpc.org'

treasury = Ironclad_Treasury(treasury_address, protocol_data_provider_address, rpc_url)
treasury.set_web_3()

reserve_address_list = treasury.get_reserve_address_list()

receipt_token_list = treasury.get_receipt_token_list()

a_token_list = treasury.get_a_token_list()

v_token_list = treasury.get_v_token_list()

print(a_token_list)

print('V Tokens')

print(v_token_list)

