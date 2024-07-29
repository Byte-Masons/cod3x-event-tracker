import sys
import os
import time
import pandas as pd
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lending_pool import lending_pool_helper as lph

class oToken():

    WAIT_TIME = 1.05

    def __init__(self, exercise_address: str, rpc_url: str):
        self.exercise_address = exercise_address
        self.rpc_url = rpc_url

        web3 = lph.get_web_3(rpc_url)
        self.web3 = web3

        contract = self.get_exercise_contract()
        self.exercise_contract = contract

        time.sleep(self.WAIT_TIME)

        decimals = 18
        decimals = 10 ** decimals
        self.decimals = decimals
        
        self.o_token_address = self.get_o_token_address()

        time.sleep(self.WAIT_TIME)

        self.payment_token_address = self.get_payment_token_address()

        time.sleep(self.WAIT_TIME)
    
    # # gets the discount_exercise contract abi
    def get_exercise_abi(self):
        contract_abi = [ { "inputs": [ { "internalType": "contract OptionsToken", "name": "oToken_", "type": "address" }, { "internalType": "address", "name": "owner_", "type": "address" }, { "internalType": "contract IERC20", "name": "paymentToken_", "type": "address" }, { "internalType": "contract IERC20", "name": "underlyingToken_", "type": "address" }, { "internalType": "contract IOracle", "name": "oracle_", "type": "address" }, { "internalType": "uint256", "name": "multiplier_", "type": "uint256" }, { "internalType": "uint256", "name": "instantExitFee_", "type": "uint256" }, { "internalType": "uint256", "name": "minAmountToTriggerSwap_", "type": "uint256" }, { "internalType": "address[]", "name": "feeRecipients_", "type": "address[]" }, { "internalType": "uint256[]", "name": "feeBPS_", "type": "uint256[]" }, { "components": [ { "internalType": "address", "name": "swapper", "type": "address" }, { "internalType": "address", "name": "exchangeAddress", "type": "address" }, { "internalType": "enum ExchangeType", "name": "exchangeTypes", "type": "uint8" }, { "internalType": "uint256", "name": "maxSwapSlippage", "type": "uint256" } ], "internalType": "struct SwapProps", "name": "swapProps_", "type": "tuple" } ], "stateMutability": "nonpayable", "type": "constructor" }, { "inputs": [], "name": "Exercise__AmountOutIsZero", "type": "error" }, { "inputs": [], "name": "Exercise__FeeGreaterThanMax", "type": "error" }, { "inputs": [], "name": "Exercise__InvalidFeeAmounts", "type": "error" }, { "inputs": [], "name": "Exercise__InvalidOracle", "type": "error" }, { "inputs": [], "name": "Exercise__MultiplierOutOfRange", "type": "error" }, { "inputs": [], "name": "Exercise__NotOToken", "type": "error" }, { "inputs": [], "name": "Exercise__PastDeadline", "type": "error" }, { "inputs": [], "name": "Exercise__SlippageTooHigh", "type": "error" }, { "inputs": [], "name": "Exercise__ZapMultiplierIncompatible", "type": "error" }, { "inputs": [], "name": "Exercise__feeArrayLengthMismatch", "type": "error" }, { "inputs": [ { "internalType": "uint256", "name": "exType", "type": "uint256" } ], "name": "SwapHelper__InvalidExchangeType", "type": "error" }, { "inputs": [], "name": "SwapHelper__ParamHasAddressZero", "type": "error" }, { "inputs": [], "name": "SwapHelper__SlippageGreaterThanMax", "type": "error" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "uint256", "name": "amount", "type": "uint256" } ], "name": "Claimed", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": False, "internalType": "address[]", "name": "feeRecipients", "type": "address[]" }, { "indexed": False, "internalType": "uint256[]", "name": "feeBPS", "type": "uint256[]" }, { "indexed": False, "internalType": "uint256", "name": "totalAmount", "type": "uint256" } ], "name": "DistributeFees", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "address", "name": "sender", "type": "address" }, { "indexed": True, "internalType": "address", "name": "recipient", "type": "address" }, { "indexed": False, "internalType": "uint256", "name": "amount", "type": "uint256" }, { "indexed": False, "internalType": "uint256", "name": "paymentAmount", "type": "uint256" } ], "name": "Exercised", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "address", "name": "user", "type": "address" }, { "indexed": True, "internalType": "address", "name": "newOwner", "type": "address" } ], "name": "OwnershipTransferred", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": False, "internalType": "address", "name": "account", "type": "address" } ], "name": "Paused", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": False, "internalType": "address[]", "name": "feeRecipients", "type": "address[]" }, { "indexed": False, "internalType": "uint256[]", "name": "feeBPS", "type": "uint256[]" } ], "name": "SetFees", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "uint256", "name": "instantFee", "type": "uint256" } ], "name": "SetInstantFee", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": False, "internalType": "uint256", "name": "minAmountToTrigger", "type": "uint256" } ], "name": "SetMinAmountToTrigger", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "uint256", "name": "newMultiplier", "type": "uint256" } ], "name": "SetMultiplier", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "contract IOracle", "name": "newOracle", "type": "address" } ], "name": "SetOracle", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": True, "internalType": "address", "name": "newTreasury", "type": "address" } ], "name": "SetTreasury", "type": "event" }, { "anonymous": False, "inputs": [ { "indexed": False, "internalType": "address", "name": "account", "type": "address" } ], "name": "Unpaused", "type": "event" }, { "inputs": [], "name": "FEE_DENOMINATOR", "outputs": [ { "internalType": "uint256", "name": "", "type": "uint256" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "to", "type": "address" } ], "name": "claim", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "", "type": "address" } ], "name": "credit", "outputs": [ { "internalType": "uint256", "name": "", "type": "uint256" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "from", "type": "address" }, { "internalType": "uint256", "name": "amount", "type": "uint256" }, { "internalType": "address", "name": "recipient", "type": "address" }, { "internalType": "bytes", "name": "params", "type": "bytes" } ], "name": "exercise", "outputs": [ { "internalType": "uint256", "name": "paymentAmount", "type": "uint256" }, { "internalType": "address", "name": "", "type": "address" }, { "internalType": "uint256", "name": "", "type": "uint256" }, { "internalType": "uint256", "name": "", "type": "uint256" } ], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "uint256", "name": "", "type": "uint256" } ], "name": "feeBPS", "outputs": [ { "internalType": "uint256", "name": "", "type": "uint256" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "uint256", "name": "", "type": "uint256" } ], "name": "feeRecipients", "outputs": [ { "internalType": "address", "name": "", "type": "address" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "uint256", "name": "amount", "type": "uint256" } ], "name": "getPaymentAmount", "outputs": [ { "internalType": "uint256", "name": "paymentAmount", "type": "uint256" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "instantExitFee", "outputs": [ { "internalType": "uint256", "name": "", "type": "uint256" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "minAmountToTriggerSwap", "outputs": [ { "internalType": "uint256", "name": "", "type": "uint256" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "multiplier", "outputs": [ { "internalType": "uint256", "name": "", "type": "uint256" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "oToken", "outputs": [ { "internalType": "contract IOptionsToken", "name": "", "type": "address" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "oracle", "outputs": [ { "internalType": "contract IOracle", "name": "", "type": "address" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "owner", "outputs": [ { "internalType": "address", "name": "", "type": "address" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "pause", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [], "name": "paused", "outputs": [ { "internalType": "bool", "name": "", "type": "bool" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "paymentToken", "outputs": [ { "internalType": "contract IERC20", "name": "", "type": "address" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "address[]", "name": "_feeRecipients", "type": "address[]" }, { "internalType": "uint256[]", "name": "_feeBPS", "type": "uint256[]" } ], "name": "setFees", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "uint256", "name": "_instantExitFee", "type": "uint256" } ], "name": "setInstantExitFee", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "uint256", "name": "_minAmountToTriggerSwap", "type": "uint256" } ], "name": "setMinAmountToTriggerSwap", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "uint256", "name": "multiplier_", "type": "uint256" } ], "name": "setMultiplier", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "internalType": "contract IOracle", "name": "oracle_", "type": "address" } ], "name": "setOracle", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [ { "components": [ { "internalType": "address", "name": "swapper", "type": "address" }, { "internalType": "address", "name": "exchangeAddress", "type": "address" }, { "internalType": "enum ExchangeType", "name": "exchangeTypes", "type": "uint8" }, { "internalType": "uint256", "name": "maxSwapSlippage", "type": "uint256" } ], "internalType": "struct SwapProps", "name": "_swapProps", "type": "tuple" } ], "name": "setSwapProps", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [], "name": "swapProps", "outputs": [ { "internalType": "address", "name": "swapper", "type": "address" }, { "internalType": "address", "name": "exchangeAddress", "type": "address" }, { "internalType": "enum ExchangeType", "name": "exchangeTypes", "type": "uint8" }, { "internalType": "uint256", "name": "maxSwapSlippage", "type": "uint256" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "newOwner", "type": "address" } ], "name": "transferOwnership", "outputs": [], "stateMutability": "nonpayable", "type": "function" }, { "inputs": [], "name": "underlyingToken", "outputs": [ { "internalType": "contract IERC20", "name": "", "type": "address" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "unpause", "outputs": [], "stateMutability": "nonpayable", "type": "function" } ]
        
        return contract_abi

    # # will make our web3 token contract object for our given token_address
    def get_exercise_contract(self):
        token_abi = self.get_exercise_abi()
        contract_address = self.exercise_address

        web3 = self.web3

        contract = lph.get_contract(contract_address, token_abi, web3)
        time.sleep(self.WAIT_TIME)

        return contract
    
    # # gets the o_token_address
    def get_o_token_address(self):
        o_token_address = self.exercise_contract.functions.oToken().call()

        return o_token_address
    
    # # gets the paymentToken used to exercise our option tokens
    def get_payment_token_address(self):
        payment_token_address = self.exercise_contract.functions.paymentToken().call()

        return payment_token_address