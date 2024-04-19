import transaction_finder as tf
rpc_url = 'https://mainnet.mode.network'

contract_address = '0x3a4ea21729f8556547E1243608872C8827DdEEd7'

web3 = tf.get_web_3(rpc_url)

contract_abi = [ { "inputs": [ { "internalType": "address", "name": "_proxy", "type": "address" }, { "internalType": "address", "name": "_asset", "type": "address" } ], "stateMutability": "nonpayable", "type": "constructor" }, { "inputs": [], "name": "asset", "outputs": [ { "internalType": "address", "name": "", "type": "address" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "decimals", "outputs": [ { "internalType": "uint8", "name": "", "type": "uint8" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "latestAnswer", "outputs": [ { "internalType": "int256", "name": "value", "type": "int256" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "latestTimestamp", "outputs": [ { "internalType": "uint256", "name": "", "type": "uint256" } ], "stateMutability": "view", "type": "function" }, { "inputs": [], "name": "proxy", "outputs": [ { "internalType": "address", "name": "", "type": "address" } ], "stateMutability": "view", "type": "function" } ]

contract = tf.get_contract(contract_address, contract_abi, web3)

asset = contract.functions.asset().call()
decimals = contract.functions.decimals().call()
latestAnswer = contract.functions.latestAnswer().call()
timestamp = contract.functions.latestTimestamp().call()
proxy = contract.functions.proxy().call()

print('Asset: ', asset)
print('Decimals: ', decimals)
print('Latest Answer: ', latestAnswer)
print('Timestamp: ', timestamp)
print('Proxy: ', proxy)