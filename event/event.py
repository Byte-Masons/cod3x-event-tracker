from typing import Dict, Any

class CustomEvent:
    
    column_list = ['from_address','to_address','tx_hash','timestamp','token_address','reserve_address',
                   'token_volume','asset_price','usd_token_amount','log_index','transaction_index','block_number']

    def __init__(self, event: Dict[str, Any]):
        self.args = dict(event['args'])
        self.event = event['event']
        self.logIndex = event['logIndex']
        self.transactionIndex = event['transactionIndex']
        self.transactionHash = event['transactionHash'].hex()
        self.address = event['address']
        self.blockHash = event['blockHash'].hex()
        self.blockNumber = event['blockNumber']

    def __getitem__(self, key):
        return getattr(self, key)

    def get(self, key, default=None):
        return getattr(self, key, default)
    
    def convert_web3_event(web3_event):
        return CustomEvent(web3_event)