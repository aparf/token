import json
from web3 import Web3
import pandas as pd
import requests
import warnings
from collections import ChainMap
from web3 import exceptions
from json import JSONDecodeError


class Token:
    """
    NFT parsing module for python.

    token is a python module providing access to the NFT parsing functions. The module aims to create a simple
    interface to parse attributes and sales of different ethereum NFT collections. token is designed to be accessible
    and reusable for the general user without Web3 knowledge. The module uses Etherscan API and Web3 providers.

    Parameters
    ----------
    providerURL : str
        URL of the user's web3 provider. example: infura.
    tokenContractAddress : str
        Token's contract address. The address of a token contract can be obtained from https://etherscan.io
    etherscanAPIKey : str
         Your Etherscan API key. The API key can be obtained from https://etherscan.io/apis.

    Attributes
    ----------
    web3 : web3.main.Web3
        Web3 instance of the class.
    providerURL : str
        Web3 provider's URL.
    abi : dict
        Contract's Application Binary interface.
    tokenContractAddress : str
        Contract's address.
    contract : web3.eth.Contract
        Token's contract, used to invoke contract's functions.
    etherscanAPIKey : str
        User's etherscan API key.
    attributes : list
        List of dictionaries with traits name being the key and value being the value.
    transactions : list
        List of sales (transactions which value is higher than zero) details dictionaries.
    """

    def __init__(self, providerURL, tokenContractAddress, etherscanAPIKey):
        self.web3 = Web3(Web3.HTTPProvider(providerURL))
        if not (self.web3.isConnected()):
            raise ValueError('Can not connect to Web3. Please, check the connection and Provider URL correctness.')
        self.providerURL = providerURL
        abiURL = ('https://api.etherscan.io/api?module=contract&action=getabi&address='
                  + tokenContractAddress
                  + '&apikey='
                  + etherscanAPIKey)
        abiRequest = requests.get(abiURL).json()
        if abiRequest['status'] != '1':
            raise ValueError("Token's contract address or/and Etherscan API Key are invalid.")
        self.abi = abiRequest['result']
        self.tokenContractAddress = tokenContractAddress
        self.contract = self.web3.eth.contract(address=self.tokenContractAddress, abi=self.abi)
        self.etherscanAPIKey = etherscanAPIKey
        self.attributes = []
        self.transactions = []

    def cleanAttributes(self):
        """
        Clear the list of attributes.
        """

        self.attributes = []

    def cleanTransactions(self):
        """
        Clear the list of transactions.
        """

        self.transactions = []

    def parseAttributes(self, start=0, end=-9999):
        """
        Parsing attributes of a token's collection from id start to id finish. If the attributes are not provided,
        the parsing starts from the first NFT token in the collection and ends on the last.

        It is vital to notion that name of each token is parsed and added to attribute, as the order of parsed tokens can
        be wrong.

        Attributes
        ----------
        start : int
            Starting token id. Parsing of the attributes starts from this token.
        end : int
            Ending token id. Parsing of the attributes ends on this token.
        """

        baseURL = self.contract.functions.tokenURI(1).call()[:-1]
        if end == -9999:
            try:
                end = self.contract.functions.maxTokens().call()
            except exceptions.ABIFunctionNotFound:
                end = self.contract.functions.totalSupply().call()
        if start == 0:
            try:
                r = requests.get(baseURL + str(start)).json()
                r['attributes'].append({'trait_type': 'Name', 'value': r['name']})
                self.attributes.append([dict(ChainMap(*[{d['trait_type']: d['value']} for d in r['attributes']]))])
            except JSONDecodeError:
                warnings.warn("The NFT collection starts from id #1, not id #0.")
                pass
            start += 1

        for i in range(start, end + 1):
            try:
                r = requests.get(baseURL + str(i)).json()
                if not ('attributes' in r):
                    warnings.warn("The parsed json does not contain attributes.")
                else:
                    r['attributes'].append({'trait_type': 'Name', 'value': r['name']})
                    self.attributes.append([dict(ChainMap(*[{d['trait_type']: d['value']} for d in r['attributes']]))])
            except ConnectionAbortedError:
                warnings.warn(
                    "The connection has aborted. Most likely the real number of tokens is less than the totalSupply.")
            except JSONDecodeError:
                warnings.warn("Json has not been found. Most likely the parser has reached the end.")

    def attributesToDF(self, file=True, folderPath=''):
        """
        Transforming list of attributes into pandas Dataframe. By default, csv file is created.

        Attributes
        ----------
        file : bool
            If the file is True, then create a csv file of Dataframe.
        folderPath : str
            Path to the folder where csv file will be created.

        Returns
        ----------
        attributesDF : pd.DataFrame
            Dataframe of token's attributes.
        """

        if not self.attributes:
            raise KeyError("The attributes array is empty.")
        attributesDF = pd.concat([pd.DataFrame(attribute) for attribute in self.attributes])
        if file:
            if folderPath[-1] != '/':
                folderPath += '/'
            attributesFileName = (folderPath
                                  + "attributes_"
                                  + self.contract.functions.symbol().call()
                                  + '.csv')
            with open(attributesFileName, "a+") as file:
                attributesDF.to_csv(file)
            file.close()
        return (attributesDF)

    def attributesToTextFile(self, folderPath=''):
        """
         Transforming list of attributes into text file.

         Attributes
         ----------.
         folderPath : str
             Path to the folder where csv file will be created.
         """

        if folderPath[-1] != '/':
            folderPath += '/'

        if not self.attributes:
            raise KeyError("The attributes array is empty.")

        attributesFileName = (folderPath
                              + "attributes_"
                              + self.contract.functions.symbol().call()
                              + '.txt')
        with open(attributesFileName, "a+") as file:
            file.write(json.dumps(self.attributes))
        file.close()

    def parseTransactions(self, transactionsStep=10000, startBlock="00000000", endBlock=-9999):
        """
        Parsing token's collection sales, transactions with value larger than zero, from startBlock to endBlock,
        analysing transactionsStep transactions per one etherscan API call. If the attributes are not provided,
        the parsing starts from the first block and ends on the latest one.

        It is vital to mention that the maximum value of transactionsStep which etherscan API supports is 10,000.

        Attributes
        ----------
        transactionsStep: int
            Number of transactions per API call. Largest supported value is 10,000.
        endBlock : int
            Starting block. Parsing of the transactions starts from this block.
        startBlock : int
            Ending block. Parsing of the transactions ends on this block.
        """

        if transactionsStep > 10000:
            raise ValueError("Transaction's step larger than 10,000 is not supported by Etherscan API.")
        if endBlock == -9999:
            endBlock = str(self.web3.eth.block_number)
        transactionsURL = ('https://api.etherscan.io/api?module=account&action=tokennfttx&'
                           + 'contractaddress=' + self.tokenContractAddress
                           + '&page=1&offset=' + str(transactionsStep)
                           + '&startblock=' + startBlock
                           + '&endblock=' + endBlock
                           + '&sort=enc&apikey=' + str(self.etherscanAPIKey))
        r = requests.get(transactionsURL).json()
        while r['status'] == '1':
            for i in range(0, transactionsStep):
                try:
                    tempTransHash = r['result'][i]['hash']
                    tempTransValue = self.web3.eth.getTransaction(tempTransHash)['value']
                    if (self.web3.eth.getTransaction(tempTransHash)['value']) != 0:
                        transSale = r['result'][i]
                        transSale['value'] = tempTransValue
                        self.transactions.append(transSale)
                except IndexError:
                    warnings.warn('Parser has reached the end of transactions.')
                    return
            startBlock = r['result'][transactionsStep - 1]['blockNumber']
            transactionsURL = ('https://api.etherscan.io/api?module=account&action=tokennfttx&'
                               + 'contractaddress=' + self.tokenContractAddress
                               + '&page=1&offset=' + str(transactionsStep)
                               + '&startblock=' + startBlock
                               + '&endblock=' + endBlock
                               + '&sort=enc&apikey=' + str(self.etherscanAPIKey))
            r = requests.get(transactionsURL).json()
        raise TimeoutError('The transactions have not been parsed successfully.')

    def transactionsToDF(self, file=True, folderPath=''):
        """
        Transforming list of attributes into pandas Dataframe. By default, csv file is created.

        Attributes
        ----------
        file : bool
            If the file is True, then create a csv file of Dataframe.
        folderPath : str
            Path to the folder where csv file will be created.

        Returns
        ----------
        attributesDF : pd.DataFrame
            Dataframe of token's attributes.
        """

        if not (self.transactions):
            raise KeyError("The transactions array is empty.")
        transactionsDF = pd.DataFrame(self.transactions)
        if (file):
            if folderPath[-1] != '/':
                folderPath += '/'
            transactionsFileName = (folderPath
                                    + "transactions_"
                                    + self.contract.functions.symbol().call()
                                    + '.csv')
            with open(transactionsFileName, "a+") as file:
                transactionsDF.to_csv(file)
            file.close()
        return (transactionsDF)

    def transactionsToTextFile(self, path=''):
        """
         Transforming list of transactions into text file.

         Attributes
         ----------.
         folderPath : str
             Path to the folder where csv file will be created.
         """
        if not (self.transactions):
            raise KeyError("The transactions array is empty.")
        transactionsFileName = (path
                                + "transactions_"
                                + self.contract.functions.symbol().call()
                                + '.txt')
        with open(transactionsFileName, "a+") as file:
            file.write(json.dumps(self.transactions))
        file.close()
