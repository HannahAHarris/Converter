import logging

# configure logging for debugging purposes
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# logging.disable(logging.INFO)
# logging.disable(logging.DEBUG) # disable logging for general run. comment out for logging.

class ChiX_conversion(object):
    """
    Base class containing all generalisable methods for converting input data to output format.
    """
    def __init__(self):
        """
        Specific variables for base class: message types (short and long messages), list for storing securities seen,
        list for storing undisclosed orders.
        """
        # set of short order message types
        self.shortMessageType = set(['E', 'A', 'X', 'P'])  # add order, execute order, cancel order, hidden order exec

        # set of long order message types
        self.longMessageType = set(['e', 'a', 'x', 'p'])

        # create list to store order IDs for undisclosed orders
        self.undisclosedOrderList = []

        # create list to store securities seen.
        self.securityList = []

    def getTransType(self, row, loc):
        """
        Method for retrieving the transaction type.
        Takes the row and loc (the position in that row where the transType is stored).
        Returns the transaction type for the message/row of data (can be 'E'/'e', 'A'/'a', 'X'/'x', 'P'/'p')
        """
        return row[loc]


    def getMessageLength(self, transType):
        """
        Method to establish msg length based on transType and messageType list in "__init__"
        Take transType (based on getTransType method) and returns message length string (either "short" or "long").
        """
        if transType in self.shortMessageType:
            messageLength = 'short'
        else:
            messageLength = 'long'
        return messageLength


    def padTime(self, x, pad_to=2):
        """
        preliminary method to pad timestamp conversion to ensure correct format: "00:00:00.000"
        Takes an input (x) and a pad_to argument that defaults to 2.
        Returns input with additional '0' padding (1 --> 001).
        """
        x = str(x)
        while len(x) < pad_to:
            x = '0' + x
        return x


    def millis_to_stringTime(self, x):
        """
        preliminary method to convert milliseconds to stringTime, utilising padTime method.
        Takes input data (x) and returns time in correct string based output format ('00:00:00.000').
        """
        hours = int(x / (60*60*1e3))
        mins = int((x - hours*60*60*1e3) / (60*1e3))
        secs = int((x - (hours*60*60*1e3) - (mins*60*1e3)) / 1e3)
        micros = int(x - (hours*60*60*1e3) - (mins*60*1e3) - (secs*1e3))

        stringTime = "%s:%s:%s.%s" % (self.padTime(hours), self.padTime(mins), self.padTime(secs),
                                      self.padTime(micros, 3))
        return stringTime


    def getTimeStamp(self, row, start, end):
        """
        method to convert millisecond time stamp to correct format "00:00:00:.000" utilising millis_to_stringTime method.
        Takes a row, and a start and end position.
        returns timestamp in correct format for output (string type)
        """
        initTimeStamp = int(row[start:end].strip())
        str_timeStamp = self.millis_to_stringTime(initTimeStamp)
        timeStamp = str_timeStamp + "000"
        return timeStamp


    def getOrderId(self, row, start, end):
        """
        method to retrieve order ID from input data.
        Takes row and start and end position where the first and last element of the order ID can be found.
        Returns order ID for that message in string format.
        """
        orderID = row[start:end].strip()
        return orderID


    def getTransSide(self, row, loc):
        """
        method to retrieve and convert transaction side from "B" or "S" to "Bid" or "Ask". Raises error if value unknown.
        Takes row and loc (the position in the input data where the transSide is found)
        Returns the transSide in form "Bid" or "Ask"
        """
        if row[loc] == 'B':
            transSide = 'Bid'
        elif row[loc] == 'S':
            transSide = 'Ask'
        else:
            raise ValueError("Unknown transSide: %s" % row[loc])
        return transSide


    def getCounterSide(self, transSide):
        """
        method to get the counterSide for the order message, based on the transSide.
        Used when creating aggressive order messages.
        Takes transSide based on output of getTransSide method.
        Returns the counterSide in form "Bid" or "Ask"
        """
        if transSide == 'Bid':
            counterSide = 'Ask'
        else:
            counterSide = 'Bid'
        return counterSide


    def getVolume(self, row, idx_dict, transType):
        """
        method to extract volume from input data. Position of volume data in input file varies based on
        transType and message length.
        Takes row, TransType, idx_dict (a dictionary indexed by messageType, containing a dictionary indexed by
        start and end, referencing the position of the volume in the row).
            eg. idx_dict = {'long':{'start': 20, 'end':30}, 'short':{'start': 20, 'end':26}}
        Returns volume for that input message as an int.
        """
        msg_type = self.getMessageLength(transType)
        if msg_type not in idx_dict.keys():
            raise ValueError("%s is not in idx_dict keys: %s" % (msg_type, idx_dict.keys()))
        volume = int(row[idx_dict[msg_type]['start']:idx_dict[msg_type]['end']].strip())
        return volume # if volume is 0 then order is undisclosed


    def returnPriceString(self, price):
        """
        Method to correctly place decimal in converted price string (input file does not include decimal points).
        Takes price based on getPrice method.
        returns price, with correct decimal point placement.
        """
        priceStr = str(price)
        if priceStr[-2:-1] == '.':
            price = '%.2f' % price
        return price

    def returnPriceDenominator(self, transType):
        """
        Method to correctly establish the denomination for the price string in short and long messageTypes
        Takes transType (either 'short' or 'long')
        Returns denominator for price, based on transType length.
        """
        denominator = 10000.0
        if transType == 'long':
            denominator = 10000000.0
        return denominator


    def getPrice(self, row, idx_dict, transType):
        """
        Method to get the price from input data and convert it to the correct output format
        Takes row, transType (to establish msg length), idx_dict (dict containing start and end locations referenced by msg length)
        Returns price in correct format for output, utilising PriceString and PriceDenominator methods.
        """
        msg_type = self.getMessageLength(transType)
        if msg_type not in idx_dict.keys():
            raise ValueError("%s is not in idx_dict keys: %s" % (msg_type, idx_dict.keys()))
        price = int(row[idx_dict[msg_type]['start']:idx_dict[msg_type]['end']].strip())
        price = price / self.returnPriceDenominator(transType)  # Refactor by denominator
        price = self.returnPriceString(price)  # correctly place decimal
        return price


    def getTransValue(self, price, volume):
        """
        Method to calculate transaction value based on price and volume
        Takes price (from getPrice method converted to float from string) and volume based on getVolume method.
        Return value as a int."""
        if type(price) == str:  # no need to convert if price is already float. But based on price method, it will str.
            price = float(price)
        transValue = int(price * volume)
        return transValue  #TODO: should this be a float accounting for rounding?


    def getSecurity(self, row, idx_dict, transType):
        """
        Method to get the security for each input message. Location varies based on transType and messageLength.
        Method also stores unseen securities to the securityList.
        Takes row, idx_dict and  transType.
        Returns security as a string.
        """
        msg_type = self.getMessageLength(transType)
        if msg_type not in idx_dict.keys():
            raise ValueError("%s is not in idx_dict keys: %s" % (msg_type, idx_dict.keys()))
        security = row[idx_dict[msg_type]['start']:idx_dict[msg_type]['end']].strip()
        if security not in self.securityList:  # if security is not in the security list, add it
            self.securityList.append(security)
        return security

