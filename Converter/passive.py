import base
import logging
# configure logging for debugging purposes
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# logging.disable(logging.INFO)
# logging.disable(logging.DEBUG) # disable logging for general run. comment out for logging.

class PassiveOrderWriter(base.ChiX_conversion):
    """
    Writes passive ENTER msgs, inheriting from ChiX_conversion module.
    Contains methods to write passive ENTER msgs. These methods only take row,
    extra args from ChiX_Converter are called from the base class method, using details from passiveOrderWriter __init__
    This class also stores details to passiveDict for aggressive ENTER msgs and AMEND/DELET msgs.
    These messages rely on details that only exist in a passive ENTER msg.
    """

    def __init__(self):
        """
        Provides dicts and idx_dicts giving location of all elements needed to write passive ENTER msgs.
        Creates empty dict used for writing aggressive ENTER and AMEND/DELET msgs.
        The "__init__" inherits the base class "__init__" and ensures they are not overwritten.
        """
        super(PassiveOrderWriter, self).__init__() # ensure variables from super "__init__" are inherited
        self.timestamp_loc = {'start': 1, 'end': 9}
        self.transtype_loc = 9
        self.orderid_loc = {'start': 10, 'end': 19}
        self.transside_loc = 19
        self.volume_loc = {'long':{'start': 20, 'end':30}, 'short':{'start': 20, 'end':26}}
        self.price_loc = {'long': {'start': 36, 'end': 55}, 'short': {'start': 32, 'end': 42}}
        self.security_loc = {'long': {'start': 30, 'end': 36}, 'short': {'start': 26, 'end': 32}}
        self.passiveDict = {}


    def getTimeStamp(self, row):
        return super(PassiveOrderWriter, self).getTimeStamp(row, start=self.timestamp_loc['start'],
                                                            end=self.timestamp_loc['end'])


    def getTransType(self, row):
        return super(PassiveOrderWriter, self).getTransType(row, loc=self.transtype_loc)


    def getOrderId(self, row):
        return super(PassiveOrderWriter, self).getOrderId(row, start=self.orderid_loc['start'],
                                                          end=self.orderid_loc['end'])


    def getTransSide(self, row):
        return super(PassiveOrderWriter, self).getTransSide(row, self.transside_loc)


    def getVolume(self, row, transType=None):
        if transType is None:
            transType = self.getTransType(row)
        return super(PassiveOrderWriter, self).getVolume(row, idx_dict=self.volume_loc, transType=transType)


    def getPrice(self, row, transType=None):
        if transType is None:
            transType = self.getTransType(row)
        return super(PassiveOrderWriter, self).getPrice(row, idx_dict=self.price_loc, transType=transType)


    def getSecurity(self, row, transType):
        return super(PassiveOrderWriter, self).getSecurity(row, idx_dict=self.security_loc, transType=transType)


    def writer(self, row):
        """
        Write passive orders like:
        "* 57 10:00:00.013000:  ENTER CGF 57 Ask 7.30 1979 14446 <ON > (@1 {*O=57})"
        Store security, side, price and volume to passiveDict
        Append the undisclosed order list if the volume for the message !> 0
        Takes row and returns passive ENTER msg in correct format.
        """

        currentTransType = self.getTransType(row) # set transType using getTransType to avoid repeating func call.
        if self.getVolume(row, transType=currentTransType) > 0: # check volume is greater than 0

            # Store on every passive order ID to update data (since price can be amended)
            # Dict will be updated automatically for amends for price, where a full cancel if followed by a re-entry of passive.
            # In the case of trades and amend for volume, the dict needs to be updated manually.
            self.passiveDict[self.getOrderId(row)]= {'security': self.getSecurity(row, transType=currentTransType),
                                                 'side': self.getTransSide(row),
                                                 'price': self.getPrice(row, transType=currentTransType),
                                                 'volume': self.getVolume(row, transType=currentTransType)}
            return "* %s %s:  ENTER %s %s %s %s %s %s <ON > (@1 {*O=%s})" % (
                self.getOrderId(row),
                self.getTimeStamp(row),
                self.getSecurity(row, transType=currentTransType),
                self.getOrderId(row),
                self.getTransSide(row),
                self.getPrice(row, transType=currentTransType),
                self.getVolume(row, transType=currentTransType),
                self.getTransValue(self.getPrice(row, transType=currentTransType),
                                   self.getVolume(row, transType=currentTransType)),
                self.getOrderId(row)
            )
        else:
            self.undisclosedOrderList.append(self.getOrderId(row)) # if volume !>0 then add orderID to list for tracking
            return "undisclosed order"



