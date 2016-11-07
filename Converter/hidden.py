import logging
import base

# configure logging for debugging purposes
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# logging.disable(logging.INFO)
# logging.disable(logging.DEBUG) # disable logging for general run. comment out for logging.

class HiddenExeWriter(base.ChiX_conversion):
    """
    Class to handle msg inputs of type 'P'/'p' and write off market trade msgs for hidden orders.
    """
    def __init__(self):
        """
        Inherits from base class __init__
        """
        super(HiddenExeWriter, self).__init__()
        self.volume_loc = {'short':{'start':20, 'end': 26}, 'long':{'start':20, 'end':30}}
        self.timestamp_loc = {'start': 1, 'end': 9}
        self.transtype_loc = 9
        self.id_loc = {'short':{'start':42, 'end': 51}, 'long':{'start':55, 'end':64}}
        self.price_loc = {'short':{'start':32, 'end': 42}, 'long':{'start':36, 'end':55}}
        self.security_loc = {'short':{'start':26, 'end': 32}, 'long':{'start':30, 'end':36}}

    def getTimeStamp(self, row):
        return super(HiddenExeWriter, self).getTimeStamp(row, start=self.timestamp_loc['start'],
                                                            end=self.timestamp_loc['end'])


    def getTransType(self, row):
        return super(HiddenExeWriter, self).getTransType(row, loc=self.transtype_loc)


    def getVolume(self, row, transType=None):
        if transType is None:
            transType = self.getTransType(row)
        return super(HiddenExeWriter, self).getVolume(row, idx_dict=self.volume_loc, transType=transType)

    def getPrice(self, row, transType=None):
        if transType is None:
            transType = self.getTransType(row)
        return super(HiddenExeWriter, self).getPrice(row, idx_dict=self.price_loc, transType=transType)


    def getSecurity(self, row, transType):
        return super(HiddenExeWriter, self).getSecurity(row, idx_dict=self.security_loc, transType=transType)


    def getHiddenID(self, row, transType=None):
        """
        method gets the unique trade ID for the hidden order msg.
        """
        if transType is None:
            transType = self.getTransType(row)
        msg_type = self.getMessageLength(transType)
        hiddenId = int(row[self.id_loc[msg_type]['start']:self.id_loc[msg_type]['end']].strip())
        return hiddenId


    def writer(self, row):
        """
        Write hidden execution messages like:
        * 111 10:00.00:  OFFTR BHP 111 exec=10:00.00 100.0 50 5000 <OF > T({*F=111}) B() A()
        """

        currentTransType = self.getTransType(row) # set transType using getTransType method to avoid repeating func call.

        return '* %s %s:  OFFTR %s %s exec= %s %s %s %s <OF> T({*F=}) B() A() OFF MARKET TRADE MESSAGE' % (
            self.getHiddenID(row, transType=currentTransType),
            self.getTimeStamp(row),
            self.getSecurity(row, transType=currentTransType),
            self.getHiddenID(row, transType=currentTransType),
            self.getTimeStamp(row),
            self.getPrice(row, transType=currentTransType),
            self.getVolume(row, transType=currentTransType),
            self.getTransValue(self.getPrice(row, transType=currentTransType),
            self.getVolume(row, transType=currentTransType))
        )


