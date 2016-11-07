"""
Converts Chi-X level three data to SMARTS format.
Requires re-ordering by time at end, or this can be done after txt to fav is complete.
"""
import logging
# configure logging for debugging purposes
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# logging.disable(logging.INFO)
logging.disable(logging.DEBUG) # disable logging for general run. comment out for logging.

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


    #  TODO: move to separate module (convertTime) and call for this project
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
        Return value as a int.
        """
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


class PassiveOrderWriter(ChiX_conversion):
    """
    Writes passive ENTER msgs, inheriting from the base class ChiX_conversion.
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
            return "* %s:  ENTER %s %s %s %s %s %s <ON > (@1 {*O=%s})" % (
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


class AggHandler(ChiX_conversion): # TODO: handle updating of passiveDict to account for partially traded orders...
    """
    Class for writing TRADE and aggressive ENTER msgs.
    Elements of aggressive ENTER must be inferred from combination of TRADE msg and related passive ENTER msg.
    Looks Like:
    "S48249157A 685855B 146FMG 18100Y" (passive order entry)
    Followed By:
    "S48249171E 685855 150160115082 685856" (execution)
    Followed By:
    More execution msgs with the same contraID, upto different message type.
    Output looks like: "* 57 10:00:00.013000:  ENTER FMG 57 Ask 7.30 1979 14446 <ON > (@1 {*O=57})"
    If next msg is passive with same contra ID, volume must be appended to cache before aggDump.
    """

    def __init__(self):
        """
        Contains dicts and idx_dicts for loc of all elements that can be inferred from the execution message.
        Inherits from base class __init__
        Contains call to reset_cache() method
        """
        super(AggHandler, self).__init__() # makes sure variables from super "__init__" are also inherited
        self.timestamp_loc = {'start': 1, 'end': 9}
        self.transtype_loc = 9
        self.volume_loc = {'short':{'start':19, 'end': 25}, 'long':{'start':19, 'end':28}}
        self.contra_id_loc = {'short':{'start':34, 'end': 43}, 'long': {'start':38, 'end':47}}
        self.passive_id_loc = {'start': 10, 'end': 19}
        self.traderef_loc = {'short':{'start':25, 'end': 34}, 'long': {'start':29, 'end':38}}
        self.reset_cache()

    def reset_cache(self):
        """
        Method resets the cache
        """
        self.cacheContraID = None
        self.cacheVolume = 0
        self.cachePrice = None
        self.cacheSecurity = None
        self.cacheAggSide = None
        self.cacheTimeStamp = None


    def append_cache(self, volume, price, contraID, security, aggSide, timestamp):
        """
        Method to append_cache with volume, price, contraID, security, aggSide, timestamp, used to write agg order msgs.
        """

        logging.debug("Appending to cache for contraId: %s" % contraID)
        self.cacheVolume += volume  # appended for each trade msg with the same contraID
        self.cachePrice = price  # updated for each trade msg with the same contraID
        if self.cacheContraID is None: # added once for each batch (will not change)
            self.cacheContraID = contraID
            self.cacheSecurity = security
            self.cacheTimeStamp = timestamp
            self.cacheAggSide = aggSide


    def getTimeStamp(self,row):
            return super(AggHandler, self).getTimeStamp(row, start=self.timestamp_loc['start'],
                                                              end=self.timestamp_loc['end'])

    def getTransType(self, row):
        return super(AggHandler, self).getTransType(row, loc=self.transtype_loc)


    def getVolume(self, row, transType=None):
        if transType is None:
            transType = self.getTransType(row)
        return super(AggHandler, self).getVolume(row, idx_dict=self.volume_loc, transType=transType)


    def getContraID(self, row, transType):
        msgLength = self.getMessageLength(transType)
        return super(AggHandler, self).getOrderId(row, start=self.contra_id_loc[msgLength]['start'],
                                                        end=self.contra_id_loc[msgLength]['end'])


    def getPassiveID(self, row):
        return super(AggHandler, self).getOrderId(row, start=self.passive_id_loc['start'],
                                                        end=self.passive_id_loc['end'])


    def getTradeRef(self, row, transType):
        """
        Method to calculate unique trade ref for trade msg output, based on exe msg input.
        Takes row, and transType to establish msg length.
        Returns trade reference for that execution msg (unique for the trade, dif from passive and agg order ID)
        """
        msg_type = self.getMessageLength(transType)
        if msg_type not in self.traderef_loc.keys():
            raise ValueError("%s is not in idx_dict keys: %s" % (msg_type, self.traderef_loc.keys()))
        tradeRef = row[self.traderef_loc[msg_type]['start']:self.traderef_loc[msg_type]['end']].strip()
        return tradeRef


    def exeWriter(self,row, passive_dict):
        """
        Method to write trade msg based on execution msg input.
        Output looks like: '* 111 10:00:00.00:  TRADE BHP 111 100 50 80 <ON > B(12345 ) A(9876 ) T({*F=111})'
        Price must be inferred from passive.
        Value must be combination of price and volume.
        Additional Logic:
            if lastContra == None, appendCache
            else if lastContra == contra, appendCache
            else if lastContra != contra, aggdump (write agg order msg), then append cache with new contra and details.
        Takes row and passiveDict (containing passive order details needed to calculate price for trade msg).
        Returns execution msg, and agg order msg when necessary (in the case that the last execution msg seen is for a
        different contraID to the previous msgs).
        """

        # set variables, including price and value calculations based on relevant passive order
        currentTransType = self.getTransType(row)
        id = self.getPassiveID(row)
        contraID = self.getContraID(row,transType=currentTransType)
        aggOrd = None

        try:  # look for id in the passive dict and set price and security based on values in dict
            p_dict = passive_dict[id]
        except:
            raise KeyError("%s ID not in passive_dict" % id)
        price = p_dict['price']
        security = p_dict['security']
        if type(price) == str:
            price = float(price)

        # get volume based on the execution msg input data
        volume = self.getVolume(row, transType=currentTransType)
        value = int(price*volume) # TODO: deal with rounding if necessary...

        # get bid and ask sides based on passive dict 'side' value.
        if p_dict['side'] == 'Bid':
            aggSide = self.getCounterSide(transSide='Bid')
            bidSide = id
            askSide = contraID
        elif p_dict['side'] == 'Ask':
            aggSide = self.getCounterSide(transSide='Ask')
            bidSide = contraID
            askSide = id
        else:
            raise ValueError("Side not recognised, wtf is this %s" % p_dict['side'])

        # TODO: update the passive dict with trade messages for simplicity...Only Volume needs to be updated
        p_dict['volume'] = p_dict['volume'] - volume
        logging.debug('Passive Dict has been updated due to trade')


        # write trade string using above variables
        tradeString = "* %s:  TRADE %s %s %s %s %s <ON > B(%s  ) A(%s  ) T(*F=%s})" % (
         self.getTimeStamp(row),
         security,
         self.getTradeRef(row,transType=currentTransType),
         price,
         volume,
         value,
         bidSide,
         askSide,
         self.getTradeRef(row,transType=currentTransType),
        )

        # Handle agg order msg, indicated by consecutive trades (append cache when needed, dump agg msgs when needed)
        logging.info("Contra ID %s cache contra id %s" % (contraID, self.cacheContraID))
        if self.cacheContraID is None:
            logging.info("Contra ID is None, appending to cache")
            self.append_cache(volume, price, contraID, security, aggSide, timestamp=(self.getTimeStamp(row)))
        elif contraID == self.cacheContraID:
            logging.info("Contra ID has not changed, appending to cache")
            self.append_cache(volume, price, contraID, security, aggSide, timestamp=(self.getTimeStamp(row)))
        else:
            logging.info("New Contra ID, dumping cache, and appending")
            aggOrd = self.aggOrderDump()
            self.append_cache(volume, price, contraID, security, aggSide, timestamp=(self.getTimeStamp(row)))

        return tradeString, aggOrd  # both the trade string and the agg msg string must be returned by the func, but agg msg may be None.


    def aggOrderDump(self, row=None, passiveWriter=None):
        """
        Method to write agg order msgs by dumping the cache.
        Takes row and passiveWriter (passiveOrderWriter class to allow for methods to be drawn from this class).
        Returns agg order msg string.
        """
        # conduct pre-checks
        if row is not None:
            if passiveWriter is None:
                raise ValueError("No passiveWriter class object passed to aggOrderDump")
            # If passiveID matches contraID, add passive volume to cacheVolume
            if passiveWriter.getOrderId(row) == self.cacheContraID:
                self.cacheVolume += passiveWriter.getVolume(row)

        # set aggOrder msg variables based on cache, then clear cache.
        volume = self.cacheVolume
        price = self.cachePrice
        contraID = self.cacheContraID
        security = self.cacheSecurity
        side = self.cacheAggSide
        timeStamp = self.cacheTimeStamp

        self.reset_cache()

        return "* %s:  ENTER %s %s %s %s %s %s <ON > (@1 {*O=%s}) SIMULATED AGGRESSIVE ORDER" % (
            timeStamp,
            security,
            contraID,
            side,
            price,
            volume,
            self.getTransValue(price, volume),
            contraID
            )


class AmdDelWriter(ChiX_conversion):
    """
    Class to handle msg inputs of type 'x'/'X' to write amendMsg and DelMsg.
    Requires input from 'x'/'X' transTypes, and related passive dict.
    Delete logic: if 'x'/'X' is for all volume and new passiveID != cancelID then msg = DEL
    Amend logic: if 'x'/'X' is for partial volume
    """
    def __init__(self):
        """
        Inherits from base class __init__
        """
        super(AmdDelWriter, self).__init__()
        self.volume_loc = {'short':{'start':19, 'end': 25}, 'long':{'start':19, 'end':28}}
        self.timestamp_loc = {'start': 1, 'end': 9}
        self.transtype_loc = 9
        self.orderid_loc = {'start': 10, 'end': 19}
        self.reset_cache()

    def getTimeStamp(self, row):
        return super(AmdDelWriter, self).getTimeStamp(row, start=self.timestamp_loc['start'],
                                                            end=self.timestamp_loc['end'])


    def getTransType(self, row):
        return super(AmdDelWriter, self).getTransType(row, loc=self.transtype_loc)


    def getOrderId(self, row):
        return super(AmdDelWriter, self).getOrderId(row, start=self.orderid_loc['start'],
                                                          end=self.orderid_loc['end'])


    def getVolume(self, row, transType=None):
        if transType is None:
            transType = self.getTransType(row)
        return super(AmdDelWriter, self).getVolume(row, idx_dict=self.volume_loc, transType=transType)


    def reset_cache(self):
        """
        Method resets the cache of cancel msg info
        """
        self.cacheEmpty = True
        self.cacheID = None
        self.cacheVolume = 0
        self.cacheTimeStamp = None
        self.delWritten = False


    def cacheForCancel(self, cacheEmpty, volume, ID, timestamp, security, side, price):
        """
        Method to cache details from cancel msgs.
        Used to write Delete msg if new passive does not have the same ID.
        """
        self.cacheEmpty = cacheEmpty  # cacheEmpty default to true
        self.cacheVolume = volume  # appended for each cancel msg with the same contraID
        self.cacheTimeStamp = timestamp
        self.cacheID = ID
        self.caheSecurity = security
        self.cacheSide = side
        self.cachePrice = price


    def cacheAndWrite(self, row, amend_dict):
        """
        method writes relevant details of 'X'/'x' msgs to the cancelCache when passiveVol == cancelVol
        (must wait for next passive to determine whether cancel was amend or delete).
        writes AMEND for volume when passiveVol > cancelVol
        Must updated passiveDict in case of AMEND for volume. In this case the passive will not be re-entered so
        details will not automatically update.
        """
        currentTransType = self.getTransType(row)
        id = self.getOrderId(row)
        cancelVol = self.getVolume(row, currentTransType)
        logging.debug('Cancel Volume: %s' % cancelVol)
        cancelTime = self.getTimeStamp(row)

        try:  # look for id in the passive dict and set price and security based on values in dict
            p_dict = amend_dict[id]
            logging.debug('Passive Volume: %s' % p_dict['volume'])
            logging.debug(p_dict)
        except:
            raise KeyError("%s ID not in passive_dict" % id)

        cacheSecurity = p_dict['security']
        cacheSide = p_dict['side']
        cachePrice = p_dict['price']

        if type(cachePrice) == str:
            cachePrice = float(cachePrice)

        if cancelVol >= p_dict['volume']:  # only save to cache if cancel volume >= total passive volume.
            # Cancels with greater volume than the original passive can occur and should be treated as cancels for complete volume.
            # The mis-specification is likely due to inflight error. The cancel is sent but by the time it is recieved a trade has already occured for partial volume.
            # Cache necessary until next passive is seen to establish whether amend or delete should be written.
            cacheEmpty = False # change cache status Empty to False
            self.cacheForCancel(cacheEmpty, cancelVol, id, cancelTime, cacheSecurity, cacheSide, cachePrice)
            logging.debug("%s,%s,%s,%s,%s,%s,%s" % (cacheEmpty, cancelVol, id, cancelTime, cacheSecurity, cacheSide, cachePrice))

        elif cancelVol < p_dict['volume']:  # if cancel vol less than passive vol an amend for volume can be written.
            newVolume = p_dict['volume'] - cancelVol
            p_dict['volume'] = newVolume # TODO: updates passive dict to have new volume based on amend for volume
            logging.debug('Passive Dict Volume Updated because of amend for Volume')
            value = newVolume*cachePrice
            amend =  "* %s:  AMEND %s %s %s abs %s %s %s ({*0=%s})" % (cancelTime, cacheSecurity, id, cacheSide,
                                                                       cachePrice, newVolume, value, id)
            self.reset_cache()
            return amend


    def delWriter(self):
        """
        Writes deletion msg based on cacheCancel data
        """
        time = self.cacheTimeStamp
        id = self.cacheID
        security = self.caheSecurity
        side = self.cacheSide

        return "* %s:  DELET %s %s %s 0 ()" % (time,id, security, side)

    def amendWriter(self, row, passiveWriter):
        """
        Writes amend OR delete msg based on cacheCancel data
        """
        cacheVolume = self.cacheVolume
        time = self.cacheTimeStamp
        id = self.cacheID
        security = self.caheSecurity
        side = self.cacheSide
        newPrice = passiveWriter.getPrice(row)
        newVolume = passiveWriter.getVolume(row)
        volume = newVolume - cacheVolume
        newValue = volume*newPrice

        logging.debug("CacheId: %s - PassiveId: %s" % (self.cacheID, passiveWriter.getOrderId(row)))

        if self.cacheID != passiveWriter.getOrderId(row): # if passiveID != cachedID then a delete msg is written
            self.delWritten = True
            logging.debug('Delete msg written = %s' % self.delWritten)
            return self.delWriter()  #runs delWriter method for writing deletion msgs

        else:  # if the cacheID == the passiveID then the cached details are for an amend.
            return "* %s:  AMEND %s %s %s abs %s %s %s ({*0=%s})" % (time, security, id, side, newPrice, volume,
                                                                       newValue, id)


class HiddenExeWriter(ChiX_conversion):
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

        return '* %s:  OFFTR %s %s exec= %s %s %s %s <OF> T({*F=}) B() A() OFF MARKET TRADE MESSAGE' % (
            self.getTimeStamp(row),
            self.getSecurity(row, transType=currentTransType),
            self.getHiddenID(row, transType=currentTransType),
            self.getTimeStamp(row),
            self.getPrice(row, transType=currentTransType),
            self.getVolume(row, transType=currentTransType),
            self.getTransValue(self.getPrice(row, transType=currentTransType),
            self.getVolume(row, transType=currentTransType))
        )


class Parser:
    """
    Class can write correct conversions for all specified messages.
    Currently handles passive, agg, amend, delete, hidden, and execution msgs)
    """
    def __init__(self, agg_handler, passive_writer, amd_del_writer, hidden_exe_writer):
        """
        Expects to be given all writer methods to be used to produce outputs.
        lastMessageTrade and lastMessageCancel are initiated to False, and adjusted as input is received.
        """
        self.agg_handler = agg_handler
        self.passive_writer = passive_writer
        self.amd_del_writer = amd_del_writer
        self.hidden_exe_writer = hidden_exe_writer

        self.lastMessageTrade = False
        self.lastMessageCancel = False


    def getTransType(self, row):
        """
        Gets transType presuming all input messages have transType in the same location.
        """
        return row[9]


    def parse(self, row):
        """
        Uses specified writer methods to process input data depending on the transType and related logic.
        Takes row and returns correct msg output based on writer method for that transType, or error for unrecognised transType.
        """
        aggMsg = None # set to none as aggMsg does not always have a value.
        msg = None
        passivemsg = None # set to none as passivemsg does not always have a value.
        transType = self.getTransType(row)

        # TODO: integrate this test into the script
        # A simple test
        #assert(transType in ['a', 'A', 'x', 'X', 'e', 'E'])
        # A more verbose test
        #if transType not in ['a', 'A', 'x', 'X', 'e', 'E']:
        #    raise KeyError("transtype is %s, it should be in ....." % transType)

        if transType not in ['a', 'A', 'x', 'X', 'e', 'E', 'p', 'P']:
            return 0

        # First, run execution loop if transType is execution. Set lastMessageTrade == True
        if transType in ['e', 'E']:
            self.lastMessageTrade = True
            # set both variables that can be outputted by agg_handler.exeWriter
            msg, aggMsg =  self.agg_handler.exeWriter(row, passive_dict=self.passive_writer.passiveDict)
            # Return early, either a trade msg or a trade and agg msg will be returned depending on the output of exeWriter.
            if aggMsg is not None:  #if aggMsg has a value, return both aggMsg and msg (trade msg)
                return {'msg': msg, 'aggMsg':aggMsg}
            else:  # otherwise, return trade msg only
                return msg

        # Second, run agg msg loop.
        # This can be built with simple bool check, because if it was a trade it would have returned above already.
        # This will only catch other message types, hence why we can set lastMessTrade = False here.
        # If passive msg, add details to aggmsg cache before agg msg dump. Else, dump existing agg details.
        if self.lastMessageTrade == True:
            self.lastMessageTrade = False
            if transType in ['a', 'A']:
                aggMsg = self.agg_handler.aggOrderDump(row, passiveWriter=self.passive_writer)
            else:
                aggMsg = self.agg_handler.aggOrderDump(row=None)

        # Third, write basic passive order entry msg when the previous msg != cancel
        if transType in ['a', 'A'] and self.lastMessageCancel == False:
            msg = self.passive_writer.writer(row)

        # Fourth, deal with cases where transType == cancel
        if transType in ['x', 'X']:
            self.lastMessageCancel = True
            logging.debug("LastMessageCancel == %s" % self.lastMessageCancel)
            # msg will only have a value if an amend can be printed at this time (partial volume amendment).
            # Otherwise, msg value will be None and cancel cache will be appended.
            # Next passive details are required to establish whether cancel is an amend or deletion.
            msg = self.amd_del_writer.cacheAndWrite(row, amend_dict=self.passive_writer.passiveDict)
            if msg is None:
                # if there is an agg message to dump return it
                if aggMsg is not None:
                    return aggMsg
                else:
                    # if there is nothing to dump, retun 0 and wait for cancel cache on next msg
                    return 0

        # Fifth, deal with cases where passive order occurs after cancel
        if transType in ['a', 'A'] and self.lastMessageCancel == True:
            #logging.debug("'%s'...this should be FALSE" % self.amd_del_writer.cacheEmpty)
            # if cache is empty, write passive (empty cache implies amend for volume already written).
            # When volume alone is amended the passive will not be re-entered so no need to handle this case.
            if self.amd_del_writer.cacheEmpty == True:
                msg = self.passive_writer.writer(row)
            # if cache != empty then either an amend or delete must be written based on amdWriter logic
            else:
                # If delWritten == True then both del and passive must be returned.
                # Else, the msg will be an amend and the passive will remain None.
                msg = self.amd_del_writer.amendWriter(row, passiveWriter=self.passive_writer)
                if self.amd_del_writer.delWritten == True:
                    passivemsg = self.passive_writer.writer(row)
                    self.amd_del_writer.reset_cache()

        # Sixth, deal with off-market trades
        if transType in ['p', 'P']:
            msg = self.hidden_exe_writer.writer(row)

        # Set lastMsgCancel to false if msg type is not 'X'/'x'
        if transType not in ["x", "X"]:
            self.lastMessageCancel = False

        # Handle message output, including composite messages.
        if aggMsg is not None or passivemsg is not None:
            msg = {"msg": msg}

            if aggMsg is not None:
                msg['aggMsg'] = aggMsg

            if passivemsg is not None:
                msg['passivemsg'] = passivemsg

        return msg


if __name__ == '__main__':  # execute only if run as a script.
    logging.info("Run Starting...")
    # This illustrates how the above would be run when called externally.

    # instantiate class Parser with args that call relevant writer methods (execution, agg, hidden, and passive)
    pasr = Parser(agg_handler=AggHandler(),
                  passive_writer=PassiveOrderWriter(),
                  amd_del_writer=AmdDelWriter(),
                  hidden_exe_writer=HiddenExeWriter()
                  )

    reader_object = open('/Users/hharris/Desktop/PyConverter_Test_File.txt', 'rb')
    writer_object = open('/Users/hharris/Desktop/converter_Results.txt', 'w')

    counter = 0  # set counter to limit number of results written
    for row in reader_object:
        print("\n\n\n####")

        # for python3
        row = str(row,'utf-8')

        logging.info(row)

        counter += 1  # add one to counter for each order written
        msg = pasr.parse(row)

        if msg == 0:  # ignore messages that are not of the type 'A' 'a' 'E' 'e', 'x', 'X', 'p', 'P'
            continue

        if type(msg) == dict:
            logging.info(msg)
            for key, value in msg.items():
                writer_object.write(value + "\n")
        else:
            logging.info(msg)
            writer_object.write(msg + "\n")  # '\n' specifies the end of the line for each message written

        if counter > 1E5: # write a total of 10,000 message conversions
            writer_object.close()
            break # exit after writing 10,0000 message conversions

