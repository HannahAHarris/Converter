import base
import logging

# configure logging for debugging purposes
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# logging.disable(logging.INFO)
# logging.disable(logging.DEBUG) # disable logging for general run. comment out for logging.

class AggHandler(base.ChiX_conversion): # TODO: deal with aggressive order entries where 'A'/'a' msg represents remaining now passive order related to the aggressive for a trade.
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
        value = int(price*volume) # TODO: deal with rounding

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

        # update the passive dict with new volume after trade
        p_dict['volume'] = p_dict['volume'] - volume
        logging.debug('Passive Dict has been updated due to trade')


        # write trade string using above variables
        tradeString = "* %s %s:  TRADE %s %s %s %s %s <ON > B(%s  ) A(%s  ) T(*F=%s})" % (
        self.getTradeRef(row,transType=currentTransType),
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
        # conduct pre-checks and deal with agg orders partially traded...
        if row is not None:
            logging.debug('row is not none, preparing to check for passive writer')
            if passiveWriter is None:
                raise ValueError("No passiveWriter class object passed to aggOrderDump")
            # If passiveID matches contraID, add passive volume to cacheVolume
            logging.debug('passive ID %s, contra ID %s' % (passiveWriter.getOrderId(row), self.cacheContraID))
            if passiveWriter.getOrderId(row) == self.cacheContraID:
                logging.debug('passive order ID matches contra, volume is being appended')
                self.cacheVolume += passiveWriter.getVolume(row)

        # set aggOrder msg variables based on cache, then clear cache.
        volume = self.cacheVolume
        price = self.cachePrice
        contraID = self.cacheContraID
        security = self.cacheSecurity
        side = self.cacheAggSide
        timeStamp = self.cacheTimeStamp

        self.reset_cache()
        logging.debug("dumping agg message")
        return "* %s %s:  ENTER %s %s %s %s %s %s <ON > (@1 {*O=%s})" % (
            contraID,
            timeStamp,
            security,
            contraID,
            side,
            price,
            volume,
            self.getTransValue(price, volume),
            contraID
            )


