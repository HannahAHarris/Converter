import base
import logging
# configure logging for debugging purposes
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# logging.disable(logging.INFO)
# logging.disable(logging.DEBUG) # disable logging for general run. comment out for logging.

class AmdDelWriter(base.ChiX_conversion):
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
            p_dict['volume'] = newVolume # update passive dict to have new volume based on amend for volume
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

        return "* %s %s:  DELET %s %s %s 0 ()" % (id, time, id, security, side)

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
            return "* %s %s:  AMEND %s %s %s abs %s %s %s ({*0=%s})" % (id, time, security, id, side, newPrice, volume,
                                                                       newValue, id)


