import logging

# configure logging for debugging purposes
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# logging.disable(logging.INFO)
# logging.disable(logging.DEBUG) # disable logging for general run. comment out for logging.

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
        aggOnly = False
        if transType not in ['a', 'A', 'x', 'X', 'e', 'E', 'p', 'P']:
            return 0

        logging.debug("State Variables at start: lastMessageTrade=%s, lastMessageCancel=%s" % (self.lastMessageTrade, self.lastMessageCancel))

        # Check fo undisclosed orders:
        if transType in ['a', 'A']:
            if self.passive_writer.getOrderId(row) in self.passive_writer.undisclosedOrderList:
                logging.info("Message for undisclosed order skipping")
                return 0

        if transType in ['x', 'X']:
            if self.amd_del_writer.getOrderId(row) in self.passive_writer.undisclosedOrderList:
                logging.info("Message for undisclosed order skipping")
                return 0

       # if transType in ['e', 'E']:
        #    if self.agg_handler.getPassiveID(row) in self.passive_writer.undisclosedOrderList:
         #       logging.info("Message for undisclosed order skipping")
          #      return 0

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
                # deals with partially traded agg orders
                aggMsg = self.agg_handler.aggOrderDump(row, passiveWriter=self.passive_writer)
                aggOnly = True
            else:
                aggMsg = self.agg_handler.aggOrderDump(row=None)

        # Third, write basic passive order entry msg when the previous msg != cancel or trade
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
            logging.debug('aggMsgs is not NONE or passivemsg is not NONE: agg = %s passive = %s '% (aggMsg, passivemsg))
            if aggOnly == True:
                msg = {'aggMsg':aggMsg}
            else:
                msg = {"msg": msg}  # set msg to be a dict
                if aggMsg is not None:  # add the agg msg to the msg dict
                    msg['aggMsg'] = aggMsg

                if passivemsg is not None:  # add the passivemsg to the dirc
                    msg['passivemsg'] = passivemsg

        aggOnly = False
        return msg  # return either the msg or the dict of msgs


