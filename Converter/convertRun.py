"""This script takes the specified arguments in command line and runs the converter modules to process ChiX daily data into SMARTS format.
Mandatory arguments are the  input and output file locations.
Must order output files by timestamp to deal with caching before print.
Must convert to FAV before reading into SMARTS"""

# standard imports
import argparse
import logging
import multiprocessing
import os
import sys

# import custom modules
import aggressive
import amend_delete
import hidden
import parser
import passive

# This section can be added to specify the dir python should search to access the library
#lib_path = ""  # this must be set to add the path of the lib to python so modules can be found
#sys.path.append(lib_path) # this adds the path of the lib to python so that the modules can be found

# set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def runParser(input_path, output_path, pasr, maxrows=None):
    logging.info("Run Starting...")

    # open reader and writer objects
    reader_object = open(input_path, 'rb')
    writer_object = open(output_path, 'w')

    counter = 0  # set counter to allow for modification of the number of row written
    for row in reader_object:

        #logging.info("\n\n\n####")  # add enter lines for readability

        # Check if using python 3.0 or higher, if python 3 or above, convert to utf-8
        if sys.version_info >= (3,0):
            row = str(row,'utf-8')

        # Skip blank line
        if row is "\n":
            continue

        logging.info("####\n\n%s\n" % row) # display the input row

        counter += 1  # add one to counter for each order written
        msg = pasr.parse(row)
        logging.info(counter)

        if msg == 0:  # ignore messages that are not handled by the modules. Parser module processes unknown types as 0 (line 54)
            continue

        if type(msg) == dict:
            logging.info(msg) # display the output messages
            for key, value in msg.items():
                if value != "undisclosed order":
                    writer_object.write(value + "\n") # write the messages to the output file, with an enter at the end of the line
        else:
            logging.info(msg) # display the output message
            if msg != "undisclosed order":
                writer_object.write(msg + "\n")  # write the message to the output file, with an enter at the end of the line

        if maxrows is not None:
            if counter > maxrows: # allows for specification of number of rows to process
                writer_object.close()
                break # exit after writing the number of rows specified


if __name__ == "__main__":
    # instantiate argparse to access the command line arguments specified at run time
    argparser = argparse.ArgumentParser(description='Takes arguments including IO paths to run converter')

    # specify all manditory and optional arguments
    argparser.add_argument('input_path', type=str, help='The input file path')
    argparser.add_argument('output_path', type=str, help='The output file path')

    argparser.add_argument('-info', default='HannahIsTheGreatestProgrammerOfAllTime', type=str, help='The truth.')
    argparser.add_argument('-outtag', default='output_', type=str, help='Tag for the ouput files, defaults to output_')
    argparser.add_argument('-maxrows', default=None, type=int, help='specify the number of rows to read from the input file, default to all')
    argparser.add_argument('-processors', default=1, type=int, help='specify the number of multiprocess jobs to run, redunant for individual files. Defaults to 1 to avoid explosions')
    argparser.add_argument('-inputtype', default='file', help="Defines input type as either list_txt, dir, or file")
    argparser.add_argument('--nolog', action='store_true', help='Supress log messages')

    # instantiate parse_args() method to activate above arguments
    args = argparser.parse_args()

    assert(args.inputtype in ['file', 'list_txt', 'dir' ])

    if args.nolog:
        logging.disable(logging.INFO)

    print(args.info)

    # instantiate Parser class from the parser module with the args that call all other relevant writer methods (execution, agg, hidden, and passive)
    pasr = parser.Parser(
            aggressive.AggHandler(),
            passive.PassiveOrderWriter(),
            amend_delete.AmdDelWriter(),
            hidden.HiddenExeWriter(),
            )

    # logic for handling argparser arguments
    if args.inputtype == 'file':
        if not args.input_path.endswith(".txt"):
            raise ValueError("Input file must end with .txt, did you mean to use -inputtype list_txt/dir")

        if args.output_path.endswith(".txt"):
            runParser(args.input_path, args.output_path[:-4] + args.outtag + ".txt", pasr, maxrows=args.maxrows)

        elif args.output_path.endswith("/"):
            in_name = args.input_path.split("/")[-1]
            runParser(args.input_path, args.output_path + args.outtag + in_name, pasr, maxrows=args.maxrows)

        else:
            raise ValueError("Incorrect output path, must end in .txt or /")

    else:
        if args.inputtype == 'list_txt':
            if not args.input_path.endswith(".txt"):
                raise ValueError("Input file must end with .txt, did you mean to use -inputtype dir")
            in_list = []
            read_input = open(args.input_path, 'rb')
            for row in read_input:
                in_list.append(row.rstrip('\n'))

        elif args.inputtype == 'dir':
            if not args.input_path.endswith("/"):
                raise ValueError("Expected input file as directory ending in /, did you mean to use -inputtype list_txt/file")
            in_list = [args.input_path + i for i in os.listdir(args.input_path) if i.endswith(".txt")]

        else:
            raise ValueError("args.inputtype misspecified, read doc")

        if args.output_path.endswith("/"):
            out_list = [args.output_path + args.outtag +i.split("/")[-1] for i in in_list]
        else:
            raise ValueError("args.output_path miss specified, should end in /")

        # create class for mulitproccessing to allow for multiple arguments
        class funcArgs:
            def __init__(self, ip, op):
                self.ip = ip
                self.op = op
                self.p = pasr
                self.mr = args.maxrows

        # create function wrapper that takes a class object holding the arguments
        def classy_runParser(f_args):
            runParser(f_args.ip, f_args.op, f_args.p, f_args.mr)

        funcList = [funcArgs(i, o) for i, o in zip(in_list, out_list)]

        number_processes = args.processors
        pool = multiprocessing.Pool(number_processes)
        results = pool.map_async(classy_runParser, funcList)
        pool.close()
        pool.join()

