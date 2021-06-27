import sys
import re
import binascii
import struct
from argparse_addons import Integer

from .. import database
from .utils import format_message_by_frame_id, message_to_dict


# Matches 'candump' output, i.e. "vcan0  1F0   [8]  00 00 00 00 00 00 1B C1".
RE_CANDUMP = re.compile(r'^\s*(?:\(.*?\))?\s*\S+\s+([0-9A-F]+)\s*\[\d+\]\s*([0-9A-F ]*)$')

# Matches 'candump -l' (or -L) output, i.e. "(1594172461.968006) vcan0 1F0#0000000000001BC1"
RE_CANDUMP_LOG = re.compile(r'^\(\d+\.\d+\)\s+\S+\s+([\dA-F]+)#([\dA-F]*)$')

# as above but includes timestamp
RE_CANDUMP_LOG_TSTAMP = re.compile(r'^\((\d+\.\d+)\)\s+\S+\s+([\dA-F]+)#([\dA-F]*)$')

def _mo_unpack_ts(mo):
    timestamp = float(mo.group(1))
    frame_id = mo.group(2)
    frame_id = '0' * (8 - len(frame_id)) + frame_id
    frame_id = binascii.unhexlify(frame_id)
    frame_id = struct.unpack('>I', frame_id)[0]
    data = mo.group(3)
    data = data.replace(' ', '')
    data = binascii.unhexlify(data)

    return timestamp,frame_id, data



def _mo_unpack(mo):
    frame_id = mo.group(1)
    frame_id = '0' * (8 - len(frame_id)) + frame_id
    frame_id = binascii.unhexlify(frame_id)
    frame_id = struct.unpack('>I', frame_id)[0]
    data = mo.group(2)
    data = data.replace(' ', '')
    data = binascii.unhexlify(data)

    return frame_id, data


def _do_decode(args):
    if(args.csv):
        _do_decode_CSV(args)
    else:
        _do_decode_HumanReadable(args)

def _do_decode_HumanReadable(args):
    dbase = database.load_file(args.database,
                               encoding=args.encoding,
                               frame_id_mask=args.frame_id_mask,
                               strict=not args.no_strict)
    decode_choices = not args.no_decode_choices
    re_format = None

    while True:
        line = sys.stdin.readline()

        # Break at EOF.
        if not line:
            break

        line = line.strip('\r\n')

        # Auto-detect on first valid line.
        if re_format is None:
            mo = RE_CANDUMP.match(line)

            if mo:
                re_format = RE_CANDUMP
            else:
                mo = RE_CANDUMP_LOG.match(line)

                if mo:
                    re_format = RE_CANDUMP_LOG
        else:
            mo = re_format.match(line)

        if mo:
            frame_id, data = _mo_unpack(mo)
            line += ' ::'
            line += format_message_by_frame_id(dbase,
                                               frame_id,
                                               data,
                                               decode_choices,
                                               args.single_line)

        print(line)


def _do_decode_CSV(args):
    headers = set()
    dbase = database.load_file(args.database,
                               encoding=args.encoding,
                               frame_id_mask=args.frame_id_mask,
                               strict=not args.no_strict)
    decode_choices = not args.no_decode_choices
    start_from = args.start
    end_at = args.end
    re_format = None

    if(start_from < 100):
        print("Warning: --start should be at least 100 to allow for CSV headers to be generated")

    line_n = 0

    import csv
    with open(args.csv, 'w', newline='')  as output_file:
        print("Collecting header data only from first %d messages" % start_from)

        while True:
            line_n = line_n + 1
            line = sys.stdin.readline()

            # Break at EOF.
            if not line:
                break

            line = line.strip('\r\n')

            re_format = RE_CANDUMP_LOG_TSTAMP
            mo = re_format.match(line)


            if mo:
                timestamp, frame_id, data = _mo_unpack_ts(mo)

                message_dict = message_to_dict(dbase,
                                                   frame_id,
                                                   data,
                                                   decode_choices)
                headers = headers | message_dict.keys()
                message_dict["Timestamp"] = timestamp

                if(line_n == start_from):
                    print("Saving headers: %d columns in CSV" % (len(headers)+1))
                    headers_list = list(headers)
                    headers_list.sort()
                    headers_list = ["Timestamp"] + headers_list
                    dict_writer = csv.DictWriter(output_file, headers_list)
                    dict_writer.writeheader()
                    print("Decoding data.....")
                if(line_n > start_from):
                    try:
                        dict_writer.writerow(message_dict)
                    except(Exception):
                        dict_writer.writerow(dict())
                        print("New or non-cyclic CAN message 0x%x %s not saved to CSV. Set --start to a higher value to include more messages in the CSV header" % (frame_id, message_dict))
                if(args.end > 0 and line_n >= end_at):
                    break
            else:
                print("Parse failed for line %d" % line_n)

    print("Finished. Decoded %d messages." % (line_n - start_from))




def add_subparser(subparsers):
    decode_parser = subparsers.add_parser(
        'decode',
        description=('Decode "candump" CAN frames read from standard input '
                     'and print them in a human readable format.'))
    decode_parser.add_argument(
        '-c', '--no-decode-choices',
        action='store_true',
        help='Do not convert scaled values to choice strings.')
    decode_parser.add_argument(
        '-s', '--single-line',
        action='store_true',
        help='Print the decoded message on a single line.')
    decode_parser.add_argument(
        '-e', '--encoding',
        help='File encoding.')
    decode_parser.add_argument(
        '--no-strict',
        action='store_true',
        help='Skip database consistency checks.')
    decode_parser.add_argument(
        '-m', '--frame-id-mask',
        type=Integer(0),
        help=('Only compare selected frame id bits to find the message in the '
              'database. By default the candump and database frame ids must '
              'be equal for a match.'))
    decode_parser.add_argument(
        '--csv',
        help='CSV Filename - if enabled, will output to timestamped sparse CSV file. Requires candump -L format')
    decode_parser.add_argument(
        '--start',
        type=Integer(0),
        default=1000,
        help='Skip N lines at start of input -- if using CSV file, this argument is required. The skipped messages will be used to build the CSV headers. New message IDs after this cannot be included in the CSV')
    decode_parser.add_argument(
        '--end',
        type=Integer(0),
        help='Quit after this many messages',
        default=0),
    decode_parser.add_argument(
        'database',
        help='Database file.')
    decode_parser.set_defaults(func=_do_decode)
