#!/usr/bin/python

#from optparse import OptionParser
import json,os,sys
import argparse
import subprocess
import time
from prettytable import PrettyTable

FIO_TEMP=''' \
-filename=%(device)s -direct=1 -iodepth %(iodepth)d \
-thread -rw=%(mode)s -ioengine=libaio  -bs=%(block_size)s \
-size=%(size)s -numjobs=%(numjobs)d  -runtime=%(runtime)d  -group_reporting -name=test  --output-format=json
'''

def excute(binary,args):
    cmd = args.split()
    cmd.insert(0,binary)
    print 'Running', ' '.join(cmd)
    p = subprocess.Popen(cmd, close_fds=True, shell=False,
                         stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
    try:
        out, err = p.communicate()
        if err is not None:
            print err
        if p.returncode != 0:
            raise Exception("Error running ssh command", cmd,
                            p.returncode, out, err)
    except KeyboardInterrupt:
        print "\n  User stopped manually \n"
        sys.exit()
    return p.returncode, out, err


def parse_args():
    p = argparse.ArgumentParser(description='fio test unit')

    p.add_argument('--device',
                   help='direct device or mountpoint, example: --device  /dev/sdb  or  --device /mnt/disk05',
                   type=str)
    p.add_argument('--iodepth',
                   help='default iodepth=1',
                   default=1,
                   type=int)

    p.add_argument('--mode',
                   help='default mode=read,write,randread,randwrite',
                   default="read,write,randread,randwrite",
                   type=str)
    p.add_argument('--size',
                   default="30G",
                   help='default size=30G, but  ALL space for block device ',
                   type=str)

    p.add_argument('--block_size','-bs',
                   default="4K",
                   help='default block-size=4K',
                   type=str)
    p.add_argument('--numjobs',
                   help='default numjobs=1',
                   default=1,
                   type=int)

    p.add_argument('--runtime',
                   help='default runtime=30',
                   default=30,
                   type=int)

    p.add_argument('--parse_file','-f',
                   help='parse file',
                   default=None,
                   type=str)

    p.add_argument('--description','--des',
                   help='add description  for io test unit',
                   default=None,
                   type=str)
    return p


def is_mounted(device):
    d = {}
    for l in file('/proc/mounts'):
        if l[0] == '/':
            l = l.split()
            d[l[0]] = l[1]
    if d.get(device,False):
        return True
    else:
        return False


def fio_start(args,is_raw=None):

    x = PrettyTable(["Device", "Iodepth", "Block Size","Size","Mode", "IOPS","Latency","BW","Utils","Num Jobs"])
    x.padding_width = 1 # One space between column edges and contents (default)
    suffix=''
    if  is_raw is None:
        suffix="/test.img"
    mode_list = args.mode.split(",")
    line = [locatime()]
    line.append(["Device", "Iodepth", "Block Size","Size","Mode", "IOPS","Latency","BW","Utils","Num Jobs","Runtime","Description"])
    for  mode in mode_list:
        _tmp={"device":args.device+suffix,
                  "iodepth":args.iodepth,
                  "mode":mode,
                  "block_size":args.block_size,
                  "size":args.size,
                  "numjobs":args.numjobs,
                  "runtime":args.runtime,
                  "description":args.description
                 }
        fio_args = FIO_TEMP %(_tmp)
        code,out,err = excute("fio",fio_args)
        out_json = json.loads(out)
        if mode in ['read','randread']:
            out_static= out_json.get("jobs",None)[0]['read']
        else:
            out_static= out_json.get("jobs",None)[0]['write']


        x.add_row([args.device+suffix,
            args.iodepth,
            args.block_size,
            args.size,mode,
            out_static['iops'],
            str(round(out_static['lat']['mean']/1000,4))+" ms",
            str(round(out_static['bw_mean']/1024,2))+" MB/s",
            out_json['disk_util'][0]['util'],
            args.numjobs
            ])

        line.append(
            dict(Device=args.device+suffix,
            Iodepth=args.iodepth,
            Block_Size=args.block_size,
            Size=args.size,
            Mode=mode,
            IOPS=out_static['iops'],
            Lantey=out_static['lat']['mean'],
            BW=out_static['bw_mean'],
            Utils=out_json['disk_util'][0]['util'],
            Runtime=args.runtime,
            Description=args.description,
            Num_Jobs=args.numjobs
                )
            )

        if is_raw is None:
            os.remove(args.device+suffix)
    dump_file(line)
    print x

def add_unit(raw,raw_type):
    pass





def locatime():
    return  time.asctime( time.localtime(time.time()) )

def dump_file(line):
    T = time.localtime(time.time())
    y,m,d = T.tm_year,T.tm_mon,T.tm_mday
    jsObj = json.dumps(line)
    filename='./'+str(y)+'-'+str(m)+'-'+str(d)+'.json'
    with open(filename, 'a+') as fp:
           fp.write(jsObj+"\n")

def print_parse(line):
    jsObj = json.loads(line)
    first_raw=jsObj.pop(1)
    first_raw.insert(0,jsObj.pop(0))
    if 'Description' not in first_raw:
        first_raw.append('Description')

    if 'Num Jobs' not in  first_raw:
        first_raw.insert(first_raw.index('Runtime'),'Num Jobs')
    x=PrettyTable(first_raw)
    x.padding_width = 1
    for j in jsObj:
      x.add_row(['',
                j['Device'],
                j['Iodepth'],
                j['Block_Size'],
                j['Size'],
                j['Mode'],
                j['IOPS'],
                str(round(j['Lantey']/1000,4))+" ms",
                str(round(j['BW']/1024,2))+" MB/s",
                j['Utils'],
                j.get('Num_Jobs',1),
                j['Runtime'],
                j.get('Description','')
                ])
    print x

def parse_file(filename):

    with open(filename,"r") as fp:
       lines = fp.readlines()
    for line in lines:
        print_parse(line)


def get_dev_size(blockdev):



    dev = blockdev.split('/').pop()
    prefix_dev = dev[:3]
    parted_dev = dev[3:]
    if  parted_dev :
      if prefix_dev == 'rbd':
          blocks = int(open('/sys/block/{}/size'.format(dev)).read())
      else:
          blocks = int(open('/sys/block/{}/{}/size'.format(prefix_dev,dev)).read())
    else:
      blocks = int(open('/sys/block/{}/size'.format(dev)).read())
    return str(blocks * 512 / 1024 /1024 / 1024)+"G"

def main():
    p = parse_args()
    args = p.parse_args()
    if len(sys.argv)==1:
        p.print_help()
        sys.exit(1)

    if args.parse_file is not None:
        parse_file(args.parse_file)
        sys.exit(0)

    if args.device.startswith("/dev/"):
        if is_mounted(args.device):
            sys.stderr.write("%s is mounting,please unmount first \n" %(args.device))


        else:
             args.size = get_dev_size(args.device)
             fio_start(args,is_raw=True)

    elif not os.path.ismount(args.device):
         sys.stderr.write(" %s is not mounting \n" %(args.device))

    else:

         fio_start(args)


if __name__ == '__main__':
    main()
