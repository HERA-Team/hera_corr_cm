#!/usr/bin/env python

if __name__ == "__main__":
    import argparse
    from hera_corr_cm.hera_corr_handler import HeraCorrHandler

    parser = argparse.ArgumentParser(description='Process commands from the corr:message redis channel.',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-r', dest='redishost', type=str, default='redishost',
                        help ='Hostname of redis server')
    parser.add_argument('-t', dest='testmode', action='store_true', default=False,
                        help ='Use this flag to run in test mode, where no commands are executed')
    args = parser.parse_args()

    handler = HeraCorrHandler(redishost=args.redishost, testmode=args.testmode)
    
    while(True):
        handler.process_command()
